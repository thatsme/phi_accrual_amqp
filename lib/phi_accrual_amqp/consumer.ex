defmodule PhiAccrualAmqp.Consumer do
  @moduledoc """
  AMQP consumer that feeds broker deliveries into the `PhiAccrual`
  core detector.

  Opens an AMQP connection, opens a channel, subscribes to one
  configured queue, and on every delivery calls
  `PhiAccrual.observe(detector_key, receipt_ts)` where `receipt_ts`
  comes from `:erlang.monotonic_time(:millisecond)` at the instant
  the delivery message lands in this GenServer's mailbox. The
  detector key is extracted from the envelope by
  `PhiAccrualAmqp.Envelope.extract/2`.

  ## Clock discipline (read this)

  `phi_accrual`'s estimator works on **local monotonic time only**.
  The publisher's `BasicProperties.timestamp` and any broker-stamped
  header (e.g., from the `rabbitmq_message_timestamp` plugin) are
  cross-process wall clocks; using them to feed the EWMA breaks the
  detector. This module passes them through as diagnostic telemetry
  metadata but never as the value handed to `observe/2`.

  ## Liveness caveat (read this too)

  In AMQP, "delivery received" proves three things are alive in
  combination: publisher, broker, and the network paths between
  them and you. A high phi value does NOT pin the fault on the
  publisher. If you need publisher-only liveness, choose a transport
  with no intermediary (e.g., `phi_accrual_udp`).

  ## Mapping deliveries to detector keys

  See `PhiAccrualAmqp.Envelope` for the resolver contract. The
  default extracts `meta.routing_key`. For static N-queues-per-node
  topologies pass a constant resolver per consumer:

      Consumer.start_link(
        queue: "heartbeats.node_a",
        key_resolver: fn _meta -> :node_a end
      )

  ## Connection lifecycle

  The consumer manages its own connection, channel, and subscription.
  On startup it schedules an async `:connect` so the supervisor can
  come up before the broker is reachable. On any failure — broker
  unreachable, channel error, server-initiated `basic.cancel`,
  connection or channel process death — the consumer tears down what
  it has and reconnects with exponential backoff between
  `:reconnect_min_ms` and `:reconnect_max_ms`.

  This deliberately differs from `PhiAccrualUdp.Listener`'s fail-fast
  socket open: an AMQP connection is a remote-broker contract that
  can blip during normal operation, while a UDP socket open is a
  local syscall that essentially never fails after success.

  ## Telemetry

      [:phi_accrual_amqp, :connection, :up]
        measurements: %{}
        metadata:     %{queue}

      [:phi_accrual_amqp, :connection, :down]
        measurements: %{}
        metadata:     %{queue, reason}

      [:phi_accrual_amqp, :consumer, :registered]
        measurements: %{}
        metadata:     %{queue, consumer_tag}

      [:phi_accrual_amqp, :consumer, :cancelled]
        measurements: %{}
        metadata:     %{queue, consumer_tag, reason}

      [:phi_accrual_amqp, :sample, :received]
        measurements: %{}
        metadata:     %{detector_key, envelope_timestamp, routing_key, exchange, queue}
        # envelope_timestamp may be nil if the publisher didn't set one;
        # it is NEVER what gets passed to PhiAccrual.observe/2.

      [:phi_accrual_amqp, :extract, :error]
        measurements: %{}
        metadata:     %{reason, routing_key, exchange, queue}
        # reason ∈ [:no_detector_key, :resolver_raised]

  The `:sample, :received` event name is shared with `phi_accrual_udp`, but
  the payload shape differs (identity key `detector_key` vs `node`; timestamp
  in metadata vs measurements). Handlers are not cross-transport drop-in.
  """

  use GenServer
  require Logger

  alias PhiAccrualAmqp.Envelope

  @default_url "amqp://localhost"
  @default_reconnect_min_ms 1_000
  @default_reconnect_max_ms 30_000

  @type opts :: [
          queue: String.t(),
          url: String.t(),
          connection_opts: keyword() | String.t(),
          key_resolver: Envelope.resolver(),
          reconnect_min_ms: pos_integer(),
          reconnect_max_ms: pos_integer(),
          connect: boolean(),
          name: GenServer.name()
        ]

  defstruct [
    :queue,
    :url,
    :connection_opts,
    :key_resolver,
    :reconnect_min_ms,
    :reconnect_max_ms,
    :conn,
    :conn_ref,
    :channel,
    :channel_ref,
    :consumer_tag,
    :backoff_ms
  ]

  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    queue = Keyword.fetch!(opts, :queue)

    state = %__MODULE__{
      queue: queue,
      url: Keyword.get(opts, :url, @default_url),
      connection_opts: Keyword.get(opts, :connection_opts),
      key_resolver: Keyword.get(opts, :key_resolver, &Envelope.default_key_resolver/1),
      reconnect_min_ms: Keyword.get(opts, :reconnect_min_ms, @default_reconnect_min_ms),
      reconnect_max_ms: Keyword.get(opts, :reconnect_max_ms, @default_reconnect_max_ms),
      backoff_ms: 0
    }

    if Keyword.get(opts, :connect, true) do
      send(self(), :connect)
    end

    {:ok, state}
  end

  @impl true
  def handle_info(:connect, state) do
    case open(state) do
      {:ok, conn, ch, ctag} ->
        conn_ref = Process.monitor(conn.pid)
        ch_ref = Process.monitor(ch.pid)

        :telemetry.execute(
          [:phi_accrual_amqp, :connection, :up],
          %{},
          %{queue: state.queue}
        )

        {:noreply,
         %{
           state
           | conn: conn,
             conn_ref: conn_ref,
             channel: ch,
             channel_ref: ch_ref,
             consumer_tag: ctag,
             backoff_ms: state.reconnect_min_ms
         }}

      {:error, reason} ->
        :telemetry.execute(
          [:phi_accrual_amqp, :connection, :down],
          %{},
          %{queue: state.queue, reason: reason}
        )

        schedule_reconnect(state)
    end
  end

  def handle_info({:basic_consume_ok, %{consumer_tag: ctag}}, state) do
    :telemetry.execute(
      [:phi_accrual_amqp, :consumer, :registered],
      %{},
      %{queue: state.queue, consumer_tag: ctag}
    )

    {:noreply, state}
  end

  def handle_info({:basic_deliver, _payload, meta}, state) do
    receipt_ts = :erlang.monotonic_time(:millisecond)

    case Envelope.extract(meta, key_resolver: state.key_resolver) do
      {:ok, %Envelope{detector_key: key, timestamp: ts}} ->
        :telemetry.execute(
          [:phi_accrual_amqp, :sample, :received],
          %{},
          %{
            detector_key: key,
            envelope_timestamp: ts,
            routing_key: Map.get(meta, :routing_key),
            exchange: Map.get(meta, :exchange),
            queue: state.queue
          }
        )

        PhiAccrual.observe(key, receipt_ts)

      {:error, reason} ->
        :telemetry.execute(
          [:phi_accrual_amqp, :extract, :error],
          %{},
          %{
            reason: reason,
            routing_key: Map.get(meta, :routing_key),
            exchange: Map.get(meta, :exchange),
            queue: state.queue
          }
        )
    end

    {:noreply, state}
  end

  def handle_info({:basic_cancel, %{consumer_tag: ctag}}, state) do
    :telemetry.execute(
      [:phi_accrual_amqp, :consumer, :cancelled],
      %{},
      %{queue: state.queue, consumer_tag: ctag, reason: :server_cancelled}
    )

    teardown_and_reconnect(state, :server_cancelled)
  end

  def handle_info({:basic_cancel_ok, _}, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{conn_ref: ref} = state) do
    :telemetry.execute(
      [:phi_accrual_amqp, :connection, :down],
      %{},
      %{queue: state.queue, reason: reason}
    )

    cleared = %{
      state
      | conn: nil,
        conn_ref: nil,
        channel: nil,
        channel_ref: nil,
        consumer_tag: nil
    }

    schedule_reconnect(cleared)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{channel_ref: ref} = state) do
    :telemetry.execute(
      [:phi_accrual_amqp, :connection, :down],
      %{},
      %{queue: state.queue, reason: {:channel_down, reason}}
    )

    teardown_and_reconnect(state, reason)
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    close(state)
    :ok
  end

  defp open(state) do
    try do
      case connect(state) do
        {:ok, conn} ->
          case open_channel_and_consume(conn, state) do
            {:ok, ch, ctag} ->
              {:ok, conn, ch, ctag}

            {:error, reason} ->
              safe_close_connection(conn)
              {:error, reason}
          end

        {:error, reason} ->
          {:error, reason}
      end
    catch
      :exit, reason -> {:error, {:exit, reason}}
    end
  end

  defp open_channel_and_consume(conn, state) do
    with {:ok, ch} <- AMQP.Channel.open(conn),
         {:ok, ctag} <- AMQP.Basic.consume(ch, state.queue, self(), no_ack: true) do
      {:ok, ch, ctag}
    end
  end

  defp safe_close_connection(conn) do
    AMQP.Connection.close(conn)
  catch
    _, _ -> :ok
  end

  defp connect(%{connection_opts: nil, url: url}), do: AMQP.Connection.open(url)
  defp connect(%{connection_opts: opts}), do: AMQP.Connection.open(opts)

  defp teardown_and_reconnect(state, _reason) do
    cleared = close(state)
    schedule_reconnect(cleared)
  end

  defp schedule_reconnect(state) do
    delay = max(state.backoff_ms, state.reconnect_min_ms)
    Process.send_after(self(), :connect, delay)
    next = min(delay * 2, state.reconnect_max_ms)
    {:noreply, %{state | backoff_ms: next}}
  end

  defp close(state) do
    state
    |> close_channel()
    |> close_connection()
  end

  defp close_channel(%{channel: nil} = state), do: state

  defp close_channel(%{channel: ch, channel_ref: ref} = state) do
    if ref, do: Process.demonitor(ref, [:flush])

    try do
      AMQP.Channel.close(ch)
    catch
      _, _ -> :ok
    end

    %{state | channel: nil, channel_ref: nil, consumer_tag: nil}
  end

  defp close_connection(%{conn: nil} = state), do: state

  defp close_connection(%{conn: conn, conn_ref: ref} = state) do
    if ref, do: Process.demonitor(ref, [:flush])

    try do
      AMQP.Connection.close(conn)
    catch
      _, _ -> :ok
    end

    %{state | conn: nil, conn_ref: nil}
  end
end
