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
  it has and reconnects with jittered exponential backoff between
  `:reconnect_min_ms` and `:reconnect_max_ms`. The ceiling doubles per
  attempt; the delay is drawn uniformly between the floor and that
  ceiling, so consumers attached to a restarting broker spread their
  retries instead of arriving in lockstep.

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
        metadata:     %{queue, reason, keys}
        # keys lists the detector keys this consumer was feeding when
        # delivery stopped. φ for those keys will climb while the
        # transport is down; a policy layer can use this to decide the
        # excursion is not evidence about the entities themselves.

      [:phi_accrual_amqp, :keys, :evicted]
        measurements: %{tracked}
        metadata:     %{queue, key, incoming_key, max_tracked_keys}
        # emitted when the tracked-key set is at :max_tracked_keys and a
        # new key displaces the least recently seen one.

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
  @default_max_tracked_keys 1_000

  # `heartbeat: 10` matches the amqp client's own record default on both
  # the URI and keyword paths; it is set explicitly so the value is
  # pinned rather than inherited. `connection_timeout` is the one that
  # changes behaviour: the client defaults to 60s via a URI and 50s via
  # a keyword list, which is how long `open/1` — and therefore any
  # in-flight `status/2` call — can block against a blackholed broker.
  @default_conn_opts [heartbeat: 10, connection_timeout: 5_000]

  @type opts :: [
          queue: String.t(),
          url: String.t(),
          connection_opts: keyword() | String.t(),
          key_resolver: Envelope.resolver(),
          reconnect_min_ms: pos_integer(),
          reconnect_max_ms: pos_integer(),
          max_tracked_keys: pos_integer(),
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
    :backoff_ms,
    :max_tracked_keys,
    :seen_keys,
    :last_delivery_at,
    :disconnected_since
  ]

  @doc """
  Start a consumer.

  Pass `:name` to register the process under a name. Without it the
  consumer runs unnamed, so several consumers — one per queue — can
  coexist in the same supervision tree.
  """
  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts) do
    case Keyword.fetch(opts, :name) do
      {:ok, name} -> GenServer.start_link(__MODULE__, opts, name: name)
      :error -> GenServer.start_link(__MODULE__, opts)
    end
  end

  @doc """
  Build a supervisor child specification.

  The standard supervisor keys `:id`, `:restart` and `:shutdown` are
  read from the same keyword list as the consumer options and are not
  passed on to `start_link/1`.

  `:id` defaults to `:name` when one is given, and otherwise to
  `{PhiAccrualAmqp.Consumer, queue}`. That makes one-consumer-per-queue
  topologies work without spelling out an `:id`:

      children = [
        {PhiAccrualAmqp.Consumer, queue: "heartbeats.node_a"},
        {PhiAccrualAmqp.Consumer, queue: "heartbeats.node_b"}
      ]

  Defaults otherwise match `use GenServer`: `restart: :permanent`,
  `shutdown: 5_000`, `type: :worker`.
  """
  @spec child_spec(opts()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    {sup_opts, start_opts} = Keyword.split(opts, [:id, :restart, :shutdown])

    %{
      id: Keyword.get_lazy(sup_opts, :id, fn -> default_id(start_opts) end),
      start: {__MODULE__, :start_link, [start_opts]},
      type: :worker,
      restart: Keyword.get(sup_opts, :restart, :permanent),
      shutdown: Keyword.get(sup_opts, :shutdown, 5_000)
    }
  end

  defp default_id(start_opts) do
    case Keyword.fetch(start_opts, :name) do
      {:ok, name} -> name
      :error -> {__MODULE__, Keyword.get(start_opts, :queue)}
    end
  end

  @doc """
  Current state of the consumer.

  Returns a map with:

    * `:connected?` — whether a connection and channel are currently open
    * `:queue` — the configured queue
    * `:consumer_tag` — the broker-assigned tag, or `nil` when unsubscribed
    * `:backoff_ms` — the ceiling the next reconnect will draw below
    * `:disconnected_since` — local monotonic ms at which the current
      outage began, or `nil` when connected
    * `:last_delivery_at` — local monotonic ms of the last delivery that
      produced a detector key, or `nil` if none has arrived
    * `:keys_tracked` — how many detector keys this consumer has seen,
      bounded by `:max_tracked_keys`

  The two timestamps are local monotonic milliseconds from the same clock
  as `:erlang.monotonic_time(:millisecond)`, so durations are derived by
  subtracting from a fresh reading of it. They are not wall clocks and
  carry no meaning off this node.

  ## Blocking

  Connection attempts run synchronously inside the consumer, so a call
  landing during one waits for it to finish. `@default_conn_opts` caps
  that at `connection_timeout`, but a call can still block for seconds
  against an unreachable broker — precisely when a health check is most
  likely to run. The `timeout` argument therefore defaults to 5000 rather
  than `:infinity`, and callers should be prepared for the exit.
  """
  @spec status(GenServer.server(), timeout()) :: %{
          connected?: boolean(),
          queue: String.t(),
          consumer_tag: String.t() | nil,
          backoff_ms: non_neg_integer(),
          disconnected_since: integer() | nil,
          last_delivery_at: integer() | nil,
          keys_tracked: non_neg_integer()
        }
  def status(server, timeout \\ 5_000) do
    GenServer.call(server, :status, timeout)
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
      max_tracked_keys: Keyword.get(opts, :max_tracked_keys, @default_max_tracked_keys),
      backoff_ms: 0,
      seen_keys: %{},
      last_delivery_at: nil,
      disconnected_since: :erlang.monotonic_time(:millisecond)
    }

    if Keyword.get(opts, :connect, true) do
      send(self(), :connect)
    end

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      connected?: not is_nil(state.conn) and not is_nil(state.channel),
      queue: state.queue,
      consumer_tag: state.consumer_tag,
      backoff_ms: state.backoff_ms,
      disconnected_since: state.disconnected_since,
      last_delivery_at: state.last_delivery_at,
      keys_tracked: map_size(state.seen_keys)
    }

    {:reply, status, state}
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
             backoff_ms: state.reconnect_min_ms,
             disconnected_since: nil
         }}

      {:error, reason} ->
        state = mark_disconnected(state)
        emit_connection_down(state, reason)
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

    state =
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
          note_key(state, key, receipt_ts)

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

          state
      end

    {:noreply, state}
  end

  def handle_info({:basic_cancel, %{consumer_tag: ctag}}, state) do
    :telemetry.execute(
      [:phi_accrual_amqp, :consumer, :cancelled],
      %{},
      %{queue: state.queue, consumer_tag: ctag, reason: :server_cancelled}
    )

    # teardown_and_reconnect/2 closes the connection, so by the time this
    # clause returns the connection genuinely is down. Emitting here keeps
    # :keys on exactly one event name, and keeps :disconnected_since
    # accurate for status/2 — without this, a server cancel would leave a
    # consumer reporting as connected while it sits in backoff with no
    # subscription. close_channel/1 demonitors with :flush, so no :DOWN
    # follows to emit a second time.
    state = mark_disconnected(state)
    emit_connection_down(state, :server_cancelled)

    teardown_and_reconnect(state, :server_cancelled)
  end

  def handle_info({:basic_cancel_ok, _}, state), do: {:noreply, state}

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{conn_ref: ref} = state) do
    state = mark_disconnected(state)
    emit_connection_down(state, reason)

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
    state = mark_disconnected(state)
    emit_connection_down(state, {:channel_down, reason})

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

  defp connect(%{connection_opts: nil, url: url}),
    do: AMQP.Connection.open(url, @default_conn_opts)

  defp connect(%{connection_opts: url}) when is_binary(url),
    do: AMQP.Connection.open(url, @default_conn_opts)

  defp connect(%{connection_opts: opts}) when is_list(opts),
    do: AMQP.Connection.open(Keyword.merge(@default_conn_opts, opts))

  defp teardown_and_reconnect(state, _reason) do
    cleared = close(state)
    schedule_reconnect(cleared)
  end

  defp mark_disconnected(%{disconnected_since: nil} = state),
    do: %{state | disconnected_since: :erlang.monotonic_time(:millisecond)}

  defp mark_disconnected(state), do: state

  # The detector keys this consumer was feeding are carried on the event
  # so a policy layer can tell "φ is climbing because the entity went
  # quiet" from "φ is climbing because this transport stopped
  # delivering". The consumer takes no action on the estimators itself:
  # it does not own them (`PhiAccrual.observe/2` auto-tracks, and other
  # sources may feed the same key), and the only lever core exposes,
  # `PhiAccrual.untrack/1`, destroys the estimator's calibration
  # outright — a disproportionate answer to a transient blip.
  defp emit_connection_down(state, reason) do
    :telemetry.execute(
      [:phi_accrual_amqp, :connection, :down],
      %{},
      %{
        queue: state.queue,
        reason: reason,
        keys: Map.keys(state.seen_keys)
      }
    )
  end

  # Bounded because the default resolver is the routing key: a wildcard
  # binding can mint an unbounded number of distinct keys, and this map
  # would otherwise grow with them. Eviction is least-recently-seen and
  # only runs once the cap is reached, so the O(n) scan is bounded by
  # `:max_tracked_keys`.
  defp note_key(state, key, ts) do
    seen = state.seen_keys

    seen =
      if Map.has_key?(seen, key) or map_size(seen) < state.max_tracked_keys do
        seen
      else
        evict_oldest(seen, state, key)
      end

    %{state | seen_keys: Map.put(seen, key, ts), last_delivery_at: ts}
  end

  defp evict_oldest(seen, state, incoming) do
    {oldest, _ts} = Enum.min_by(seen, fn {_key, ts} -> ts end)

    :telemetry.execute(
      [:phi_accrual_amqp, :keys, :evicted],
      %{tracked: map_size(seen)},
      %{
        queue: state.queue,
        key: oldest,
        incoming_key: incoming,
        max_tracked_keys: state.max_tracked_keys
      }
    )

    Map.delete(seen, oldest)
  end

  defp schedule_reconnect(state) do
    {delay, next} =
      backoff_delay(state.backoff_ms, state.reconnect_min_ms, state.reconnect_max_ms)

    Process.send_after(self(), :connect, delay)
    {:noreply, %{state | backoff_ms: next}}
  end

  # Full-jitter backoff with a floor. The ceiling doubles per attempt up
  # to `:reconnect_max_ms`; the delay actually slept is drawn uniformly
  # from `[min_ms, ceiling]`. Drawing matters when a broker restarts with
  # many consumers attached: undithered doubling has all of them retry in
  # lockstep at 1s, 2s, 4s, stampeding a broker that is still recovering.
  # The floor keeps the documented lower bound intact rather than drawing
  # from zero.
  @doc false
  @spec backoff_delay(non_neg_integer(), pos_integer(), pos_integer()) ::
          {pos_integer(), pos_integer()}
  def backoff_delay(backoff_ms, min_ms, max_ms) do
    cap = max(min_ms, max_ms)
    ceiling = backoff_ms |> max(min_ms) |> min(cap)
    {draw(min_ms, ceiling), min(ceiling * 2, cap)}
  end

  defp draw(min_ms, ceiling) when ceiling > min_ms,
    do: min_ms + :rand.uniform(ceiling - min_ms + 1) - 1

  defp draw(min_ms, _ceiling), do: min_ms

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
