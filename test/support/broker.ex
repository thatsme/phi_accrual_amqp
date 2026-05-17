defmodule PhiAccrualAmqp.Test.Broker do
  @moduledoc false

  # Test-only helper. NOT compiled into the package: see
  # `elixirc_paths/1` in mix.exs.

  @spec url() :: String.t()
  def url, do: System.get_env("RABBITMQ_URL", "amqp://localhost")

  @doc """
  Cheap probe: open a connection, close it immediately, return whether
  it worked. Used as the integration-suite skip gate.

  Catches exits because `AMQP.Connection.open/1` can exit (rather than
  return `{:error, _}`) when the broker process dies mid-handshake.
  """
  @spec available?() :: boolean()
  def available? do
    case open_with_timeout(url(), 2_000) do
      {:ok, conn} ->
        _ = safe_close(conn)
        true

      _ ->
        false
    end
  end

  @spec declare_topology(AMQP.Channel.t(), term()) :: {String.t(), String.t()}
  def declare_topology(channel, suffix) do
    exchange = "phi.test.#{suffix}"
    queue = "phi.test.q.#{suffix}"

    :ok =
      AMQP.Exchange.declare(channel, exchange, :topic,
        durable: false,
        auto_delete: true
      )

    # Queue declaration constraints:
    #
    #   - RabbitMQ 4.x rejects transient non-exclusive queues
    #     (`transient_nonexcl_queues` is deprecated), so must be
    #     durable or exclusive.
    #   - Exclusive is unusable because the test's connection
    #     declares the queue but the Consumer's connection consumes
    #     from it.
    #   - auto_delete must be off: the reconnect test briefly leaves
    #     the queue without consumers when killing the Consumer's
    #     channel; auto_delete would race the reconnect and delete
    #     the queue from under it.
    #
    # Cleanup runs via on_exit in the test. If on_exit is missed
    # (test process killed, VM crash), the `x-expires` argument
    # below makes the queue self-heal after 60 seconds of being
    # unused, so a missed cleanup leaves no broker residue.
    {:ok, _info} =
      AMQP.Queue.declare(channel, queue,
        durable: true,
        auto_delete: false,
        exclusive: false,
        arguments: [{"x-expires", :long, 60_000}]
      )

    :ok = AMQP.Queue.bind(channel, queue, exchange, routing_key: "#")
    {exchange, queue}
  end

  @spec cleanup(AMQP.Channel.t(), String.t(), String.t()) :: :ok
  def cleanup(channel, exchange, queue) do
    _ =
      try do
        AMQP.Queue.delete(channel, queue)
      catch
        _, _ -> :ok
      end

    _ =
      try do
        AMQP.Exchange.delete(channel, exchange)
      catch
        _, _ -> :ok
      end

    :ok
  end

  defp open_with_timeout(url, timeout_ms) do
    parent = self()
    ref = make_ref()

    {pid, mon} =
      spawn_monitor(fn ->
        result =
          try do
            AMQP.Connection.open(url)
          catch
            kind, reason -> {:error, {kind, reason}}
          end

        send(parent, {ref, result})
      end)

    receive do
      {^ref, result} ->
        Process.demonitor(mon, [:flush])
        result

      {:DOWN, ^mon, :process, ^pid, reason} ->
        {:error, {:exit, reason}}
    after
      timeout_ms ->
        Process.exit(pid, :kill)
        Process.demonitor(mon, [:flush])
        {:error, :timeout}
    end
  end

  defp safe_close(conn) do
    AMQP.Connection.close(conn)
  catch
    _, _ -> :ok
  end
end
