defmodule PhiAccrualAmqp.ConsumerIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias PhiAccrualAmqp.Consumer
  alias PhiAccrualAmqp.Test.Broker

  setup context do
    if Broker.available?() do
      url = Broker.url()
      {:ok, conn} = AMQP.Connection.open(url)
      {:ok, ch} = AMQP.Channel.open(conn)

      suffix =
        "#{context.test |> Atom.to_string() |> String.replace(~r/\W/, "_")}_#{System.unique_integer([:positive])}"

      {exchange, queue} = Broker.declare_topology(ch, suffix)

      on_exit(fn ->
        try do
          Broker.cleanup(ch, exchange, queue)
          AMQP.Channel.close(ch)
          AMQP.Connection.close(conn)
        catch
          _, _ -> :ok
        end
      end)

      %{broker: true, exchange: exchange, queue: queue, channel: ch, url: url}
    else
      # ExUnit in Elixir 1.19 does not accept {:skip, _} from setup,
      # so we mark the context and gate each test body via
      # `with_broker/2`, which logs and short-circuits.
      %{broker: false, skip_reason: "no broker reachable at #{Broker.url()}"}
    end
  end

  # ExUnit 1.19 has no runtime skip API (filtering happens before setup
  # in ExUnit.Runner.prepare_tests/2; only compile-time @tag :skip
  # marks a test as :skipped in the run summary). A compile-time
  # broker probe would be cached across runs and produce stale skip
  # state. So the integration suite uses a context flag set in setup,
  # and this guard:
  #
  #   - broker: true  → run the test body
  #   - broker: false + binary skip_reason → log and pass
  #     (implicit pass, but explicitly flagged; no broker-summary
  #     visibility is possible in 1.19)
  #   - anything else → flunk, because a context shaped that way
  #     means setup is buggy and an implicit pass would silently
  #     hide a broken integration test
  defp with_broker(%{broker: true} = ctx, fun), do: fun.(ctx)

  defp with_broker(%{broker: false, skip_reason: reason}, _fun) when is_binary(reason) do
    IO.puts("\n  [SKIP] #{reason}")
    :ok
  end

  defp with_broker(ctx, _fun) do
    flunk("""
    with_broker/2 entered with an invalid context.

    This means setup neither produced a live broker session nor a
    valid skip flag. An integration test would have implicit-passed
    silently. Treating as a hard failure to prevent that.

    Context: #{inspect(ctx)}
    """)
  end

  defp attach_telemetry(events) do
    ref = make_ref()
    id = "integration-#{inspect(ref)}"
    test_pid = self()

    :ok =
      :telemetry.attach_many(
        id,
        events,
        fn ev, m, md, _ -> send(test_pid, {:event, ref, ev, m, md}) end,
        nil
      )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
    ref
  end

  defp wait_registered(ref, timeout \\ 3_000) do
    assert_receive {:event, ^ref, [:phi_accrual_amqp, :consumer, :registered], _, _}, timeout
  end

  test "delivery feeds the detector with default resolver (proves amqp 4.x meta shape)",
       ctx do
    with_broker(ctx, fn %{exchange: ex, queue: q, channel: ch, url: url} ->
      ref =
        attach_telemetry([
          [:phi_accrual_amqp, :consumer, :registered],
          [:phi_accrual_amqp, :sample, :received]
        ])

      {:ok, _pid} = start_supervised({Consumer, url: url, queue: q})
      wait_registered(ref)

      routing_key = "heartbeat.node_x"
      :ok = AMQP.Basic.publish(ch, ex, routing_key, "")

      assert_receive {:event, ^ref, [:phi_accrual_amqp, :sample, :received], %{},
                      %{
                        detector_key: ^routing_key,
                        routing_key: ^routing_key,
                        exchange: ^ex,
                        queue: ^q
                      }},
                     3_000
    end)
  end

  test "custom resolver extracts app_id from envelope", ctx do
    with_broker(ctx, fn %{exchange: ex, queue: q, channel: ch, url: url} ->
      ref =
        attach_telemetry([
          [:phi_accrual_amqp, :consumer, :registered],
          [:phi_accrual_amqp, :sample, :received]
        ])

      resolver = fn %{app_id: id} -> id end

      {:ok, _pid} =
        start_supervised({Consumer, url: url, queue: q, key_resolver: resolver})

      wait_registered(ref)

      :ok = AMQP.Basic.publish(ch, ex, "any.key", "", app_id: "node_y")

      assert_receive {:event, ^ref, [:phi_accrual_amqp, :sample, :received], %{},
                      %{detector_key: "node_y"}},
                     3_000
    end)
  end

  test "extract error fires when resolver returns nil", ctx do
    with_broker(ctx, fn %{exchange: ex, queue: q, channel: ch, url: url} ->
      ref =
        attach_telemetry([
          [:phi_accrual_amqp, :consumer, :registered],
          [:phi_accrual_amqp, :extract, :error]
        ])

      resolver = fn _ -> nil end

      {:ok, _pid} =
        start_supervised({Consumer, url: url, queue: q, key_resolver: resolver})

      wait_registered(ref)

      :ok = AMQP.Basic.publish(ch, ex, "any.key", "")

      assert_receive {:event, ^ref, [:phi_accrual_amqp, :extract, :error], _,
                      %{reason: :no_detector_key, queue: ^q}},
                     3_000
    end)
  end

  # Note: this test produces a visible `[error] GenServer ... terminating
  # ** (stop) :unexpected_delivery_and_no_default_consumer` log from
  # amqp_client's SelectiveConsumer process. That is an expected,
  # tolerated race: when we Process.exit/2 the channel pid, the broker
  # has already enqueued a basic.cancel for our consumer tag, which is
  # delivered into a SelectiveConsumer state that no longer has the
  # consumer registered. The log is harmless — the assertion below
  # proves the Consumer recovers cleanly and re-registers. We
  # deliberately do NOT capture_log here: the noise is part of the
  # signal that the channel really died, and silencing it could mask a
  # genuine consumer crash in future regressions.
  test "consumer reconnects after channel loss", ctx do
    with_broker(ctx, fn %{queue: q, url: url} ->
      ref =
        attach_telemetry([
          [:phi_accrual_amqp, :consumer, :registered],
          [:phi_accrual_amqp, :connection, :down]
        ])

      {:ok, pid} =
        start_supervised({Consumer, url: url, queue: q, reconnect_min_ms: 200})

      wait_registered(ref)

      %{channel: ch} = :sys.get_state(pid)
      Process.exit(ch.pid, :kill)

      assert_receive {:event, ^ref, [:phi_accrual_amqp, :connection, :down], _, _},
                     3_000

      # Clean re-registration after the death + backoff + reconnect.
      # This is the assertion that proves recovery; the noisy log above
      # is not a failure signal.
      assert_receive {:event, ^ref, [:phi_accrual_amqp, :consumer, :registered], _, _},
                     5_000
    end)
  end
end
