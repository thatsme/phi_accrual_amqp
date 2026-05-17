defmodule PhiAccrualAmqp.ConsumerTest do
  use ExUnit.Case, async: false

  alias PhiAccrualAmqp.Consumer

  # These tests exercise the delivery-handling and lifecycle-telemetry
  # paths without a live broker, by starting the Consumer in
  # `connect: false` mode and sending it the same `:basic_deliver` /
  # `:basic_consume_ok` / `:basic_cancel` messages the `:amqp` library
  # would send. End-to-end tests against a real broker live elsewhere
  # (and require a running RabbitMQ).

  defp subscribe(events) do
    ref = make_ref()
    id = "consumer-test-#{inspect(ref)}"

    :ok =
      :telemetry.attach_many(
        id,
        events,
        fn ev, m, md, {pid, r} -> send(pid, {:event, r, ev, m, md}) end,
        {self(), ref}
      )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(id) end)
    ref
  end

  defp start_offline(opts \\ []) do
    name = :"consumer_#{System.unique_integer([:positive])}"

    {:ok, pid} =
      start_supervised(
        {Consumer,
         [
           name: name,
           queue: Keyword.get(opts, :queue, "test.q"),
           connect: false
         ] ++ Keyword.drop(opts, [:queue])}
      )

    pid
  end

  describe "delivery handling" do
    test "extracts routing_key and emits :sample :received with detector_key" do
      pid = start_offline()
      ref = subscribe([[:phi_accrual_amqp, :sample, :received]])

      send(pid, {:basic_deliver, "payload-ignored",
                 %{routing_key: "heartbeat.node_a", exchange: "ha", timestamp: 42}})

      assert_receive {:event, ^ref, _, %{},
                      %{detector_key: "heartbeat.node_a",
                        envelope_timestamp: 42,
                        routing_key: "heartbeat.node_a",
                        exchange: "ha",
                        queue: "test.q"}}, 500
    end

    test "envelope_timestamp is nil when publisher set none" do
      pid = start_offline()
      ref = subscribe([[:phi_accrual_amqp, :sample, :received]])

      send(pid, {:basic_deliver, "p", %{routing_key: "x", exchange: ""}})

      assert_receive {:event, ^ref, _, %{}, %{envelope_timestamp: nil}}, 500
    end

    test "custom key_resolver overrides default" do
      pid = start_offline(key_resolver: fn _meta -> :node_b end)
      ref = subscribe([[:phi_accrual_amqp, :sample, :received]])

      send(pid, {:basic_deliver, "p", %{routing_key: "ignored", exchange: ""}})

      assert_receive {:event, ^ref, _, _, %{detector_key: :node_b}}, 500
    end

    test "emits :extract :error with :no_detector_key when resolver returns nil" do
      pid = start_offline(key_resolver: fn _ -> nil end)
      ref = subscribe([[:phi_accrual_amqp, :extract, :error]])

      send(pid, {:basic_deliver, "p", %{routing_key: "", exchange: ""}})

      assert_receive {:event, ^ref, _, _,
                      %{reason: :no_detector_key, queue: "test.q"}}, 500
    end

    test "emits :extract :error with :resolver_raised when resolver throws" do
      pid = start_offline(key_resolver: fn _ -> raise "boom" end)
      ref = subscribe([[:phi_accrual_amqp, :extract, :error]])

      send(pid, {:basic_deliver, "p", %{routing_key: "x", exchange: ""}})

      assert_receive {:event, ^ref, _, _, %{reason: :resolver_raised}}, 500
    end

    test "calls PhiAccrual.observe/2 with local receipt time, not envelope timestamp" do
      key = {:test_node, System.unique_integer([:positive])}
      pid = start_offline(key_resolver: fn _ -> key end)

      # Wildly skewed envelope timestamp. The detector should NOT see it;
      # it should see local monotonic receipt time. We can't observe the
      # exact value, but we can confirm the key was tracked at all.
      send(pid, {:basic_deliver, "p",
                 %{routing_key: "x", exchange: "", timestamp: 0}})

      Process.sleep(50)

      assert key in PhiAccrual.tracked_nodes(),
             "expected #{inspect(key)} in tracked nodes, got: " <>
               inspect(PhiAccrual.tracked_nodes())
    end
  end

  describe "lifecycle telemetry" do
    test ":consumer :registered fires on basic_consume_ok" do
      pid = start_offline()
      ref = subscribe([[:phi_accrual_amqp, :consumer, :registered]])

      send(pid, {:basic_consume_ok, %{consumer_tag: "ctag-1"}})

      assert_receive {:event, ^ref, _, _,
                      %{queue: "test.q", consumer_tag: "ctag-1"}}, 500
    end

    test ":consumer :cancelled fires on server-initiated basic_cancel" do
      pid =
        start_offline(
          reconnect_min_ms: 60_000,
          reconnect_max_ms: 60_000
        )

      ref = subscribe([[:phi_accrual_amqp, :consumer, :cancelled]])

      send(pid, {:basic_cancel, %{consumer_tag: "ctag-1"}})

      assert_receive {:event, ^ref, _, _,
                      %{queue: "test.q",
                        consumer_tag: "ctag-1",
                        reason: :server_cancelled}}, 500
    end

    test "basic_cancel_ok is benign" do
      pid = start_offline()
      send(pid, {:basic_cancel_ok, %{consumer_tag: "ctag-1"}})
      Process.sleep(20)
      assert Process.alive?(pid)
    end

    test "unknown messages are ignored, consumer stays alive" do
      pid = start_offline()
      send(pid, {:something_random, 1, 2, 3})
      Process.sleep(20)
      assert Process.alive?(pid)
    end
  end
end
