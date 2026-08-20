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

      send(
        pid,
        {:basic_deliver, "payload-ignored",
         %{routing_key: "heartbeat.node_a", exchange: "ha", timestamp: 42}}
      )

      assert_receive {:event, ^ref, _, %{},
                      %{
                        detector_key: "heartbeat.node_a",
                        envelope_timestamp: 42,
                        routing_key: "heartbeat.node_a",
                        exchange: "ha",
                        queue: "test.q"
                      }},
                     500
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

      assert_receive {:event, ^ref, _, _, %{reason: :no_detector_key, queue: "test.q"}},
                     500
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
      send(pid, {:basic_deliver, "p", %{routing_key: "x", exchange: "", timestamp: 0}})

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

      assert_receive {:event, ^ref, _, _, %{queue: "test.q", consumer_tag: "ctag-1"}},
                     500
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
                      %{queue: "test.q", consumer_tag: "ctag-1", reason: :server_cancelled}},
                     500
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

  describe "child_spec/1" do
    test "defaults :id to {Consumer, queue} so per-queue children are distinct" do
      assert %{id: {Consumer, "heartbeats.node_a"}} =
               Consumer.child_spec(queue: "heartbeats.node_a")

      assert %{id: {Consumer, "heartbeats.node_b"}} =
               Consumer.child_spec(queue: "heartbeats.node_b")
    end

    test "defaults :id to :name when one is given" do
      assert %{id: :hb_a} = Consumer.child_spec(name: :hb_a, queue: "q")
    end

    test "honors :id, :restart and :shutdown" do
      spec =
        Consumer.child_spec(
          queue: "q",
          id: :custom,
          restart: :transient,
          shutdown: 1_000
        )

      assert %{id: :custom, restart: :transient, shutdown: 1_000, type: :worker} = spec
    end

    test "supervisor keys are not forwarded to start_link/1" do
      %{start: {Consumer, :start_link, [start_opts]}} =
        Consumer.child_spec(queue: "q", id: :custom, restart: :transient, shutdown: 1_000)

      assert start_opts == [queue: "q"]
    end

    test "defaults match use GenServer" do
      assert %{restart: :permanent, shutdown: 5_000, type: :worker} =
               Consumer.child_spec(queue: "q")
    end

    test "two consumers on different queues start under one supervisor" do
      children = [
        {Consumer, queue: "heartbeats.node_a", connect: false},
        {Consumer, queue: "heartbeats.node_b", connect: false}
      ]

      assert {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one)

      assert length(Supervisor.which_children(sup)) == 2
    end
  end

  describe "start_link/1 naming" do
    test "runs unnamed when no :name is given, so instances do not collide" do
      assert {:ok, a} = Consumer.start_link(queue: "q.a", connect: false)
      assert {:ok, b} = Consumer.start_link(queue: "q.b", connect: false)

      assert a != b
      refute Process.whereis(Consumer)
    end

    test "registers under :name when given" do
      name = :"consumer_named_#{System.unique_integer([:positive])}"
      assert {:ok, pid} = Consumer.start_link(name: name, queue: "q", connect: false)

      assert Process.whereis(name) == pid
    end
  end

  describe "backoff_delay/3" do
    test "first attempt waits exactly the floor" do
      assert {1_000, 2_000} = Consumer.backoff_delay(0, 1_000, 30_000)
    end

    test "ceiling doubles per attempt and caps at max" do
      assert {_, 2_000} = Consumer.backoff_delay(1_000, 1_000, 30_000)
      assert {_, 4_000} = Consumer.backoff_delay(2_000, 1_000, 30_000)
      assert {_, 30_000} = Consumer.backoff_delay(16_000, 1_000, 30_000)
      assert {_, 30_000} = Consumer.backoff_delay(30_000, 1_000, 30_000)
    end

    test "delay always lands within the floor and the current ceiling" do
      for _ <- 1..500 do
        {delay, _next} = Consumer.backoff_delay(8_000, 1_000, 30_000)
        assert delay >= 1_000
        assert delay <= 8_000
      end
    end

    test "delay never exceeds max even when backoff overshoots it" do
      for _ <- 1..200 do
        {delay, next} = Consumer.backoff_delay(90_000, 1_000, 30_000)
        assert delay <= 30_000
        assert next == 30_000
      end
    end

    test "draws are spread rather than identical" do
      delays =
        for _ <- 1..200, into: MapSet.new() do
          {delay, _} = Consumer.backoff_delay(20_000, 1_000, 30_000)
          delay
        end

      # Undithered doubling would collapse this to a single value.
      assert MapSet.size(delays) > 50
    end

    # start_link/1 now rejects min > max, so this is defence in depth for
    # direct callers rather than the de-facto validation it once was.
    test "degenerate config where min exceeds max still yields the floor" do
      assert {5_000, 5_000} = Consumer.backoff_delay(0, 5_000, 1_000)
    end
  end

  describe "disconnect legibility" do
    defp deliver(pid, routing_key) do
      send(pid, {:basic_deliver, "p", %{routing_key: routing_key, exchange: "x"}})
    end

    # Nothing listens on port 1, so :connect fails fast and drives the
    # real connection-down path rather than a synthesized :DOWN message.
    defp start_unreachable(opts) do
      start_offline(
        [
          connection_opts: [host: "127.0.0.1", port: 1, connection_timeout: 200],
          # park the retry beyond the test's lifetime; min and max must
          # agree or validation rejects the pair
          reconnect_min_ms: 60_000,
          reconnect_max_ms: 60_000
        ] ++ opts
      )
    end

    test ":connection :down carries the keys this consumer was feeding" do
      pid = start_unreachable([])
      ref = subscribe([[:phi_accrual_amqp, :connection, :down]])

      deliver(pid, "heartbeat.node_a")
      deliver(pid, "heartbeat.node_b")
      # let the deliveries land before provoking the disconnect
      _ = :sys.get_state(pid)

      send(pid, :connect)

      assert_receive {:event, ^ref, [:phi_accrual_amqp, :connection, :down], %{},
                      %{queue: "test.q", keys: keys}},
                     8_000

      assert Enum.sort(keys) == ["heartbeat.node_a", "heartbeat.node_b"]
    end

    test "keys is empty before any delivery has been seen" do
      pid = start_unreachable([])
      ref = subscribe([[:phi_accrual_amqp, :connection, :down]])

      send(pid, :connect)

      assert_receive {:event, ^ref, _, %{}, %{keys: []}}, 8_000
    end

    test "the consumer never untracks keys it does not own" do
      pid = start_unreachable([])
      deliver(pid, "heartbeat.node_owned")
      _ = :sys.get_state(pid)

      send(pid, :connect)
      _ = :sys.get_state(pid)

      # untrack/1 would destroy the estimator's calibration; the
      # consumer must leave it standing so phi accrues honestly.
      assert "heartbeat.node_owned" in PhiAccrual.tracked_nodes()
    end

    test "deliveries that fail extraction do not enter the key set" do
      pid = start_unreachable([])
      ref = subscribe([[:phi_accrual_amqp, :connection, :down]])

      send(pid, {:basic_deliver, "p", %{routing_key: "", exchange: "x"}})
      _ = :sys.get_state(pid)

      send(pid, :connect)

      assert_receive {:event, ^ref, _, %{}, %{keys: []}}, 8_000
    end
  end

  describe "tracked-key bound" do
    test "the key set is capped and evicts least-recently-seen" do
      pid = start_offline(max_tracked_keys: 2)
      ref = subscribe([[:phi_accrual_amqp, :keys, :evicted]])

      deliver(pid, "k.a")
      deliver(pid, "k.b")
      _ = :sys.get_state(pid)
      deliver(pid, "k.c")

      assert_receive {:event, ^ref, [:phi_accrual_amqp, :keys, :evicted], %{tracked: 2},
                      %{key: "k.a", incoming_key: "k.c", max_tracked_keys: 2}},
                     500
    end

    test "re-seeing a known key does not evict anything" do
      pid = start_offline(max_tracked_keys: 2)
      ref = subscribe([[:phi_accrual_amqp, :keys, :evicted]])

      deliver(pid, "k.a")
      deliver(pid, "k.b")
      deliver(pid, "k.a")
      _ = :sys.get_state(pid)

      refute_receive {:event, ^ref, [:phi_accrual_amqp, :keys, :evicted], _, _}, 100
    end

    test "the key set never exceeds the cap under churn" do
      pid = start_offline(max_tracked_keys: 3)

      for n <- 1..50, do: deliver(pid, "k.#{n}")

      state = :sys.get_state(pid)
      assert map_size(state.seen_keys) == 3
    end
  end

  describe "status/2" do
    test "reports a never-connected consumer as disconnected" do
      pid = start_offline()

      assert %{
               connected?: false,
               queue: "test.q",
               consumer_tag: nil,
               last_delivery_at: nil,
               keys_tracked: 0,
               disconnected_since: since
             } = Consumer.status(pid)

      assert is_integer(since)
    end

    test "counts the detector keys seen so far" do
      pid = start_offline()
      deliver(pid, "k.a")
      deliver(pid, "k.b")
      deliver(pid, "k.a")

      assert %{keys_tracked: 2, last_delivery_at: at} = Consumer.status(pid)
      assert is_integer(at)
    end

    test "keys_tracked honours the cap" do
      pid = start_offline(max_tracked_keys: 2)
      for n <- 1..10, do: deliver(pid, "k.#{n}")

      assert %{keys_tracked: 2} = Consumer.status(pid)
    end

    test "a server cancel leaves the consumer marked disconnected" do
      pid = start_offline()
      before = Consumer.status(pid)

      send(pid, {:basic_cancel, %{consumer_tag: "ctag-1"}})
      _ = :sys.get_state(pid)

      assert %{connected?: false, consumer_tag: nil, disconnected_since: since} =
               Consumer.status(pid)

      # the field was already set at init; a cancel must not clear it
      assert is_integer(since)
      assert since >= before.disconnected_since
    end

    test "a server cancel emits :connection :down carrying the keys" do
      pid = start_offline()
      ref = subscribe([[:phi_accrual_amqp, :connection, :down]])

      deliver(pid, "k.cancelled")
      _ = :sys.get_state(pid)

      send(pid, {:basic_cancel, %{consumer_tag: "ctag-1"}})

      assert_receive {:event, ^ref, [:phi_accrual_amqp, :connection, :down], %{},
                      %{reason: :server_cancelled, keys: ["k.cancelled"]}},
                     1_000
    end

    test "a server cancel emits connection down exactly once" do
      pid = start_offline()
      ref = subscribe([[:phi_accrual_amqp, :connection, :down]])

      send(pid, {:basic_cancel, %{consumer_tag: "ctag-1"}})
      _ = :sys.get_state(pid)

      assert_receive {:event, ^ref, _, _, %{reason: :server_cancelled}}, 1_000
      refute_receive {:event, ^ref, _, _, _}, 200
    end

    test "backoff_ms advances as reconnects are scheduled" do
      pid = start_offline(reconnect_min_ms: 1_000, reconnect_max_ms: 30_000)

      send(pid, {:basic_cancel, %{consumer_tag: "ctag-1"}})
      _ = :sys.get_state(pid)

      assert %{backoff_ms: backoff} = Consumer.status(pid)
      assert backoff > 0
    end
  end

  describe "telemetry measurements" do
    test ":sample :received carries the exact value handed to observe/2" do
      key = {:measured_node, System.unique_integer([:positive])}
      pid = start_offline(key_resolver: fn _ -> key end)
      ref = subscribe([[:phi_accrual_amqp, :sample, :received]])

      # A wildly skewed envelope timestamp that must not reach the detector.
      send(pid, {:basic_deliver, "p", %{routing_key: "x", exchange: "", timestamp: 0}})

      assert_receive {:event, ^ref, _, %{monotonic_time: mono, system_time: sys}, _}, 500

      refute mono == 0, "envelope timestamp leaked into the measurement"
      assert is_integer(mono) and is_integer(sys)

      # The measurement is not merely close to the observed value; it is
      # the value, as recorded by the estimator core itself.
      Process.sleep(50)
      assert %{last_arrival_ts: ^mono} = PhiAccrual.inspect_state(key)
    end

    test ":sample :received monotonic_time tracks the local monotonic clock" do
      pid = start_offline()
      ref = subscribe([[:phi_accrual_amqp, :sample, :received]])

      before = :erlang.monotonic_time(:millisecond)
      deliver(pid, "k.clock")

      assert_receive {:event, ^ref, _, %{monotonic_time: mono}, _}, 500

      assert mono >= before
      assert mono <= :erlang.monotonic_time(:millisecond)
    end

    test ":connection :down counts what :keys lists" do
      pid = start_offline()
      ref = subscribe([[:phi_accrual_amqp, :connection, :down]])

      deliver(pid, "k.a")
      deliver(pid, "k.b")
      _ = :sys.get_state(pid)

      send(pid, {:basic_cancel, %{consumer_tag: "ctag-1"}})

      assert_receive {:event, ^ref, _, %{tracked: 2}, %{keys: keys}}, 1_000
      assert length(keys) == 2
    end

    test ":consumer :registered carries system_time" do
      pid = start_offline()
      ref = subscribe([[:phi_accrual_amqp, :consumer, :registered]])

      send(pid, {:basic_consume_ok, %{consumer_tag: "ctag-1"}})

      assert_receive {:event, ^ref, _, %{system_time: sys}, _}, 500
      assert is_integer(sys)
    end

    test ":consumer :cancelled carries system_time" do
      pid = start_offline()
      ref = subscribe([[:phi_accrual_amqp, :consumer, :cancelled]])

      send(pid, {:basic_cancel, %{consumer_tag: "ctag-1"}})

      assert_receive {:event, ^ref, _, %{system_time: sys}, _}, 500
      assert is_integer(sys)
    end

    test ":extract :error carries system_time" do
      pid = start_offline()
      ref = subscribe([[:phi_accrual_amqp, :extract, :error]])

      send(pid, {:basic_deliver, "p", %{routing_key: "", exchange: "x"}})

      assert_receive {:event, ^ref, _, %{system_time: sys}, _}, 500
      assert is_integer(sys)
    end

    test "no event ships an empty measurement map" do
      pid = start_offline()

      ref =
        subscribe([
          [:phi_accrual_amqp, :sample, :received],
          [:phi_accrual_amqp, :extract, :error],
          [:phi_accrual_amqp, :consumer, :cancelled]
        ])

      deliver(pid, "k.a")
      send(pid, {:basic_deliver, "p", %{routing_key: "", exchange: "x"}})
      send(pid, {:basic_cancel, %{consumer_tag: "ctag-1"}})

      for _ <- 1..3 do
        assert_receive {:event, ^ref, _, measurements, _}, 1_000
        refute measurements == %{}
      end
    end
  end

  describe "option validation" do
    test "rejects an unknown option rather than silently defaulting" do
      assert_raise ArgumentError, ~r/unknown option \[:reconnect_min\]/, fn ->
        Consumer.start_link(queue: "q", reconnect_min: 500, connect: false)
      end
    end

    test "names every unknown option it found" do
      assert_raise ArgumentError, ~r/unknown options \[:bogus, :nonsense\]/, fn ->
        Consumer.start_link(queue: "q", bogus: 1, nonsense: 2, connect: false)
      end
    end

    test "requires :queue" do
      assert_raise ArgumentError, ~r/:queue is required/, fn ->
        Consumer.start_link(connect: false)
      end
    end

    test "rejects an empty queue name" do
      assert_raise ArgumentError, ~r/expected :queue to be a non-empty binary/, fn ->
        Consumer.start_link(queue: "", connect: false)
      end
    end

    test "rejects mistyped options" do
      assert_raise ArgumentError, ~r/expected :reconnect_min_ms to be a positive integer/, fn ->
        Consumer.start_link(queue: "q", reconnect_min_ms: 0, connect: false)
      end

      assert_raise ArgumentError, ~r/expected :max_tracked_keys to be a positive integer/, fn ->
        Consumer.start_link(queue: "q", max_tracked_keys: "lots", connect: false)
      end

      assert_raise ArgumentError, ~r/expected :key_resolver to be a function of arity 1/, fn ->
        Consumer.start_link(queue: "q", key_resolver: fn -> :nope end, connect: false)
      end

      assert_raise ArgumentError, ~r/expected :connect to be a boolean/, fn ->
        Consumer.start_link(queue: "q", connect: "false")
      end
    end

    test "rejects a backoff floor above the ceiling" do
      assert_raise ArgumentError, ~r/must not exceed :reconnect_max_ms/, fn ->
        Consumer.start_link(
          queue: "q",
          reconnect_min_ms: 60_000,
          reconnect_max_ms: 30_000,
          connect: false
        )
      end
    end

    test "compares the floor against the default ceiling" do
      assert_raise ArgumentError, ~r/must not exceed :reconnect_max_ms \(30000\)/, fn ->
        Consumer.start_link(queue: "q", reconnect_min_ms: 60_000, connect: false)
      end
    end

    test "accepts the documented option set" do
      assert {:ok, pid} =
               Consumer.start_link(
                 queue: "q",
                 url: "amqp://localhost",
                 key_resolver: fn _ -> :k end,
                 reconnect_min_ms: 100,
                 reconnect_max_ms: 200,
                 max_tracked_keys: 10,
                 connect: false
               )

      assert is_pid(pid)
    end

    test "supervisor keys reach child_spec without tripping validation" do
      children = [{Consumer, queue: "q.sup", connect: false, id: :custom, shutdown: 1_000}]

      assert {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one)
      assert [{:custom, _, :worker, _}] = Supervisor.which_children(sup)
    end
  end
end
