# phi_accrual_amqp

Dedicated AMQP consumer source for [`phi_accrual`](https://hex.pm/packages/phi_accrual). Treats broker deliveries on a configured queue as liveness signals.

> WARNING **Alpha — `v0.1.x`.** Public API and telemetry schema may change before `v1.0` based on real-deployment feedback.

## Why a separate package

The core `phi_accrual` library is intentionally transport-agnostic. Heartbeat transports live in their own packages so consumers can mix and match — UDP for decision-grade detection with no intermediary, BEAM distribution for observability-grade, AMQP when broker-mediated traffic is already the system's backbone. See the [phi_accrual roadmap](https://hexdocs.pm/phi_accrual/readme.html#roadmap) for the ecosystem rationale.

## Consumer-only by design

AMQP applications usually already publish messages that prove node liveness. A dedicated heartbeat publisher would conflate **broker liveness** with **node liveness** — the broker stays healthy, your synthetic tick keeps flowing, phi stays low, even if the producer is sending nothing of substance. So this package ships a `Consumer` and nothing else. Use your existing application traffic as the heartbeat signal, or reach for [`phi_accrual_udp`](https://hex.pm/packages/phi_accrual_udp) when you need a transport with no intermediary.

## Quick start

```elixir
# mix.exs
def deps do
  [
    {:phi_accrual, "~> 1.0"},
    {:phi_accrual_amqp, "~> 0.1"}
  ]
end
```

In your supervision tree:

```elixir
children = [
  {PhiAccrualAmqp.Consumer,
    url: "amqp://guest:guest@rabbit/",
    queue: "phi.heartbeats"}
]
```

Topology — exchange declaration, queue declaration, bindings — is your application's responsibility. The consumer subscribes to an existing queue.

## Mapping deliveries to detector keys

The detector key is what gets passed to `PhiAccrual.observe/2`. It is extracted from the delivery envelope by a `:key_resolver` function — `(meta -> term() | nil)`.

**Default**: `meta.routing_key`. Fits the common topic-exchange topology where heartbeats are published with `heartbeat.<node>` and a single queue bound with `#` fans them all in.

**Custom resolvers** for other topologies:

```elixir
# One queue per node — fixed key per Consumer instance
{PhiAccrualAmqp.Consumer,
  queue: "heartbeats.node_a",
  key_resolver: fn _meta -> :node_a end}

# Identity in a header
{PhiAccrualAmqp.Consumer,
  queue: "heartbeats",
  key_resolver: fn meta ->
    case meta[:headers] do
      [{"node", :longstr, name} | _] -> name
      _ -> nil
    end
  end}

# Identity in app_id property
{PhiAccrualAmqp.Consumer,
  queue: "heartbeats",
  key_resolver: fn %{app_id: id} -> id end}
```

Returning `nil` drops the delivery with a `[:phi_accrual_amqp, :extract, :error]` telemetry event (`reason: :no_detector_key`). Resolver exceptions are caught (`reason: :resolver_raised`).

## Clock discipline

The receiver does **not** use any envelope timestamp for the EWMA — it uses local monotonic receipt time, preserving `phi_accrual`'s clock discipline. The publisher's `BasicProperties.timestamp` (and any broker-stamped header) is emitted as diagnostic-only telemetry. AMQP 0-9-1 does not specify the unit for that field; this package passes the integer through untouched.

## Liveness semantics caveat

In AMQP, "delivery received" proves three things are alive in combination: publisher, broker, and the network paths between them and you. A rising phi value does **not** pin the fault on the publisher. If you need publisher-only liveness, choose a transport with no intermediary.

## Connection lifecycle

The consumer manages its own connection, channel, and subscription. On startup it schedules an async connect so the supervisor can come up before the broker is reachable. On any failure — broker unreachable, channel error, server-initiated `basic.cancel`, connection or channel process death — it tears down what it has and reconnects with exponential backoff between `:reconnect_min_ms` (default 1s) and `:reconnect_max_ms` (default 30s). This deliberately differs from `phi_accrual_udp`'s fail-fast `:gen_udp.open` — AMQP connections are remote-broker contracts that blip during normal operation.

## Telemetry

```
[:phi_accrual_amqp, :connection, :up]
  metadata: %{queue}

[:phi_accrual_amqp, :connection, :down]
  metadata: %{queue, reason}

[:phi_accrual_amqp, :consumer, :registered]
  metadata: %{queue, consumer_tag}

[:phi_accrual_amqp, :consumer, :cancelled]
  metadata: %{queue, consumer_tag, reason}

[:phi_accrual_amqp, :sample, :received]
  measurements: %{}
  metadata:     %{detector_key, envelope_timestamp, routing_key, exchange, queue}
  # envelope_timestamp may be nil; never the value passed to PhiAccrual.observe/2

[:phi_accrual_amqp, :extract, :error]
  metadata: %{reason, routing_key, exchange, queue}
  # reason ∈ [:no_detector_key, :resolver_raised]
```

### Cross-transport note

The `[:phi_accrual_amqp, :sample, :received]` event shares its name with
`[:phi_accrual_udp, :sample, :received]`, but the payloads are **not**
interchangeable — a telemetry handler written for one transport will not
work unchanged against the other:

- **Identity key.** `phi_accrual_amqp` reports the monitored entity under
  `metadata.detector_key`, matching the `t:PhiAccrual.detector_key/0` type in
  `phi_accrual` core. `phi_accrual_udp` reports it under `metadata.node`.
- **Diagnostic timestamp.** `phi_accrual_amqp` places it in `metadata`
  (`envelope_timestamp`, nullable). `phi_accrual_udp` 1.x places it in
  `measurements` (`packet_timestamp_ms`).

These differences are deliberate, not oversights: `phi_accrual_amqp` uses
`detector_key` because an AMQP source has no Erlang node, and keeps the
nullable timestamp out of `measurements` so numeric aggregators are not fed
`nil`. Convergence of the two transports' telemetry payloads is tracked for
`phi_accrual_udp` 2.0.

## Running the tests

Unit tests are broker-free and fast:

```sh
mix test
```

Integration tests need a running RabbitMQ broker and are excluded by default. Start a broker with the bundled compose file, then opt them in:

```sh
docker compose -f docker-compose.test.yml up -d --wait
mix test --include integration
docker compose -f docker-compose.test.yml down
```

Or, equivalent:

```sh
mix test.all          # unit + integration
mix test.integration  # integration only
```

`RABBITMQ_URL` overrides the default `amqp://localhost`. If no broker is reachable, the integration tests skip rather than fail.

Requirements: OTP 28 / Elixir 1.19 (the `amqp` 4.x line — the 3.x line does not build on OTP 28).

## License

Apache-2.0.
