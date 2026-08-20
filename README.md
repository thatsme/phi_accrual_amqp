# phi_accrual_amqp

Dedicated AMQP consumer source for [`phi_accrual`](https://hex.pm/packages/phi_accrual). Treats broker deliveries on a configured queue as liveness signals.

> WARNING **Alpha — `v0.x`.** Public API and telemetry schema may change before `v1.0` based on real-deployment feedback.

> **Protocol: AMQP 0-9-1.** This transport uses AMQP 0-9-1 (the RabbitMQ
> protocol) via the `amqp` client library. It works with **RabbitMQ** and
> other AMQP 0-9-1 brokers. It does **not** work with AMQP 1.0 brokers such
> as ActiveMQ Artemis, Apache Qpid, Azure Service Bus, or Solace — AMQP 1.0
> is a different, incompatible protocol. A 1.0 transport, if it ever exists,
> would be a separate package.

## Why a separate package

The core `phi_accrual` library is intentionally transport-agnostic. Heartbeat transports live in their own packages so consumers can mix and match — UDP for decision-grade detection with no intermediary, BEAM distribution for observability-grade, AMQP when broker-mediated traffic is already the system's backbone. See the [phi_accrual roadmap](https://hexdocs.pm/phi_accrual/readme.html#roadmap) for the ecosystem rationale. That list names `phi_accrual_udp` and a planned `phi_accrual_libcluster`; it predates this package, so the omission is chronology rather than exclusion.

## Consumer-only by design

AMQP applications usually already publish messages that prove node liveness. A dedicated heartbeat publisher would conflate **broker liveness** with **node liveness** — the broker stays healthy, your synthetic tick keeps flowing, phi stays low, even if the producer is sending nothing of substance. So this package ships a `Consumer` and nothing else. Use your existing application traffic as the heartbeat signal, or reach for [`phi_accrual_udp`](https://hex.pm/packages/phi_accrual_udp) when you need a transport with no intermediary.

## Quick start

```elixir
# mix.exs
def deps do
  [
    {:phi_accrual, "~> 1.1"},
    {:phi_accrual_amqp, "~> 0.2"}
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

`:connection_opts` takes precedence over `:url`: when it is set, `:url` is ignored entirely rather than merged. A keyword list passed there is merged over the connection defaults, so anything given wins. The full option list, including `:connect`, is documented on `PhiAccrualAmqp.Consumer`.

## Running several consumers

`Consumer` builds its own child specification, so one consumer per queue can sit in the same supervision tree with no further ceremony:

```elixir
children = [
  {PhiAccrualAmqp.Consumer, queue: "heartbeats.node_a"},
  {PhiAccrualAmqp.Consumer, queue: "heartbeats.node_b"}
]
```

The child `:id` defaults to `{PhiAccrualAmqp.Consumer, queue}`, or to `:name` when one is given. The standard supervisor keys `:id`, `:restart` and `:shutdown` are read from the same keyword list as the consumer options and are not forwarded to `start_link/1`.

A consumer runs unnamed unless `:name` is passed. A name is only needed for processes that application code addresses directly.

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

The consumer manages its own connection, channel, and subscription. On startup it schedules an async connect so the supervisor can come up before the broker is reachable. On any failure — broker unreachable, channel error, server-initiated `basic.cancel`, connection or channel process death — it tears down what it has and reconnects with jittered exponential backoff between `:reconnect_min_ms` (default 1s) and `:reconnect_max_ms` (default 30s). The ceiling doubles per attempt; the delay is drawn uniformly between the floor and that ceiling, so a fleet of consumers attached to a restarting broker spreads its retries instead of stampeding in lockstep. This deliberately differs from `phi_accrual_udp`'s fail-fast `:gen_udp.open` — AMQP connections are remote-broker contracts that blip during normal operation.

## What a disconnect means for the detector

When the connection drops, the consumer stops feeding `PhiAccrual.observe/2` and phi for the affected keys climbs. That is the detector answering its question correctly — nothing has been heard from those entities. It is not a malfunction, and the consumer does not attempt to correct it.

The consumer deliberately takes no action on the estimators:

- It does not own them. `PhiAccrual.observe/2` auto-tracks, so the estimator is materialised by core rather than by this package — and a UDP listener, a `DistributionPing` source, or application code may be feeding the same key from another angle.
- The only lever core exposes is `PhiAccrual.untrack/1`, which terminates the estimator and destroys its calibration: mean, variance and sample count alike. Applying that to a forty-second broker blip forces a rebuild from `:insufficient_data`, a worse outcome than the phi excursion it would avoid.

Core is positioned as observability-grade, with thresholding and policy left to the consuming application. The consumer's obligation is therefore legibility, not correction: `[:phi_accrual_amqp, :connection, :down]` carries the `keys` that were being fed, which is what a policy layer needs in order to read a phi excursion on those keys as a transport outage rather than as evidence about the entities themselves.

An application that genuinely wants estimators torn down on disconnect can attach a handler to that event and call `PhiAccrual.untrack/1` itself. It is not offered as configuration, because the sharp edge belongs with the application that chose it.

A server-initiated `basic.cancel` counts as a disconnect for this purpose. The consumer tears the connection down and reconnects, so `[:phi_accrual_amqp, :connection, :down]` fires with `reason: :server_cancelled` alongside the `[:consumer, :cancelled]` event that carries the consumer tag. Keys appear on the connection event only, so a policy layer attaches to one name and never has to reconcile overlapping key sets.

### Bounding the tracked-key set

The keys reported on disconnect are those the consumer has seen deliveries for, capped by `:max_tracked_keys` (default 1000). The cap matters because the default resolver returns the routing key: a wildcard binding can mint unbounded distinct keys. At the cap, the least-recently-seen key is evicted and `[:phi_accrual_amqp, :keys, :evicted]` fires — a signal that the binding or the resolver is broader than intended.


## Inspecting a consumer

`PhiAccrualAmqp.Consumer.status/2` reports what a health endpoint needs:

```elixir
%{
  connected?: true,
  queue: "phi.heartbeats",
  consumer_tag: "amq.ctag-...",
  backoff_ms: 1000,
  disconnected_since: nil,
  last_delivery_at: -576460733,
  keys_tracked: 12
}
```

`:disconnected_since` and `:last_delivery_at` are local monotonic milliseconds from the same clock as `:erlang.monotonic_time(:millisecond)`; durations come from subtracting them from a fresh reading. They are not wall clocks and mean nothing off this node.

Connection attempts run synchronously inside the consumer, so a call that lands during one waits for it to finish. The timeout argument defaults to 5000 rather than `:infinity` for that reason, and callers should expect the exit — a health check is exactly the caller most likely to arrive mid-outage.

## Connection defaults

Connections are opened with `heartbeat: 10` and `connection_timeout: 5_000`. The heartbeat matches the AMQP client's own default and is set explicitly so it stays pinned. The timeout is a deliberate tightening: the client otherwise allows 60s via a URI and 50s via a keyword list, which is how long a connection attempt — and any `status/2` call waiting behind it — can block against a broker that accepts packets but never completes the handshake. Both are overridden by a keyword list passed as `:connection_opts`, which is merged over them. A binary `:connection_opts` is a URL rather than a keyword list, so there is nothing to merge and the defaults stand — and because the client resolves explicit options ahead of URI query parameters, they also win over a `heartbeat` or `connection_timeout` embedded in the URL itself. A keyword list is the only form that can change them.

## Option validation

`start_link/1` validates its options and raises `ArgumentError` before the process starts. Unknown keys are rejected rather than ignored: a mistyped `:reconnect_min` would otherwise pass through `Keyword.get/3` and silently yield the default, which is the failure mode that costs an afternoon. Types are checked, and a `:reconnect_min_ms` above `:reconnect_max_ms` is refused rather than quietly clamped.

Validation is hand-rolled rather than delegated to an options library. Nine flat options with no nesting do not justify a fourth runtime dependency on a package whose argument is that the core stays small by choice.

## Telemetry

```
[:phi_accrual_amqp, :connection, :up]
  measurements: %{system_time}
  metadata:     %{queue}

[:phi_accrual_amqp, :connection, :down]
  measurements: %{tracked}
  metadata:     %{queue, reason, keys}
  # tracked counts what keys lists: the list is for policy, the count for a gauge
  # also fires on a server-initiated cancel, with reason: :server_cancelled
  # keys: the detector keys this consumer was feeding when delivery stopped

[:phi_accrual_amqp, :keys, :evicted]
  measurements: %{tracked}
  metadata:     %{queue, key, incoming_key, max_tracked_keys}

[:phi_accrual_amqp, :consumer, :registered]
  measurements: %{system_time}
  metadata:     %{queue, consumer_tag}

[:phi_accrual_amqp, :consumer, :cancelled]
  measurements: %{system_time}
  metadata:     %{queue, consumer_tag, reason}

[:phi_accrual_amqp, :sample, :received]
  measurements: %{monotonic_time, system_time}
  # monotonic_time is the exact value handed to PhiAccrual.observe/2
  metadata:     %{detector_key, envelope_timestamp, routing_key, exchange, queue}
  # envelope_timestamp may be nil; never the value passed to PhiAccrual.observe/2

[:phi_accrual_amqp, :extract, :error]
  measurements: %{system_time}
  metadata:     %{reason, routing_key, exchange, queue}
  # reason ∈ [:no_detector_key, :resolver_raised]
```

### Cross-transport note

The `[:phi_accrual_amqp, :sample, :received]` event shares its name with
`[:phi_accrual_udp, :sample, :received]`, but the payloads are **not**
interchangeable — a telemetry handler written for one transport will not
work unchanged against the other:

- **Identity key.** `phi_accrual_amqp` reports the monitored entity under
  `metadata.detector_key`. Both `phi_accrual_udp` and `phi_accrual` core
  itself use `metadata.node` — core emits `%{node, local_pause?}` on
  `[:phi_accrual, :sample, :observed]`. This package is the one that
  departs, deliberately: an AMQP source has no Erlang node, and
  `detector_key` is the honest name for what the key actually holds. The
  value type is unchanged — it is still a `t:PhiAccrual.detector_key/0`.
- **Diagnostic timestamp.** `phi_accrual_amqp` places it in `metadata`
  (`envelope_timestamp`, nullable). `phi_accrual_udp` 1.x places it in
  `measurements` (`packet_timestamp_ms`).

These differences are deliberate, and they are not scheduled to converge.
The timestamp stays out of `measurements` because it is nullable, and numeric
aggregators should not be fed `nil`.

The transports are specializations of a transport-agnostic core rather than
interchangeable adapters. The only contract binding them together is
`PhiAccrual.observe/2`; each package's telemetry describes what its own
transport actually carries, and per-transport handlers are the intended
model rather than a gap awaiting closure.

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

Requirements: Elixir `~> 1.15`, as declared in `mix.exs` and exercised in CI
against Elixir 1.15.8 / OTP 26.2. Releases are built and verified on Elixir
1.19 / OTP 28, which CI also covers; that combination requires the `amqp` 4.x
line, since `amqp` 3.x does not build on OTP 28.

## License

Apache-2.0.
