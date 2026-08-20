# Changelog

All notable changes to this project are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-08-20

### Added

- **Option validation in `start_link/1`** — raises `ArgumentError` on
  unknown keys, mistyped values, a missing or empty `:queue`, and a
  `:reconnect_min_ms` above `:reconnect_max_ms`. Unknown keys matter
  most: a mistyped `:reconnect_min` previously passed through
  `Keyword.get/3` and silently yielded the default. Hand-rolled rather
  than delegated to an options library, which would be a fourth runtime
  dependency for nine flat options.
- **`PhiAccrualAmqp.Consumer.status/2`** — reports `:connected?`,
  `:queue`, `:consumer_tag`, `:backoff_ms`, `:disconnected_since`,
  `:last_delivery_at` and `:keys_tracked`. The two timestamps are local
  monotonic milliseconds. Connection attempts run synchronously inside
  the consumer, so the call can block behind one; the `timeout`
  argument defaults to 5000 rather than `:infinity`.
- **`:keys` metadata on `[:phi_accrual_amqp, :connection, :down]`** —
  the detector keys the consumer was feeding when delivery stopped. A
  policy layer can use it to distinguish a phi excursion caused by a
  transport outage from one that is evidence about the entities
  themselves. The consumer takes no action on the estimators: it does
  not own them (`PhiAccrual.observe/2` auto-tracks, and other sources
  may feed the same key), and `PhiAccrual.untrack/1` — core's only
  lever — destroys the estimator's calibration outright.
- **`:max_tracked_keys` option** (default 1000) — bounds the set of
  detector keys a consumer remembers, with least-recently-seen
  eviction. The default resolver returns the routing key, so a
  wildcard binding could otherwise grow the set without limit.
- **`[:phi_accrual_amqp, :keys, :evicted]` telemetry event** —
  measurements `%{tracked}`, metadata
  `%{queue, key, incoming_key, max_tracked_keys}`. Fires when the cap
  displaces a key, signalling a broader binding or resolver than
  intended.
- **`PhiAccrualAmqp.Consumer.child_spec/1`** — reads the standard
  supervisor keys `:id`, `:restart` and `:shutdown` from the consumer
  option list without forwarding them to `start_link/1`. `:id`
  defaults to `:name` when one is given, and otherwise to
  `{PhiAccrualAmqp.Consumer, queue}`, so one consumer per queue can be
  supervised together without spelling out an `:id`.

- **`:connect` and the `:connection_opts`-over-`:url` precedence rule
  are now documented.** Both were observable behaviour with no
  description in any document; neither is new in this release.

### Fixed

- **The consumer now traps exits, so `terminate/2` runs on supervisor
  shutdown.** Without it the process was killed outright and the
  connection — started under the `amqp_client` supervision tree rather
  than linked to the consumer — outlived the consumer that opened it.

### Changed

- **Every telemetry event now carries a non-empty measurement map.**
  `[:sample, :received]` gains `%{monotonic_time, system_time}`, where
  `monotonic_time` is the exact value handed to `PhiAccrual.observe/2`
  — so a handler can derive inter-arrival intervals directly instead of
  reconstructing them, and the clock-discipline promise becomes
  inspectable rather than merely documented. `[:connection, :down]`
  gains `%{tracked}`, counting what its `:keys` metadata lists, because
  `Telemetry.Metrics` cannot aggregate a list. The remaining events
  carry `%{system_time}`. Handlers that pattern-matched on `%{}`
  continue to match; handlers that asserted an empty map do not.

  This is `phi_accrual_amqp`'s own committed telemetry schema. It is
  not a step toward unifying payloads with `phi_accrual_udp`: the
  transports are deliberate specializations of a transport-agnostic
  core, the only contract between them is `PhiAccrual.observe/2`, and
  per-transport handlers are the intended model rather than a gap left
  to close.
- **Connections are opened with `heartbeat: 10` and
  `connection_timeout: 5_000`.** The heartbeat matches the AMQP
  client's own default and is now pinned explicitly. The timeout
  replaces the client's 60s (URI) and 50s (keyword) defaults, bounding
  how long a connection attempt — and any `status/2` call queued behind
  it — can block against a broker that accepts packets without
  completing the handshake. A keyword list passed as `:connection_opts`
  is merged over them and wins; a binary `:connection_opts` is a URL,
  so there is nothing to merge and the defaults stand — and they also
  take precedence over the same values embedded in the URL's query
  string, since the client resolves explicit options ahead of parsed
  URI parameters.
- **A server-initiated `basic.cancel` now emits
  `[:phi_accrual_amqp, :connection, :down]`** with
  `reason: :server_cancelled`, and marks the consumer disconnected.
  Previously the cancel path went straight to teardown, so a cancelled
  consumer reported no outage at all while it sat in backoff with no
  subscription. `[:consumer, :cancelled]` is unchanged and still
  carries the consumer tag.
- **Reconnect backoff is now jittered.** The ceiling still doubles per
  attempt between `:reconnect_min_ms` and `:reconnect_max_ms`, but the
  delay actually waited is drawn uniformly between the floor and that
  ceiling. Undithered doubling had every consumer attached to a
  restarting broker retry in lockstep, stampeding a broker that was
  still recovering. No configuration change is required, and the
  documented bounds are unchanged.
- **`start_link/1` no longer registers the process under
  `PhiAccrualAmqp.Consumer` by default.** A consumer runs unnamed
  unless `:name` is given. The previous default made a second consumer
  fail to start with `{:error, {:already_started, pid}}`, which blocked
  the one-queue-per-node topology described in the README. Callers that
  relied on the implicit name should pass `name: PhiAccrualAmqp.Consumer`
  explicitly.

## [0.1.0] - 2026-05-18

Initial public release.

### Added

- **`PhiAccrualAmqp.Consumer`** — AMQP 0-9-1 consumer with
  connection/channel lifecycle, server-cancel handling, and
  exponential-backoff reconnect between `:reconnect_min_ms` (default
  1s) and `:reconnect_max_ms` (default 30s). Feeds broker deliveries
  to `PhiAccrual.observe/2` using local monotonic receipt time.
- **`PhiAccrualAmqp.Envelope`** — pure projection from AMQP delivery
  metadata to a `t:PhiAccrual.detector_key/0`. Configurable
  `:key_resolver`, defaulting to `routing_key`.
- **Consumer-only by design** — no synthetic heartbeat publisher is
  shipped, to avoid conflating broker liveness with node liveness.
- **Telemetry events** under `[:phi_accrual_amqp, ...]`:
  `[:connection, :up | :down]`,
  `[:consumer, :registered | :cancelled]`, `[:sample, :received]`,
  and `[:extract, :error]`.
- **Broker-backed integration suite**, gated behind the
  `:integration` ExUnit tag.
- **Requires `phi_accrual ~> 1.1`** (for `t:PhiAccrual.detector_key/0`).

### Notes

- **AMQP 0-9-1 only** (RabbitMQ-class brokers). Not compatible with
  AMQP 1.0 brokers such as ActiveMQ Artemis, Apache Qpid, Azure
  Service Bus, or Solace — AMQP 1.0 is a different, incompatible
  protocol.
- **Alpha.** Public API and telemetry schema may change before `v1.0`
  based on real-deployment feedback.
- **Known.** The `[:sample, :received]` telemetry payload is not
  drop-in compatible with `phi_accrual_udp` — see the README
  cross-transport note for the shape differences. The channel-death
  `:unexpected_delivery_and_no_default_consumer` log surfaced by
  `amqp_client`'s `SelectiveConsumer` is an expected, tolerated
  reconnect race; the Consumer recovers via the normal reconnect
  path.

[Unreleased]: https://github.com/thatsme/phi_accrual_amqp/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/thatsme/phi_accrual_amqp/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/thatsme/phi_accrual_amqp/releases/tag/v0.1.0
