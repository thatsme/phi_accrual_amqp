# Changelog

All notable changes to this project are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

### Changed

- **Connections are opened with `heartbeat: 10` and
  `connection_timeout: 5_000`.** The heartbeat matches the AMQP
  client's own default and is now pinned explicitly. The timeout
  replaces the client's 60s (URI) and 50s (keyword) defaults, bounding
  how long a connection attempt — and any `status/2` call queued behind
  it — can block against a broker that accepts packets without
  completing the handshake. Values passed in `:connection_opts` still
  win.
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

[Unreleased]: https://github.com/thatsme/phi_accrual_amqp/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/thatsme/phi_accrual_amqp/releases/tag/v0.1.0
