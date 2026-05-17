# Changelog

All notable changes to this project are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - TBD

Initial public release. **Alpha** — public API and telemetry schema
may change before `v1.0` based on real-deployment feedback.

### Added

- **Envelope projection** (`PhiAccrualAmqp.Envelope`) — pure mapping
  from an AMQP delivery's meta map to
  `(detector_key, diagnostic_timestamp)`. No wire codec: AMQP has no
  wire format to parse at this layer; the broker hands the `:amqp`
  library a structured envelope. Configurable `:key_resolver` with
  default of `routing_key`.
- **AMQP consumer** (`PhiAccrualAmqp.Consumer`) — GenServer that
  owns a connection, channel, and subscription on one configured
  queue. Calls `PhiAccrual.observe/2` per delivery with local
  monotonic receipt time. Decode/extract failures emit
  `[:phi_accrual_amqp, :extract, :error]` telemetry with reason
  classification.
- **Connection lifecycle** — async connect on startup so the
  supervisor can come up before the broker is reachable; monitors
  connection and channel pids; reconnects with exponential backoff
  between `:reconnect_min_ms` (default 1s) and `:reconnect_max_ms`
  (default 30s) on broker unreachable, channel error,
  server-initiated `basic.cancel`, or process death. Has no UDP
  equivalent — distinct from `phi_accrual_udp.Listener`'s fail-fast
  `:gen_udp.open`.
- **Telemetry schema** — `[:connection, :up | :down]`,
  `[:consumer, :registered | :cancelled]`, `[:sample, :received]`,
  `[:extract, :error]`.

### Notes

- **Consumer-only by design.** No `Publisher` module is shipped: in
  AMQP-using systems, any existing application traffic already
  proves node liveness, and a dedicated synthetic publisher
  conflates broker liveness with node liveness. See README for
  rationale.
- **Receiver-driven clock discipline.** The EWMA uses local
  monotonic receipt time, never the envelope `timestamp` property
  nor any broker-stamped header. This preserves `phi_accrual`'s
  contract that cross-process timestamps are meaningless.
- **Liveness semantics.** In AMQP, "delivery received" proves
  publisher AND broker AND network are alive in combination. A
  rising phi value cannot be pinned on the publisher alone. This
  is the cost of going broker-mediated.
- Telemetry schema is **not yet committed**. May change before
  `v1.0`.

### Known limitations

- **Channel-death race produces a tolerated stray log.** When the
  AMQP channel pid dies, the broker's already-enqueued
  `basic.cancel` is delivered into amqp_client's `SelectiveConsumer`
  after the consumer registration is gone, surfacing as a noisy
  `:unexpected_delivery_and_no_default_consumer` log; the Consumer
  recovers via the normal reconnect path, but cleaning up the
  origin race is a v0.2 candidate.
- **Integration-test skip does not appear as `skipped` in the
  ExUnit run summary.** Elixir 1.19 has no runtime skip API
  (filtering happens before setup), so the broker-unavailable
  fallback only logs `[SKIP]` and implicit-passes; CI mitigates
  this by hard-failing the integration job if the broker isn't
  reachable, keeping the skip path unreachable in CI.
