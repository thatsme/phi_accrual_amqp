# Changelog

All notable changes to this project are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
