defmodule PhiAccrualAmqp do
  @moduledoc """
  Dedicated AMQP consumer source for [`phi_accrual`](https://hex.pm/packages/phi_accrual).

  Treats broker deliveries as liveness signals: each delivery on a
  subscribed queue feeds `PhiAccrual.observe/2` with local monotonic
  receipt time, keyed by a detector identity extracted from the
  envelope (default: `routing_key`).

  ## Components

    * `PhiAccrualAmqp.Envelope` — pure projection from an AMQP
      delivery's meta map to `(detector_key, diagnostic_timestamp)`.
      Does not parse the payload.
    * `PhiAccrualAmqp.Consumer` — GenServer that owns the connection,
      channel, and subscription. Calls `PhiAccrual.observe/2` per
      delivery. Reconnects with backoff on broker / network failures.

  This package is **consumer-only**. AMQP applications typically
  already publish messages that prove node liveness, so a dedicated
  heartbeat publisher would conflate broker liveness with node
  liveness (broker healthy → messages flow → phi stays low, even if
  the producer is sending nothing of substance). If you want a
  symmetric sender, use existing application traffic or
  `phi_accrual_udp` for a transport with no intermediary.

  ## Quick start

      # In your supervision tree
      children = [
        {PhiAccrualAmqp.Consumer,
          url: "amqp://guest:guest@rabbit/",
          queue: "phi.heartbeats"}
      ]

  Topology — exchange, queue declaration, bindings — is your
  application's responsibility. This package consumes from an
  existing queue.

  ## Clock discipline

  This package preserves `phi_accrual`'s clock discipline: the
  detector reasons only about local monotonic receipt times. The
  envelope `timestamp` property (publisher wall clock) and any
  broker-stamped header are diagnostic-only — they are NOT used for
  the EWMA. See `PhiAccrualAmqp.Consumer` and `PhiAccrualAmqp.Envelope`
  moduledocs.

  ## Liveness semantics

  In AMQP, "delivery received" proves publisher AND broker AND the
  network paths between them are alive in combination. A rising phi
  value cannot be pinned on the publisher alone. For
  publisher-only liveness, choose a transport with no intermediary.
  """
end
