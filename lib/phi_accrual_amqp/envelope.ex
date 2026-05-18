defmodule PhiAccrualAmqp.Envelope do
  @moduledoc """
  Pure projection from an AMQP delivery's metadata to the pair the
  detector actually needs: `(detector_key, diagnostic_timestamp)`.

  ## What this is NOT

  Unlike `PhiAccrualUdp.Packet`, this module is **not a wire codec**.
  In AMQP there is no wire format to parse — the broker hands the
  `:amqp` library a structured envelope, which the library passes to
  the consumer as a meta map. This module's job is to *project* that
  map down to the two pieces of information `PhiAccrualAmqp.Consumer`
  needs to feed `PhiAccrual.observe/2`.

  It does NOT look at the payload. The payload is opaque to
  `phi_accrual_amqp`. If your application encodes the heartbeat
  identity inside the message body, write a custom resolver in
  application code that decodes that body and pass it via
  `:key_resolver` — but the contract of this package is that the
  envelope alone is sufficient to identify the source.

  ## detector_key resolution

  The detector key is the identity passed to `PhiAccrual.observe/2`;
  see `t:PhiAccrual.detector_key/0` for the value type.
  It is extracted from the delivery meta via a `:key_resolver`
  function — a `(meta -> PhiAccrual.detector_key() | nil)` mapping.
  Returning `nil` yields `{:error, :no_detector_key}` and the
  delivery is dropped with a telemetry event.

  The default resolver returns the `:routing_key` when it is a
  non-empty binary. This fits the common topic-exchange topology
  (`heartbeat.<node>` bound with `#` to a single queue). For other
  topologies — header-based, queue-name-based, app_id-based — supply
  a custom resolver.

  ## diagnostic timestamp

  AMQP messages carry an optional `:timestamp` property
  (`BasicProperties.timestamp`). It is **not** used to feed the
  detector — `PhiAccrual` requires receiver-local monotonic time
  (see `PhiAccrualAmqp.Consumer` moduledoc on clock discipline).
  The timestamp is extracted here only for diagnostic telemetry
  (e.g., publisher-to-consumer latency when both sides are
  NTP-synced and the publisher uses a consistent unit).

  Note that AMQP 0-9-1 does not specify a unit for this field; the
  convention is POSIX seconds, but the RabbitMQ message-timestamp
  plugin and some publishers use milliseconds. This module passes
  the integer through untouched.
  """

  @type meta :: map()
  @type resolver :: (meta() -> PhiAccrual.detector_key() | nil)
  @type reason :: :no_detector_key | :resolver_raised

  @type t :: %__MODULE__{
          detector_key: PhiAccrual.detector_key(),
          timestamp: integer() | nil
        }

  defstruct [:detector_key, :timestamp]

  @doc """
  Extract `(detector_key, diagnostic_timestamp)` from delivery meta.

  Returns `{:ok, %Envelope{}}` on success, or `{:error, reason}` if
  the resolver returned `nil` or raised.

  ## Options

    * `:key_resolver` — `(meta -> PhiAccrual.detector_key() | nil)`.
      Defaults to `default_key_resolver/1`, which returns
      `meta.routing_key` when it is a non-empty binary. See
      `t:PhiAccrual.detector_key/0`.
  """
  @spec extract(meta(), keyword()) :: {:ok, t()} | {:error, reason()}
  def extract(meta, opts \\ []) when is_map(meta) do
    resolver = Keyword.get(opts, :key_resolver, &__MODULE__.default_key_resolver/1)

    try do
      case resolver.(meta) do
        nil ->
          {:error, :no_detector_key}

        key ->
          {:ok, %__MODULE__{detector_key: key, timestamp: extract_timestamp(meta)}}
      end
    rescue
      _ -> {:error, :resolver_raised}
    end
  end

  @doc """
  Default key resolver: returns the `routing_key` when it is a
  non-empty binary, otherwise `nil`.
  """
  @spec default_key_resolver(meta()) :: PhiAccrual.detector_key() | nil
  def default_key_resolver(%{routing_key: rk}) when is_binary(rk) and byte_size(rk) > 0, do: rk
  def default_key_resolver(_), do: nil

  defp extract_timestamp(%{timestamp: ts}) when is_integer(ts), do: ts
  defp extract_timestamp(_), do: nil
end
