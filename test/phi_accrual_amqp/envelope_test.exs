defmodule PhiAccrualAmqp.EnvelopeTest do
  use ExUnit.Case, async: true

  alias PhiAccrualAmqp.Envelope

  describe "extract/2 with default resolver" do
    test "extracts routing_key as detector_key" do
      meta = %{routing_key: "heartbeat.node_a", exchange: "ha"}

      assert {:ok, %Envelope{detector_key: "heartbeat.node_a", timestamp: nil}} =
               Envelope.extract(meta)
    end

    test "extracts envelope timestamp when present" do
      meta = %{routing_key: "x", timestamp: 1_700_000_000}

      assert {:ok, %Envelope{detector_key: "x", timestamp: 1_700_000_000}} =
               Envelope.extract(meta)
    end

    test "timestamp is nil when missing" do
      meta = %{routing_key: "x"}
      assert {:ok, %Envelope{timestamp: nil}} = Envelope.extract(meta)
    end

    test "timestamp is nil when not an integer (e.g., :undefined)" do
      meta = %{routing_key: "x", timestamp: :undefined}
      assert {:ok, %Envelope{timestamp: nil}} = Envelope.extract(meta)
    end

    test "errors with :no_detector_key when routing_key is empty string" do
      meta = %{routing_key: ""}
      assert {:error, :no_detector_key} = Envelope.extract(meta)
    end

    test "errors with :no_detector_key when routing_key is missing" do
      assert {:error, :no_detector_key} = Envelope.extract(%{exchange: "x"})
    end

    test "errors with :no_detector_key when routing_key is :undefined" do
      meta = %{routing_key: :undefined}
      assert {:error, :no_detector_key} = Envelope.extract(meta)
    end
  end

  describe "extract/2 with custom resolver" do
    test "constant resolver returns a fixed detector_key" do
      meta = %{routing_key: "ignored"}
      resolver = fn _ -> :node_a end

      assert {:ok, %Envelope{detector_key: :node_a}} =
               Envelope.extract(meta, key_resolver: resolver)
    end

    test "header-based resolver" do
      meta = %{
        routing_key: "",
        headers: [{"node", :longstr, "node-b"}]
      }

      resolver = fn %{headers: [{"node", _, name} | _]} -> name end

      assert {:ok, %Envelope{detector_key: "node-b"}} =
               Envelope.extract(meta, key_resolver: resolver)
    end

    test "resolver returning nil yields :no_detector_key" do
      resolver = fn _ -> nil end
      assert {:error, :no_detector_key} = Envelope.extract(%{}, key_resolver: resolver)
    end

    test "resolver raising yields :resolver_raised" do
      resolver = fn _ -> raise "boom" end
      assert {:error, :resolver_raised} = Envelope.extract(%{}, key_resolver: resolver)
    end

    test "resolver pattern-match failure yields :resolver_raised" do
      resolver = fn %{required: _} -> :ok end
      assert {:error, :resolver_raised} = Envelope.extract(%{}, key_resolver: resolver)
    end
  end

  describe "default_key_resolver/1" do
    test "returns routing_key when binary and non-empty" do
      assert Envelope.default_key_resolver(%{routing_key: "abc"}) == "abc"
    end

    test "returns nil for empty routing_key" do
      assert Envelope.default_key_resolver(%{routing_key: ""}) == nil
    end

    test "returns nil for non-binary routing_key" do
      assert Envelope.default_key_resolver(%{routing_key: :undefined}) == nil
      assert Envelope.default_key_resolver(%{routing_key: nil}) == nil
    end

    test "returns nil when key is missing entirely" do
      assert Envelope.default_key_resolver(%{}) == nil
    end
  end
end
