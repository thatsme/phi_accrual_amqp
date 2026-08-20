defmodule PhiAccrualAmqp.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/thatsme/phi_accrual_amqp"

  def project do
    [
      app: :phi_accrual_amqp,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: description(),
      package: package(),
      name: "phi_accrual_amqp",
      source_url: @source_url,
      docs: docs(),
      elixirc_options: [warnings_as_errors: true],
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      dialyzer: [
        plt_local_path: "priv/plts",
        plt_core_path: "priv/plts"
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp aliases do
    [
      "test.all": ["test --include integration"],
      "test.integration": ["test --only integration"]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:phi_accrual, "~> 1.1"},
      {:amqp, "~> 4.0"},
      {:telemetry, "~> 1.2"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false}
    ]
  end

  defp description do
    "Dedicated AMQP 0-9-1 consumer source for phi_accrual (RabbitMQ-class brokers; " <>
      "not AMQP 1.0). Treats broker deliveries as liveness signals; receiver-driven " <>
      "clock discipline. Envelope timestamp is diagnostic-only."
  end

  defp package do
    [
      maintainers: ["Alessio Battistutta"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md",
        "phi_accrual" => "https://hex.pm/packages/phi_accrual"
      },
      files: ~w(lib mix.exs README.md CHANGELOG.md LICENSE .formatter.exs)
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md", "LICENSE"],
      source_ref: "v#{@version}",
      source_url: @source_url,
      formatters: ["html"]
    ]
  end
end
