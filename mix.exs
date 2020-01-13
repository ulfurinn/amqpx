defmodule AMQPX.MixProject do
  use Mix.Project

  def project do
    [
      app: :ulfnet_amqpx,
      version: "1.0.0",
      elixir: "~> 1.6",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "AMQPX",
      description: "Higher-level functionality on top of AMQP",
      package: package(),
      source_url: "https://github.com/ulfurinn/amqpx",
      docs: [
        extras: ["README.md"],
        main: "readme"
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {AMQPX.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:amqp, "~> 1.1.0"},
      {:uuid, "~> 1.1"},
      {:jason, "~> 1.1", optional: true},
      {:poison, "~> 3.1", optional: true},
      {:ex_doc, ex_doc_version(), only: :dev, runtime: false, optional: true}
    ]
  end

  defp ex_doc_version() do
    "~> 0.21"
  end

  defp package() do
    [
      name: "ulfnet_amqpx",
      licenses: ["MIT"],
      links: %{
        "github" => "https://github.com/ulfurinn/amqpx"
      }
    ]
  end
end
