defmodule AMQPX.MixProject do
  use Mix.Project

  def project do
    [
      app: :ulfnet_amqpx,
      version: "0.1.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "AMQPX",
      docs: [
        extras: ["README.md"]
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
      {:jason, "~> 1.1", only: :test, runtime: false},
      {:ex_doc, ex_doc_version(), only: :dev, runtime: false}
    ]
  end

  defp ex_doc_version() do
    "~> 0.19.1"
  end
end
