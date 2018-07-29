defmodule AMQPX.MixProject do
  use Mix.Project

  def project do
    [
      app: :ulfnet_amqpx,
      version: "0.1.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:amqp, "~> 1.0"}
    ] ++ overrides()
  end

  defp overrides do
    :code.ensure_loaded(:ssl)
    # a hack to be compatible with OTP 21 until it is properly resolved upstream
    if Kernel.function_exported?(:ssl, :handshake, 3) do
      [{:ranch_proxy_protocol, "~> 2.0.0", override: true}]
    else
      []
    end
  end
end
