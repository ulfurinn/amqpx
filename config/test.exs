use Mix.Config

config :ulfnet_amqpx,
  connections: %{
    test: "amqp://guest:guest@localhost"
  }

# config :lager, handlers: []
