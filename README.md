# AMQPX

[![Build Status](https://travis-ci.org/ulfurinn/amqpx.svg?branch=master)](https://travis-ci.org/ulfurinn/amqpx)

A set of higher-level AMQP constructs:

* transparent connection recovery
* graceful shutdown
* RPC over AMQP
* message encoding
* retry limiting
* deduplication

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `ulfnet_amqpx` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:ulfnet_amqpx, "~> 1.0.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/ulfnet_amqpx](https://hexdocs.pm/ulfnet_amqpx).

