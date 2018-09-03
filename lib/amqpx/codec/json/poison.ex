defmodule AMQPX.Codec.JSON.Poison do
  def decode(payload), do: Poison.Parser.parse(payload)
  def decode(payload, args), do: Poison.Parser.parse(payload, args)

  def encode(payload), do: Poison.encode(payload)
end
