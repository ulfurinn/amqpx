defmodule AMQPX.Receiver.Codec.Text do
  def encode!(x) when is_binary(x), do: x
  def encode!(x), do: inspect(x)

  def decode!(x), do: x
end
