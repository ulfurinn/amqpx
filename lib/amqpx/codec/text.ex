defmodule AMQPX.Codec.Text do
  def encode(x) when is_binary(x), do: {:ok, x}
  def encode(x), do: {:ok, inspect(x)}

  def decode(x), do: {:ok, x}
end
