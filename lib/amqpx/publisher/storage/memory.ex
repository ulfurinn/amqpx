defmodule AMQPX.Publisher.Storage.Memory do
  @behaviour AMQPX.Publisher.Storage

  def init(_), do: []

  def append(id, data, list), do: [{id, data} | list]

  def confirm(id, multiple, list)

  def confirm(id, true, list),
    do: list |> Enum.reject(fn {n, _} -> n <= id end)

  def confirm(id, false, list),
    do: list |> Enum.reject(fn {n, _} -> n == id end)

  def handoff(list), do: {Enum.reverse(list) |> Enum.map(&second/1), []}

  defp second({_, x}), do: x
end
