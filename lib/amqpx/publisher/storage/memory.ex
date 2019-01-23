defmodule AMQPX.Publisher.Storage.Memory do
  @behaviour AMQPX.Publisher.Storage
  require Logger

  def init(_), do: []

  def store(id, record, state) do
    if record.ref == nil do
      ref = make_ref()
      record = %AMQPX.Publisher.Record{record | publish_seqno: id, ref: ref}
      # Logger.debug("storing new record #{inspect record}")
      [record | state]
    else
      record = %AMQPX.Publisher.Record{record | publish_seqno: id}
      # Logger.debug("updating record #{inspect(record)}")
      update_record(record, state)
    end
  end

  def confirm(id, multiple, list)

  def confirm(id, true, list),
    do: list |> Enum.reject(fn %{publish_seqno: n} -> n <= id end)

  def confirm(id, false, list),
    do: list |> Enum.reject(fn %{publish_seqno: n} -> n == id end)

  def pending(list), do: Enum.reverse(list)

  defp update_record(_, []), do: []
  defp update_record(record = %{ref: ref}, [%{ref: ref} | t]), do: [record | t]
  defp update_record(record, [h | t]), do: [h | update_record(record, t)]
end
