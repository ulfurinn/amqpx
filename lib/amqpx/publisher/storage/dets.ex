defmodule AMQPX.Publisher.Storage.DETS do
  @behaviour AMQPX.Publisher.Storage
  require Logger

  defstruct [
    :table
  ]

  def init(filename) do
    table = make_ref()
    {:ok, ^table} = :dets.open_file(table, file: String.to_charlist(filename))
    %__MODULE__{table: table}
  end

  def store(id, record, state) do
    ref = record.ref || :erlang.unique_integer([:monotonic])
    record = %AMQPX.Publisher.Record{record | publish_seqno: id, ref: ref}
    :ok = :dets.insert(state.table, {ref, record})
    state
  end

  def confirm(id, multiple, state)

  def confirm(id, true, state) do
    # Logger.debug("confirmed up to #{id}")
    :dets.select_delete(state.table, [
      {{:_, :"$1"}, [{:>=, id, {:map_get, :publish_seqno, :"$1"}}], [true]}
    ])

    state
  end

  def confirm(id, false, state) do
    # Logger.debug("confirmed #{id}")
    :dets.select_delete(state.table, [
      {{:_, :"$1"}, [{:==, id, {:map_get, :publish_seqno, :"$1"}}], [true]}
    ])

    state
  end

  def pending(state) do
    :dets.match_object(state.table, :_)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end
end
