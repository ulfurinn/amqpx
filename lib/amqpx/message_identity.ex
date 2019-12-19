defmodule AMQPX.MessageIdentity do
  @doc """

  """
  @type identity_function() ::
          {:property, atom()}
          | :message_id
          | {:payload_hash, hash_algo()}
          | :payload_hash
          | :payload
          | :callback

  @doc "Check `:crypto` for supported algorithms."
  @type hash_algo() :: atom()

  def get(payload, meta, handler, functions) do
    functions |> Enum.reduce(nil, &message_identity({payload, meta, handler}, &1, &2))
  end

  defp message_identity(_, _, id) when id != nil, do: id

  defp message_identity({_, meta, _}, {:property, property}, _) do
    case meta |> Map.get(property) do
      :undefined -> nil
      x -> x
    end
  end

  defp message_identity({payload, _, _}, {:payload_hash, algo}, _) do
    :crypto.hash(algo, payload)
  end

  defp message_identity({payload, _, _}, :payload, _) do
    payload
  end

  defp message_identity({payload, meta, handler}, :callback, _) do
    if :erlang.function_exported(handler, :identity, 2) do
      handler.identity(payload, meta)
    end
  end

  defp message_identity(context, :payload_hash, acc) do
    message_identity(context, {:payload_hash, :sha1}, acc)
  end

  defp message_identity(context, :message_id, acc) do
    message_identity(context, {:property, :message_id}, acc)
  end
end
