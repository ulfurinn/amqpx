defmodule AMQPX.Receiver.Standard.Deduplicate do
  @callback already_seen?(id :: String.t()) :: boolean()
  @callback remember_message(id :: String.t()) :: any()
  @optional_callbacks remember_message: 1

  @type option() ::
          {:identity, [AMQPX.MessageIdentity.identity_function()]} | {:callback, module()}

  defstruct [
    :identity,
    :callback
  ]

  def init(nil), do: nil

  def init(args) do
    %__MODULE__{
      identity: args |> Keyword.get(:identity, [:message_id, :payload_hash]),
      callback: args |> Keyword.get(:callback)
    }
  end

  def already_seen?(payload, meta, handler, state)
  def already_seen?(_, _, _, nil), do: false

  def already_seen?(payload, meta, handler, %__MODULE__{callback: callback, identity: identity}) do
    case AMQPX.MessageIdentity.get(payload, meta, handler, identity) do
      nil ->
        false

      id ->
        callback.already_seen?(id)
    end
  end

  def remember(payload, meta, handler, state)
  def remember(_, _, _, nil), do: nil

  def remember(payload, meta, handler, %__MODULE__{callback: callback, identity: identity}) do
    if :erlang.function_exported(callback, :remember_message, 1) do
      case AMQPX.MessageIdentity.get(payload, meta, handler, identity) do
        nil ->
          nil

        id ->
          callback.remember_message(id)
      end
    end
  end
end
