defmodule AMQPX.Receiver.Standard.Retry do
  defstruct [
    :table,
    :limit,
    :identity,
    :delay
  ]

  @doc """
  Retry options.

  `identity` specifies the list of methods to generate a unique term for a
  message. The first non-`nil` result is used. If all methods evaluate to
  `nil`, retry tracking is not used for that message.

  `delay` specifies the time to wait (in milliseconds) before rejecting the
  delivery, to prevent a hot retry loop.
  """
  @type option ::
          {:limit, integer()}
          | {:identity, [AMQPX.MessageIdentity.identity_function()]}
          | {:delay, nil | integer()}

  def init(nil), do: nil

  def init(opts) do
    table = :ets.new(__MODULE__, [:public])

    limit = opts |> Keyword.fetch!(:limit)
    identity = opts |> Keyword.get(:identity, [:message_id, :payload_hash])
    delay = opts |> Keyword.get(:delay)

    %__MODULE__{
      table: table,
      limit: limit,
      identity: identity,
      delay: delay
    }
  end

  def exhausted?(payload, meta, handler, state)
  def exhausted?(_, _, _, nil), do: false

  def exhausted?(
        payload,
        meta,
        handler,
        %__MODULE__{table: table, limit: limit, identity: identity}
      ) do
    case AMQPX.MessageIdentity.get(payload, meta, handler, identity) do
      nil ->
        false

      key ->
        seen_times =
          case :ets.lookup(table, key) do
            [] -> 0
            [{^key, n}] -> n
          end

        # A retry limit of 0 allows us to see a message once. A retry limit
        # of 1 lets us see a message twice, and so on.
        if seen_times > limit do
          true
        else
          :ets.insert(table, {key, seen_times + 1})
          false
        end
    end
  end

  def delay(state)
  def delay(%__MODULE__{delay: delay}) when is_integer(delay), do: Process.sleep(delay)
  def delay(_), do: nil

  def clear(payload, meta, handler, state)
  def clear(_, _, _, nil), do: nil

  def clear(payload, meta, handler, %__MODULE__{table: table, identity: identity}) do
    case AMQPX.MessageIdentity.get(payload, meta, handler, identity) do
      nil ->
        nil

      key ->
        :ets.delete(table, key)
    end
  end
end
