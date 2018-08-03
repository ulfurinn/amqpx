defmodule AMQPX.Receiver.Standard do
  @callback handle(payload :: String.t(), meta :: Map.t()) :: any()
  @callback format_response(response :: any(), meta :: Map.t()) ::
              {mime_type :: :json | :text | String.t(), payload :: String.t()} | payload ::
              String.t()
  @callback requeue?() :: true | false | :once

  @optional_callbacks requeue?: 0

  use GenServer
  require Logger

  defstruct [
    :conn,
    :ch,
    :ctag,
    :handlers,
    :task_sup,
    :codecs
  ]

  @doc false
  def child_spec(args) do
    %{
      id: Keyword.get(args, :id, __MODULE__),
      start: {__MODULE__, :start_link, [args]},
      shutdown: :infinity
    }
  end

  @doc false
  def start_link(args),
    do: GenServer.start_link(__MODULE__, args)

  @impl GenServer
  def init(args) do
    Process.flag(:trap_exit, true)

    {:ok, conn} = AMQPX.ConnectionPool.get(Keyword.fetch!(args, :connection))
    {:ok, ch} = AMQP.Channel.open(conn)
    AMQPX.link_channel(ch)
    :ok = AMQP.Basic.qos(ch, prefetch_count: Keyword.get(args, :prefetch, 1))

    {ex_type, ex_name, ex_opts} =
      case Keyword.fetch!(args, :exchange) do
        {type, name, opts} -> {type, name, opts}
        {type, name} -> {type, name, [durable: true]}
        name -> {:topic, name, [durable: true]}
      end

    case Keyword.pop(ex_opts, :declare) do
      {true, ex_opts} ->
        :ok = apply(AMQP.Exchange, ex_type, [ch, ex_name, ex_opts])

      _ ->
        nil
    end

    {q_name, q_opts} =
      case Keyword.get(args, :queue) do
        nil -> {"", [auto_delete: true]}
        name when is_binary(name) -> {name, []}
        opts when is_list(opts) -> {"", opts}
        {name, opts} -> {name, opts}
      end

    {:ok, %{queue: queue}} = AMQP.Queue.declare(ch, q_name, q_opts)

    bind = fn rk ->
      :ok = AMQP.Queue.bind(ch, queue, ex_name, routing_key: rk)
    end

    Keyword.fetch!(args, :keys) |> Map.keys() |> Enum.each(bind)

    {:ok, ctag} = AMQP.Basic.consume(ch, queue)

    state = %__MODULE__{
      conn: conn,
      ch: ch,
      ctag: ctag,
      handlers: Keyword.fetch!(args, :keys),
      codecs: Map.merge(default_codecs(), Keyword.get(args, :codecs, %{})),
      task_sup: Keyword.get(args, :supervisor, AMQPX.Application.task_supervisor())
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_info({:basic_consume_ok, %{consumer_tag: ctag}}, state = %__MODULE__{ctag: ctag}) do
    {:noreply, state}
  end

  def handle_info({:basic_cancel, %{consumer_tag: ctag}}, state = %__MODULE__{ctag: ctag}) do
    {:stop, :unexpected_cancel, state}
  end

  def handle_info({:basic_cancel_ok, %{consumer_tag: ctag}}, state = %__MODULE__{ctag: ctag}) do
    {:noreply, state}
  end

  def handle_info({:basic_deliver, payload, meta}, state) do
    handle_message(payload, meta, state)
    {:noreply, state}
  end

  def handle_info({:channel_died, ch, _}, state = %__MODULE__{ch: ch}) do
    {:stop, :channel_died, %__MODULE__{state | ch: nil}}
  end

  def handle_info(msg, state) do
    Logger.warn("unexpected message #{inspect(msg)}")
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_, %__MODULE__{ch: ch, ctag: ctag}) do
    if ch != nil && :erlang.is_process_alive(ch.pid) do
      try do
        AMQP.Basic.cancel(ch, ctag)

        receive do
          {:basic_cancel_ok, %{consumer_tag: ^ctag}} ->
            nil
        after
          1000 ->
            # react to timeout?
            nil
        end
      catch
        # the gen call can crash if the channel proc died in the meantime
        :exit, _ ->
          nil
      end
    end

    nil
  end

  defp handle_message(payload, meta = %{routing_key: rk}, state = %__MODULE__{handlers: handlers}) do
    case handlers do
      %{^rk => handler} ->
        handle_message(handler, payload, meta, state)

      _ ->
        nil
    end
  end

  defp handle_message(handler, payload, meta, state = %__MODULE__{ch: ch, task_sup: sup}) do
    child = fn ->
      # make the task supervisor wait for us
      Process.flag(:trap_exit, true)

      {_, ref} =
        spawn_monitor(fn ->
          codec =
            case codec(meta, state) do
              :handler -> handler
              {:handler, args} -> {handler, args}
              codec -> codec
            end

          payload
          |> decode!(codec)
          |> handler.handle(meta)
          |> rpc_reply(handler, meta, state)
        end)

      receive do
        {:DOWN, ^ref, _, _, :normal} ->
          ack(ch, meta)

        {:DOWN, ^ref, _, _, reason} ->
          requeue = requeue?(handler, meta)
          reject(ch, meta, requeue: requeue)

          if not requeue,
            do: rpc_reply(reason, handler, meta, state)
      end
    end

    Task.Supervisor.start_child(sup, child)
  end

  defp rpc_reply(data, handler, meta, state)

  defp rpc_reply(
         data,
         handler,
         meta = %{reply_to: reply_to, correlation_id: correlation_id},
         state = %__MODULE__{ch: ch}
       )
       when is_binary(reply_to) do
    {mime, payload} =
      case handler.format_response(data, meta) do
        m_p = {mime, _payload} when is_binary(mime) -> m_p
        {mime, payload} when is_atom(mime) -> {mime_to_string(mime), payload}
        payload when is_binary(payload) -> {"application/octet-stream", payload}
      end

    codec =
      case codec(meta, state) do
        :handler -> handler
        {:handler, args} -> {handler, args}
        codec -> codec
      end

    payload = encode!(payload, codec)

    AMQP.Basic.publish(
      ch,
      "",
      reply_to,
      payload,
      content_type: mime,
      correlation_id: correlation_id
    )
  end

  defp ack(ch, %{delivery_tag: dtag}), do: AMQP.Basic.ack(ch, dtag)

  defp reject(ch, %{delivery_tag: dtag}, opts), do: AMQP.Basic.reject(ch, dtag, opts)

  defp requeue?(mod, %{redelivered: redelivered}) do
    if :erlang.function_exported(mod, :requeue?, 0) do
      case mod.requeue?() do
        true -> true
        false -> false
        :once -> not redelivered
      end
    else
      false
    end
  end

  defp mime_to_string(:json), do: "application/json"
  defp mime_to_string(:text), do: "text/plain"
  defp mime_to_string(_), do: "application/octet-stream"

  defp default_codecs do
    %{
      "text/plain" => AMQPX.Receiver.Codec.Text,
      "application/octet-stream" => AMQPX.Receiver.Codec.Text
    }
  end

  defp codec(%{content_type: content_type}, state), do: codec(content_type, state)

  defp codec(content_type, %__MODULE__{codecs: codecs}) when is_map(codecs),
    do: Map.fetch!(codecs, content_type)

  defp codec(_content_type, %__MODULE__{codecs: codec}) when is_atom(codec), do: codec

  defp encode!(payload, nil), do: payload

  defp encode!(payload, codec) when is_atom(codec),
    do: codec.encode!(payload)

  defp encode!(payload, {codec, args}) when is_atom(codec),
    do: codec.encode!(payload, args)

  defp decode!(payload, nil), do: payload

  defp decode!(payload, codec) when is_atom(codec),
    do: codec.decode!(payload)

  defp decode!(payload, {codec, args}) when is_atom(codec),
    do: codec.decode!(payload, args)
end
