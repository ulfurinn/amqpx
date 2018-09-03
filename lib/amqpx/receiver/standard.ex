defmodule AMQPX.Receiver.Standard do
  @doc """
  Called on every incoming message.

  The payload type will depend on the content type of the incoming message and the codec registered for that content type.
  If there is no matching codec for that content type, the payload will be passed as is.
  """
  @callback handle(payload :: any(), meta :: Map.t()) :: any()

  @doc """
  Takes the result or `c:handle/2` or its crash reason and formats it in a way suitable for sending as a reply message.

  Only used if the incoming message indicates that a reply is necessary.

  `payload` will be passed through the codec indicated by `mime_type`.
  A payload with no matching codec for the declared MIME type will be sent as is.
  A bare payload string will be sent as is with the content type `application/octet-stream`.
  """
  @callback format_response(response :: any(), meta :: Map.t()) ::
              {mime_type :: :json | :text | String.t(), payload :: String.t()} | payload ::
              String.t()

  @doc """
  Tells the receiver whether to requeue messages when `c:handle/2` crashes.

  Be careful with choosing to always requeue. If the crash is not caused by some transient condition such as a lost database connection,
  but a permanent one such as a bug in the message handler, this will cause the message to be redelivered indefinitely at the highest rate supported by your environment,
  putting high load on the broker, the network, and the host running your application.

  Defaults to `false` if not implemented.
  """
  @callback requeue?() :: true | false | :once

  @optional_callbacks requeue?: 0

  @moduledoc """
  A message handler implementing some sane defaults.

  This server should not be started directly; use the `AMQPX.Receiver` supervisor instead.

  Each receiver sets up its own channel and makes sure it is disposed of when the receiver dies.

  If you're implementing your own receiver, remember to clean up channels to avoid leaking resources and potentially leaving messages stuck in unacked state.

  Each receiver sets up a single queue and binds it with multiple routing keys, assigning to each key a handler module implementing the `AMQPX.Receiver.Standard` behaviour; read the callback documentation for details.

  If the arriving message sets the `reply_to` and `correlation_id` attributes, the result of the message handler (or its crash reason) will be sent as a reply message. This is designed to work transparently in conjunction with `AMQPX.RPC`.

  # Message handler lifetime

  Each message spawns a `Task` placed under a `Task.Supervisor` with graceful shutdown to help ensure that under normal shutdown all message handlers are allowed to finish their work and send the acks to the broker.

  `AMQPX` provides a default supervisor process; however, to help ensure that message handlers have access to the resources they need, such as database connections,
  it is recommended that you start your own `Task.Supervisor`, set ample shutdown time, and place it in your supervision tree after the required resource but before the `AMQPX.Receiver` that will be spawning the handlers.

  # Codecs

  `AMQPX` tries to separate message encoding and the business logic of message handlers with codecs.

  A codec is a module implementing the `AMQPX.Codec` behaviour. The only codec provided out of the box is `AMQPX.Codec.Text`.

  `:text` is shorthand for "text/plain" and is handled by `AMQPX.Codec.Text` by default.

  `:json` is recognised as shorthand for `application/json`, but no codec is included in `AMQPX`; however, both `Poison` and `Jason` can be used as codec modules directly if you bundle them in your application.
  """

  use GenServer
  require Logger

  defstruct [
    :conn,
    :ch,
    :shared_ch,
    :ctag,
    :handlers,
    :task_sup,
    :codecs,
    :mime_type
  ]

  @type exchange_option ::
          {:declare, boolean()}
          | {:durable, boolean()}
          | {:passive, boolean()}
          | {:auto_delete, boolean()}
          | {:internal, boolean()}
          | {:no_wait, boolean()}
          | {:arguments, list()}

  @type option ::
          {:connection, connection_id :: atom()}
          | {:prefetch, integer()}
          | {:exchange,
             {type :: atom(), name :: String.t(), opts :: [exchange_option]}
             | {type :: atom(), name :: String.t()}
             | name :: String.t()}
          | {:queue,
             nil
             | name ::
               String.t()
               | opts ::
               Keyword.t()
               | {name :: String.t(), opts :: Keyword.t()}}
          | {:keys, %{(routing_key :: String.t()) => handler :: module()}}
          | {:codecs, %{(mime_type :: String.t()) => :handler | codec :: module()}}
          | {:supervisor, atom()}

  @doc false
  def child_spec(args) do
    %{
      id: Keyword.get(args, :id, __MODULE__),
      start: {__MODULE__, :start_link, [args]},
      shutdown: Keyword.get(args, :shutdown, :infinity)
    }
  end

  @doc """
  Starts the process.

  ## Options

  * `:connection` – do not pass directly, it will be overwritte nby `AMQPX.Receiver`
  * `:prefetch` – set the prefetch count; defaults to 1
  * `:exchange` – the exchange to bind to. The exchange is expected to exist; set `:declare` to `true` to create it. Defaults to a durable topic exchange.
  * `:queue` – the queue to consume from. Defaults to an anonymous auto-deleting queue.
  * `:keys` – a set of routing keys to bind with and their corresponding handler modules. The handler modules must implement the `AMQPX.Receiver.Standard` behaviour.
  * `:codecs` – override the default set of codecs; see the Codecs section for details
  * `:supervisor` – the named `Task.Supervisor` to use for individual message handlers
  """
  @spec start_link([option]) :: {:ok, pid()}
  def start_link(args),
    do: GenServer.start_link(__MODULE__, args)

  @impl GenServer
  def init(args) do
    Process.flag(:trap_exit, true)

    {:ok, conn} = AMQPX.ConnectionPool.get(Keyword.fetch!(args, :connection))
    {:ok, ch} = AMQP.Channel.open(conn)
    {:ok, shared_ch} = AMQPX.SharedChannel.start(ch)
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
      shared_ch: shared_ch,
      ctag: ctag,
      handlers: Keyword.fetch!(args, :keys),
      codecs: Keyword.get(args, :codecs, %{}) |> AMQPX.Codec.codecs(),
      mime_type: Keyword.get(args, :mime_type),
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

  defp handle_message(
         handler,
         payload,
         meta,
         state = %__MODULE__{ch: ch, shared_ch: shared_ch, task_sup: sup, codecs: codecs}
       ) do
    child = fn ->
      # make the task supervisor wait for us
      Process.flag(:trap_exit, true)
      AMQPX.SharedChannel.share(shared_ch)

      {_, ref} =
        spawn_monitor(fn ->
          {:ok, payload} = payload |> AMQPX.Codec.decode(meta, codecs, handler)

          payload
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
         state = %__MODULE__{ch: ch, codecs: codecs, mime_type: default_mime_type}
       )
       when is_binary(reply_to) do
    {mime, payload} =
      case handler.format_response(data, meta) do
        {mime, payload} -> {mime, payload}
        payload -> {default_mime_type || "application/octet-stream", payload}
      end

    {:ok, payload} = AMQPX.Codec.encode(payload, mime, codecs, handler)

    AMQP.Basic.publish(
      ch,
      "",
      reply_to,
      payload,
      content_type: AMQPX.Codec.expand_mime_shortcut(mime),
      correlation_id: correlation_id
    )
  end

  defp rpc_reply(_, _, _, _), do: nil

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
end
