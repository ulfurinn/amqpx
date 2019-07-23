defmodule AMQPX.Receiver.Standard do
  @doc """
  Called on every incoming message.

  The payload type will depend on the content type of the incoming message and the codec registered for that content type.
  If there is no matching codec for that content type, the payload will be passed as is.
  """
  @callback handle(payload :: any(), meta :: Map.t()) :: any()

  @doc """
  Takes the result or `c:handle/2` or its crash reason and formats it in a way that a codec can handle.

  Only used if the incoming message indicates that a reply is necessary.

  `payload` will be passed through the codec indicated by `mime_type`.
  A payload with no matching codec for the declared MIME type will be sent as is.
  A bare payload string will be sent as is with the content type `application/octet-stream`.
  """
  @callback format_response(response :: any(), meta :: Map.t()) ::
              {mime_type :: :json | :text | String.t(), payload :: any()} | payload :: any()

  @doc """
  Tells the receiver whether to requeue messages when `c:handle/2` crashes.

  Be careful with choosing to always requeue. If the crash is not caused by some transient condition such as a lost database connection,
  but a permanent one such as a bug in the message handler, this will cause the message to be redelivered indefinitely at the highest rate supported by your environment,
  putting high load on the broker, the network, and the host running your application.

  Defaults to `false` if not implemented.
  """
  @callback requeue?() :: true | false | :once

  @callback handling_node(payload :: any(), meta :: Map.t()) :: atom()

  @optional_callbacks requeue?: 0, handling_node: 2

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
    :name,
    :conn,
    :ch,
    :shared_ch,
    :ctag,
    :default_handler,
    :handlers,
    :task_sup,
    :codecs,
    :mime_type,
    {:log_traffic, false},
    {:measurer, nil}
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
          | {:name, atom()}
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
          | {:keys, list(String.t()) | %{(routing_key :: String.t()) => handler :: module()}}
          | {:handler, atom()}
          | {:codecs, %{(mime_type :: String.t()) => :handler | codec :: module()}}
          | {:supervisor, atom()}
          | {:name, atom()}
          | {:log_traffic, boolean()}
          | {:measurer, module()}

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

  * `:prefetch` – set the prefetch count; defaults to 1
  * `:exchange` – the exchange to bind to. The exchange is expected to exist; set `:declare` to `true` to create it. Defaults to a durable topic exchange.
  * `:queue` – the queue to consume from. Defaults to an anonymous auto-deleting queue.
  * `:keys` – a set of routing keys to bind with and their corresponding handler modules, or just a list of keys. The handler modules must implement the `AMQPX.Receiver.Standard` behaviour.
    If a list is given, the `:handler` option must be set.
  * `:handler` – the handler to use when no key-specific handler is set
  * `:codecs` – override the default set of codecs; see the Codecs section for details
  * `:supervisor` – the named `Task.Supervisor` to use for individual message handlers
  * `:name` – the name to register the process with
  """
  @spec start_link([option]) :: {:ok, pid()}
  def start_link(args) do
    case Keyword.get(args, :name) do
      name when is_atom(name) and name != nil ->
        GenServer.start_link(__MODULE__, args, name: name)

      _ ->
        GenServer.start_link(__MODULE__, args)
    end
  end

  def handle_handover(name, request),
    do: GenServer.call(name, {:handle_handover, request}, :infinity)

  @impl GenServer
  def init(args) do
    Process.flag(:trap_exit, true)

    name =
      case :erlang.process_info(self(), :registered_name) do
        {:registered_name, name} -> name
        _ -> nil
      end

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
        :ok = AMQP.Exchange.declare(ch, ex_name, ex_type, ex_opts)

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

    Keyword.fetch!(args, :keys)
    |> Enum.map(fn {key, _handler} -> key ; key -> key end)
    |> Enum.each(bind)

    {:ok, ctag} = AMQP.Basic.consume(ch, queue)

    state = %__MODULE__{
      name: name,
      conn: conn,
      ch: ch,
      shared_ch: shared_ch,
      ctag: ctag,
      handlers: Keyword.fetch!(args, :keys),
      default_handler: Keyword.get(args, :handler),
      codecs: Keyword.get(args, :codecs, %{}) |> AMQPX.Codec.codecs(),
      mime_type: Keyword.get(args, :mime_type),
      task_sup: Keyword.get(args, :supervisor, AMQPX.Application.task_supervisor()),
      log_traffic: Keyword.get(args, :log_traffic, false),
      measurer: Keyword.get(args, :measurer, nil)
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_call(
        {:handle_handover, {handler, payload, meta}},
        from,
        state = %__MODULE__{shared_ch: shared_ch, task_sup: sup}
      ) do
    receiver = self()
    share_ref = make_ref()

    child = fn ->
      Process.flag(:trap_exit, true)
      AMQPX.SharedChannel.share(shared_ch)
      send(receiver, {:share_acquired, share_ref})

      payload
      |> handler.handle(meta)
      |> rpc_reply(handler, meta, state)

      GenServer.reply(from, :ok)
    end

    Task.Supervisor.start_child(sup, child)

    receive do
      {:share_acquired, ^share_ref} ->
        :ok
        # TODO: what to do if it crashes before it can send?
    end

    {:noreply, state}
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

  defp handle_message(
         payload,
         meta = %{routing_key: rk},
         state = %__MODULE__{handlers: handlers, default_handler: default_handler, log_traffic: log}
       ) do
    if log,
      do: Logger.info(["RECV ", payload, " | ", inspect(meta)])

    case handlers do
      %{^rk => handler} ->
        handle_message(handler, payload, meta, state)

      _ ->
        if default_handler do
          handle_message(default_handler, payload, meta, state)
        else
          if log,
            do: Logger.info(["IGNR | ", inspect(meta)])
          nil
        end
    end
  end

  defp handle_message(
         handler,
         payload,
         meta,
         state = %__MODULE__{
           ch: ch,
           shared_ch: shared_ch,
           task_sup: sup,
           measurer: measurer
         }
       ) do
    receiver = self()
    share_ref = make_ref()

    child = fn ->
      # make the task supervisor wait for us
      Process.flag(:trap_exit, true)
      AMQPX.SharedChannel.share(shared_ch)
      send(receiver, {:share_acquired, share_ref})

      handler_fn = handler_fn(handler, payload, meta, state)

      child_fn =
        if measurer do
          fn -> measurer.measure_packet_handler(handler_fn, meta) end
        else
          handler_fn
        end

      {_, ref} = spawn_monitor(child_fn)

      receive do
        {:DOWN, ^ref, _, _, :normal} ->
          ack(ch, meta)

        {:DOWN, ^ref, _, _, reason} ->
          requeue =
            case reason do
              {:badrpc, _} -> true
              _ -> requeue?(handler, meta)
            end

          reject(ch, meta, requeue: requeue)

          if not requeue,
            do: rpc_reply(reason, handler, meta, state)
      end
    end

    Task.Supervisor.start_child(sup, child)

    receive do
      {:share_acquired, ^share_ref} ->
        :ok
        # TODO: what to do if it crashes before it can send?
    end
  end

  defp handler_fn(
         handler,
         payload,
         meta,
         state = %__MODULE__{
           name: name,
           codecs: codecs
         }
       ) do
    fn ->
      case payload |> AMQPX.Codec.decode(meta, codecs, handler) do
        {:ok, payload} ->
          # We only support handover if the process is named, and we assume it to have the same name on all nodes.
          # Otherwise we don't know who to talk to on the remote node.
          node =
            if name != nil && :erlang.function_exported(handler, :handling_node, 2) do
              handler.handling_node(payload, meta)
            else
              Node.self()
            end

          if node == Node.self() do
            payload
            |> handler.handle(meta)
            |> rpc_reply(handler, meta, state)
          else
            case :rpc.call(node, __MODULE__, :handle_handover, [
                   name,
                   {handler, payload, meta}
                 ]) do
              error = {:badrpc, _} -> exit(error)
              _ -> :ok
            end
          end

        error ->
          rpc_reply(error, handler, meta, state)
      end
    end
  end

  defp rpc_reply(data, handler, meta, state)

  defp rpc_reply(
         data,
         handler,
         meta = %{reply_to: reply_to, correlation_id: correlation_id},
         %__MODULE__{ch: ch, codecs: codecs, mime_type: default_mime_type, log_traffic: log}
       )
       when is_binary(reply_to) or is_pid(reply_to) do
    {mime, payload} =
      case handler.format_response(data, meta) do
        {{:mime_type, mime}, payload} -> {mime, payload}
        payload -> {default_mime_type || "application/octet-stream", payload}
      end

    {:ok, payload} = AMQPX.Codec.encode(payload, mime, codecs, handler)

    if log,
      do: Logger.info(["SEND ", payload, " | ", inspect(meta)])

    send_response(ch, reply_to, payload, AMQPX.Codec.expand_mime_shortcut(mime), correlation_id)
  end

  defp rpc_reply(_, _, _, _), do: nil

  defp send_response(ch, queue, payload, content_type, correlation_id) when is_binary(queue),
    do:
      AMQP.Basic.publish(ch, "", queue, payload,
        content_type: content_type,
        correlation_id: correlation_id
      )

  defp send_response(_, pid, payload, _, _) when is_pid(pid), do: send(pid, payload)

  defp ack(ch, %{delivery_tag: dtag}), do: AMQP.Basic.ack(ch, dtag)
  defp ack(_, _), do: nil

  defp reject(ch, %{delivery_tag: dtag}, opts), do: AMQP.Basic.reject(ch, dtag, opts)
  defp reject(_, _, _), do: nil

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
