defmodule AMQPX.RPC do
  @moduledoc """
  Provides RPC functionality over AMQP message passing.

  Include this in your supervision tree. See `start_link/1` for configuration options.

  The receiver works from an auto-deleting anonymous queue. If the channel is broken, response messages will be lost and callers will crash.
  This may or may not be acceptable to your use case. This may be improved in the future.
  """
  use Supervisor

  @doc false
  def child_spec(args) do
    %{
      id: Keyword.get(args, :id) || Keyword.get(args, :name) || __MODULE__,
      start: {__MODULE__, :start_link, [args]},
      type: :supervisor
    }
  end

  @doc """
  Starts an RPC client instance.

  Arguments:
  * `:connection`: a connection ID as registered with `AMQPX.ConnectionPool`.
  * `:reconnect`: the delay between reconnection attempts, in seconds. This is used so that a broker going down does not exceed the worker restart intensity. Defaults to `1`.
  * `:exchange`: the default exchange to publish to; see `call/4`/`call/6` for defails.
  * `:routing_key`: the default RK to use for publishing; see `call/4`/`call/6` for defails.
  * `:codecs`: codec overrides.
  * `:mime_type`: the content type for published messages; defaults to `application/octet-stream`. If the receiver is based on `AMQPX.Receiver.Standard`, see that the codec setup on both sides is compatible.
  """
  def start_link(args),
    do: Supervisor.start_link(__MODULE__, args)

  @impl Supervisor
  def init(args) do
    children = [
      {AMQPX.Watchdog, args},
      {AMQPX.RPC.Worker, args}
    ]

    interval = Keyword.get(args, :reconnect, 1)

    {max_restarts, max_seconds} =
      case {args[:max_restarts], args[:max_seconds]} do
        spec = {max_restarts, max_seconds} when max_restarts != nil and max_seconds != nil -> spec
        _ -> {2, interval}
      end

    Supervisor.init(
      children,
      strategy: :one_for_all,
      max_restarts: max_restarts,
      max_seconds: max_seconds
    )
  end

  @doc """
  Makes an RPC call using the worker's default configured exchange and routing key.

  Options are:
  * `:headers`
  * `:message_id`
  * `:correlation_id`
  * `:expire`: if `true`, sets the message `expiration` property equal to timeout. The
  effect will be that the receiver might not observe the message. Since after
  the timeout the caller is known to ignore the response even if it eventually
  arrives, it may be acceptable to drop the request entirely and thus reduce
  the receiver workload.

  Timeout is in milliseconds. `:infinity` is not allowed.
  """
  def call(server, payload, options \\ [], timeout),
    do: GenServer.call(server, {:call, nil, nil, payload, options, timeout}, timeout)

  @doc """
  Makes an RPC call using the explicitly provided exchange and routing key. All other arguments are the same as for `call/4`.
  """
  def call(server, exchange, routing_key, payload, options \\ [], timeout),
    do: GenServer.call(server, {:call, exchange, routing_key, payload, options, timeout}, timeout)

  defmodule Worker do
    @moduledoc false
    use GenServer

    defstruct [
      :ch,
      :queue,
      :exchange,
      :routing_key,
      :ctag,
      :codecs,
      :mime_type,
      :pending_calls
    ]

    defmodule Call do
      defstruct [
        :caller,
        :expire_at
      ]
    end

    def start_link(args),
      do: GenServer.start_link(__MODULE__, args, name: Keyword.fetch!(args, :name))

    @impl GenServer
    def init(args) do
      Process.flag(:trap_exit, true)

      {:ok, conn} = AMQPX.ConnectionPool.get(Keyword.fetch!(args, :connection))
      {:ok, ch} = AMQP.Channel.open(conn)
      AMQPX.link_channel(ch)

      :ok = AMQP.Basic.qos(ch, prefetch_count: 1)

      {:ok, %{queue: queue}} = AMQP.Queue.declare(ch, "", auto_delete: true)
      {:ok, ctag} = AMQP.Basic.consume(ch, queue)

      schedule_cleanup()

      state = %__MODULE__{
        ch: ch,
        queue: queue,
        exchange: args[:exchange],
        routing_key: args[:routing_key],
        codecs: AMQPX.Codec.codecs(args[:codecs] || %{}),
        mime_type:
          AMQPX.Codec.expand_mime_shortcut(args[:mime_type] || "application/octet-stream"),
        ctag: ctag,
        pending_calls: %{}
      }

      {:ok, state}
    end

    @impl GenServer
    def handle_call(
          {:call, exchange, routing_key, payload, options, timeout},
          from,
          state = %__MODULE__{ch: ch, mime_type: mime_type, codecs: codecs}
        ) do
      uuid = UUID.uuid4()

      with ex = exchange || state.exchange,
           rk = routing_key || state.routing_key,
           {:exchange, true} <- {:exchange, ex != nil},
           {:routing_key, true} <- {:routing_key, rk != nil} do
        call = %Call{caller: from, expire_at: Time.utc_now() |> Time.add(timeout, :millisecond)}

        {:ok, payload} = AMQPX.Codec.encode(payload, mime_type, codecs)

        headers = Keyword.get(options, :headers, [])

        args =
          [
            content_type: mime_type,
            reply_to: state.queue,
            correlation_id: Keyword.get(options, :correlation_id, uuid),
            message_id: Keyword.get(options, :message_id, uuid),
            headers: headers
          ]
          |> add_expiration(options, timeout)

        case AMQP.Basic.publish(
               ch,
               ex,
               rk,
               payload,
               args
             ) do
          :ok ->
            pending = Map.put(state.pending_calls, uuid, call)
            {:noreply, %__MODULE__{state | pending_calls: pending}}

          err ->
            {:reply, {:error, err}, state}
        end
      else
        {:exchange, _} ->
          {:reply, {:error, :no_exchange}, state}

        {:routing_key, _} ->
          {:reply, {:error, :no_routing_key}, state}
      end
    end

    @impl GenServer
    def handle_info({:basic_consume_ok, %{consumer_tag: ctag}}, state = %__MODULE__{ctag: ctag}) do
      {:noreply, state}
    end

    def handle_info({:basic_deliver, payload, meta}, state = %__MODULE__{ch: ch, codecs: codecs}) do
      AMQP.Basic.ack(ch, meta.delivery_tag)
      id = meta.correlation_id

      case state.pending_calls do
        pending = %{^id => call} ->
          parsed = AMQPX.Codec.decode(payload, meta, codecs)
          GenServer.reply(call.caller, parsed)
          pending = Map.delete(pending, id)
          {:noreply, %__MODULE__{state | pending_calls: pending}}

        _ ->
          {:noreply, state}
      end
    end

    def handle_info(:cleanup, state) do
      now = Time.utc_now()

      pending =
        state.pending_calls
        |> Enum.reject(&expired?(&1, now))
        |> Enum.into(%{})

      schedule_cleanup()
      {:noreply, %__MODULE__{state | pending_calls: pending}}
    end

    def handle_info(_, state) do
      {:noreply, state}
    end

    defp add_expiration(args, opts, timeout) do
      if Keyword.get(opts, :expire) do
        Keyword.put(args, :expiration, Integer.to_string(timeout))
      else
        args
      end
    end

    defp expired?({_, %Call{expire_at: expire_at}}, now),
      do: Time.compare(expire_at, now) == :lt

    defp schedule_cleanup,
      do: :erlang.send_after(60_000, self(), :cleanup)
  end
end
