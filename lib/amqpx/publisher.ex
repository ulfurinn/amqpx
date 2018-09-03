defmodule AMQPX.Publisher do
  use GenServer
  require Logger

  defstruct [
    :conn_name,
    :ch,
    :storage_mod,
    :storage_state
  ]

  def child_spec(args) do
    %{
      id: Keyword.get(args, :id, __MODULE__),
      start: {__MODULE__, :start_link, [args]},
      shutdown: Keyword.get(args, :shutdown, :infinity)
    }
  end

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

  def publish(name, exchange, rk, payload, options \\ []),
    do: GenServer.call(name, {:publish, {exchange, rk, payload, options}})

  @impl GenServer
  def init(opts) do
    conn = opts |> Keyword.fetch!(:connection)
    storage = opts |> Keyword.fetch!(:storage)

    state =
      %__MODULE__{conn_name: conn, storage_mod: storage, storage_state: init_storage(storage)}
      |> connect

    {:ok, state}
  end

  defp init_storage({mod, opts}),
    do: mod.init(opts)

  defp init_storage(mod) when is_atom(mod),
    do: mod.init(nil)

  @impl GenServer
  def handle_call(msg = {:publish, _}, _, state) do
    {:reply, :ok, send_msg(msg, state)}
  end

  @impl GenServer
  def handle_info({:DOWN, _, _, _, _}, state) do
    schedule_recovery()
    {:noreply, %__MODULE__{state | ch: nil}}
  end

  def handle_info(:try_recover, state) do
    {:noreply, connect(state)}
  end

  def handle_info(
        {:"basic.ack", id, multiple},
        state = %__MODULE__{storage_mod: storage_mod, storage_state: storage_state}
      ) do
    storage_state = storage_mod.confirm(id, multiple, storage_state)
    {:noreply, %__MODULE__{state | storage_state: storage_state}}
  end

  @impl GenServer
  def terminate(_, %__MODULE__{ch: ch}) do
    if ch,
      do: AMQP.Channel.close(ch)
  end

  defp connect(state = %__MODULE__{conn_name: conn_name}) do
    case AMQPX.ConnectionPool.get(conn_name) do
      {:ok, conn} ->
        %__MODULE__{state | ch: init_channel(conn)}
        |> resend

      err ->
        Logger.error(inspect(err))
        schedule_recovery()
        state
    end
  end

  defp init_channel(conn) do
    case conn |> AMQP.Channel.open() do
      {:ok, ch} ->
        ch |> configure_channel()

      err ->
        Logger.error(inspect(err))
        schedule_recovery()
        nil
    end
  end

  defp configure_channel(nil), do: nil

  defp configure_channel(ch) do
    Process.monitor(ch.pid)
    AMQP.Confirm.select(ch)
    :amqp_channel.register_confirm_handler(ch.pid, self())
    :amqp_channel.register_return_handler(ch.pid, self())
    ch
  end

  defp schedule_recovery() do
    :erlang.send_after(1_000, self(), :try_recover)
  end

  defp resend(state = %__MODULE__{storage_mod: storage_mod, storage_state: storage_state}) do
    {backlog, storage_state} = storage_mod.handoff(storage_state)
    state = %__MODULE__{state | storage_state: storage_state}
    backlog |> Enum.reduce(state, &send_msg/2)
  end

  defp send_msg(
         msg = {:publish, _},
         state = %__MODULE__{ch: nil, storage_mod: storage_mod, storage_state: storage_state}
       ) do
    storage_state = storage_mod.append(0, msg, storage_state)
    %__MODULE__{state | storage_state: storage_state}
  end

  defp send_msg(
         msg = {:publish, {exchange, routing_key, payload, options}},
         state = %__MODULE__{ch: ch, storage_mod: storage_mod, storage_state: storage_state}
       ) do
    id = :amqp_channel.next_publish_seqno(ch.pid)
    storage_state = storage_mod.append(id, msg, storage_state)
    AMQP.Basic.publish(ch, exchange, routing_key, payload, options)
    %__MODULE__{state | storage_state: storage_state}
  end
end
