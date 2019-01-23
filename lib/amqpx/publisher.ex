defmodule AMQPX.Publisher do
  use GenServer
  require Logger

  defstruct [
    :conn_name,
    :ch,
    :storage_mod,
    :storage_state,
    {:exchanges, []}
  ]

  defmodule Record do
    defstruct [
      :ref,
      :publish_seqno,
      :exchange,
      :routing_key,
      :payload,
      :options
    ]
  end

  def child_spec(args) do
    %{
      id: Keyword.get(args, :id, __MODULE__),
      start: {__MODULE__, :start_link, [args]},
      shutdown: Keyword.get(args, :shutdown, :infinity)
    }
  end

  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

  def publish(name, exchange, rk, payload, options \\ []) do
    record = %Record{
      exchange: exchange,
      routing_key: rk,
      payload: payload,
      options: options
    }

    GenServer.call(name, {:publish, record})
  end

  @impl GenServer
  def init(opts) do
    conn = opts |> Keyword.fetch!(:connection)
    storage = opts |> Keyword.fetch!(:storage)
    exchanges = opts[:exchanges] || []

    storage_mod =
      case storage do
        mod when is_atom(mod) -> mod
        {mod, _} when is_atom(mod) -> mod
      end

    state =
      %__MODULE__{
        conn_name: conn,
        storage_mod: storage_mod,
        storage_state: init_storage(storage),
        exchanges: exchanges
      }
      |> connect

    {:ok, state}
  end

  defp init_storage({mod, opts}),
    do: mod.init(opts)

  defp init_storage(mod) when is_atom(mod),
    do: mod.init(nil)

  @impl GenServer
  def handle_call({:publish, record}, _, state) do
    {:reply, :ok, send_msg(record, state)}
  end

  @impl GenServer
  def handle_info({:DOWN, _, _, pid, reason}, state = %__MODULE__{ch: %{pid: pid}}) do
    Logger.error("publishing connection died: #{reason}")
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

  def handle_info(_, state),
    do: {:noreply, state}

  @impl GenServer
  def terminate(_, %__MODULE__{ch: ch}) do
    if ch,
      do: AMQP.Channel.close(ch)
  end

  defp connect(state = %__MODULE__{conn_name: conn_name}) do
    case AMQPX.ConnectionPool.get(conn_name) do
      {:ok, conn} ->
        %__MODULE__{state | ch: init_channel(conn, state)}
        |> resend

      err ->
        Logger.error(inspect(err))
        schedule_recovery()
        state
    end
  end

  defp init_channel(conn, state) do
    case conn |> AMQP.Channel.open() do
      {:ok, ch} ->
        ch |> configure_channel(state)

      err ->
        Logger.error(inspect(err))
        schedule_recovery()
        nil
    end
  end

  defp configure_channel(nil, _), do: nil

  defp configure_channel(ch, %__MODULE__{exchanges: exchanges}) do
    Process.monitor(ch.pid)
    AMQP.Confirm.select(ch)
    :amqp_channel.register_confirm_handler(ch.pid, self())
    :amqp_channel.register_return_handler(ch.pid, self())

    exchanges
    |> Enum.each(fn exchange ->
      type = Keyword.fetch!(exchange, :type)
      name = Keyword.fetch!(exchange, :name)
      opts = Keyword.fetch!(exchange, :options)
      AMQP.Exchange.declare(ch, name, type, opts)
    end)

    ch
  end

  defp schedule_recovery() do
    :erlang.send_after(1_000, self(), :try_recover)
  end

  defp resend(state = %__MODULE__{storage_mod: storage_mod, storage_state: storage_state}) do
    backlog = storage_mod.pending(storage_state)

    if backlog != [],
      do: Logger.info("resending #{length(backlog)} messages")

    backlog |> Enum.reduce(state, &send_msg/2)
  end

  defp send_msg(
         record,
         state = %__MODULE__{ch: nil, storage_mod: storage_mod, storage_state: storage_state}
       ) do
    Logger.info("trying to send a message without a live connection, backlogging")
    storage_state = storage_mod.store(0, record, storage_state)
    %__MODULE__{state | storage_state: storage_state}
  end

  defp send_msg(
         record,
         state = %__MODULE__{ch: ch, storage_mod: storage_mod, storage_state: storage_state}
       ) do
    id = :amqp_channel.next_publish_seqno(ch.pid)
    storage_state = storage_mod.store(id, record, storage_state)
    AMQP.Basic.publish(ch, record.exchange, record.routing_key, record.payload, record.options)
    %__MODULE__{state | storage_state: storage_state}
  end
end
