defmodule AMQPX.Receiver.Standard do
  use GenServer
  require Logger

  defstruct [
    :conn,
    :ch,
    :ctag,
    :handlers,
    :task_sup,
  ]

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]},
      shutdown: :infinity
    }
  end

  def start_link(args),
    do: GenServer.start_link(__MODULE__, args)

  def init(args) do
    Process.flag(:trap_exit, true)

    {:ok, conn} = AMQPX.ConnectionPool.get(Keyword.fetch!(args, :connection))
    {:ok, ch} = AMQP.Channel.open(conn)
    :ok = AMQP.Basic.qos(ch, prefetch_count: Keyword.get(args, :prefetch, 1))

    {ex_type, ex_name, ex_opts} = case Keyword.fetch!(args, :exchange) do
      {type, name, opts} -> {type, name, opts}
      {type, name} -> {type, name, [durable: true]}
      name -> {:topic, name, [durable: true]}
    end
    :ok = apply(AMQP.Exchange, ex_type, [ch, ex_name, ex_opts])

    {q_name, q_opts} = case Keyword.get(args, :queue) do
      nil -> {"", []}
      name when is_binary(name) -> {name, []}
      opts when is_list(opts) -> {"", opts}
      {name, opts} -> {name, opts}
    end
    {:ok, %{queue: queue}} = AMQP.Queue.declare(ch, q_name, q_opts)

    bind = fn(rk) ->
      :ok = AMQP.Queue.bind(ch, queue, ex_name, routing_key: rk)
    end

    Keyword.fetch!(args, :keys) |> Map.keys |> Enum.each(bind)

    {:ok, ctag} = AMQP.Basic.consume(ch, queue)

    state = %__MODULE__{
      conn: conn,
      ch: ch,
      ctag: ctag,
      handlers: Keyword.fetch!(args, :keys),
      task_sup: Keyword.get(args, :supervisor, AMQPX.Application.task_supervisor())
    }

    {:ok, state}
  end
end
