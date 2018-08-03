defmodule AMQPX.Watchdog do
  use GenServer
  require Logger

  @moduledoc false

  def start_link(args),
    do: GenServer.start_link(__MODULE__, args)

  def init(args) do
    id = Keyword.fetch!(args, :connection)
    interval = Keyword.get(args, :reconnect, 1) * 1000

    case AMQPX.ConnectionPool.get(id) do
      {:ok, conn} ->
        Process.monitor(conn.pid)
        {:ok, nil}

      {:error, reason} ->
        Logger.error("failed to connect to RabbitMQ: #{reason}; will reconnect in #{interval} ms")
        Process.sleep(interval)
        {:stop, {:shutdown, reason}}
    end
  end

  def handle_info({:DOWN, _, _, _, reason}, state) do
    {:stop, {:connection_down, reason}, state}
  end
end
