defmodule AMQPX.ConnectionPool do
  use GenServer

  defstruct [
    connections: %{},
    config: []
  ]

  def start_link(args),
    do: GenServer.start(__MODULE__, args, name: __MODULE__)

  def get(id),
    do: GenServer.call(__MODULE__, {:get, id})

  def init(_) do
    config = Application.get_env(:ulfnet_amqpx, :connections, [])
    state = %__MODULE__{config: config}
    {:ok, state}
  end

  def handle_call({:get, id}, _, state = %__MODULE__{connections: connections, config: config}) do
    case connections do
      %{^id => conn} ->
        {:reply, {:ok, conn}, state}
      _ ->
        case try_connect(id, config) do
          resp = {:ok, conn} ->
            setup_monitor(id, conn)
            connections = Map.put(connections, id, conn)
            {:reply, resp, %__MODULE__{state | connections: connections}}
          err ->
            {:reply, err, state}
        end
    end
  end

  def handle_info({:connection_down, id, _reason}, state = %__MODULE__{connections: connections}) do
    connections = Map.delete(connections, id)
    {:noreply, %__MODULE__{state | connections: connections}}
  end

  defp try_connect(id, config) do
    case config do
      %{^id => url} ->
        AMQP.Connection.open(url)
      _ ->
        {:error, :not_configured}
    end
  end

  defp setup_monitor(id, conn) do
    ref = make_ref()
    pool = self()
    spawn fn ->
      pid = conn.pid
      Process.monitor(pid)
      send(pool, {:monitor_setup, ref})
      receive do
        {:DOWN, _, :process, ^pid, reason} ->
          send(pool, {:connection_down, id, reason})
      end
    end
    receive do
      {:monitor_setup, ^ref} -> nil
    end
  end
end
