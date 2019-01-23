defmodule AMQPX.ConnectionPool do
  use GenServer
  require Logger

  @moduledoc """
  A pool of connections for shared use.

  AMQPX encourages using multiple channels per TCP connection instead of multiple connections, wherever possible.
  `AMQPX.ConnectionPool` stores open connections that other modules can retrieve from it.

  # Configuration

  ```elixir
  config :ulfnet_amqpx, connections: %{name => url}
  ```
  """

  defstruct connections: %{},
            config: []

  @doc false
  def child_spec(_) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []}
    }
  end

  @doc false
  def start_link(),
    do: GenServer.start(__MODULE__, nil, name: __MODULE__)

  @doc "Fetch a connection by its name."
  @spec get(name :: atom(), timeout :: integer() | :infinity) ::
          {:ok, AMQP.Connection.t()} | {:error, reason :: any()}
  def get(name, timeout \\ :infinity),
    do: GenServer.call(__MODULE__, {:get, name}, timeout)

  @doc "Register a connection config outside of `sys.config`."
  @spec register(name :: atom(), url :: any()) :: any()
  def register(name, url),
    do: GenServer.call(__MODULE__, {:register, name, url})

  @doc false
  def update_config,
    do: GenServer.call(__MODULE__, :update_config)

  @impl GenServer
  def init(_) do
    config = Application.get_env(:ulfnet_amqpx, :connections, [])
    state = %__MODULE__{config: config}
    {:ok, state}
  end

  @impl GenServer
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

  def handle_call(:update_config, _, state = %__MODULE__{config: config}) do
    new_config = Application.get_env(:ulfnet_amqpx, :connections, [])
    config = Map.merge(config, new_config)
    state = %__MODULE__{state | config: config}
    {:reply, config, state}
  end

  def handle_call({:register, name, url}, _, state = %__MODULE__{config: config}) do
    state = %__MODULE__{state | config: Map.put(config, name, url)}
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({:connection_down, id, _reason}, state = %__MODULE__{connections: connections}) do
    Logger.error("lost connection #{id}")
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

    spawn(fn ->
      pid = conn.pid
      Process.monitor(pid)
      send(pool, {:monitor_setup, ref})

      receive do
        {:DOWN, _, :process, ^pid, reason} ->
          send(pool, {:connection_down, id, reason})
      end
    end)

    receive do
      {:monitor_setup, ^ref} -> nil
    end
  end
end
