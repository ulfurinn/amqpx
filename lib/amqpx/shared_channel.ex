defmodule AMQPX.SharedChannel do
  use GenServer
  require Logger

  defstruct [
    :ch,
    :users
  ]

  def start(ch),
    do: GenServer.start(__MODULE__, {ch, self()})

  def share(pid),
    do: GenServer.call(pid, {:use, self()})

  def init({ch, pid}) do
    Process.flag(:trap_exit, true)
    Process.monitor(pid)
    state = %__MODULE__{ch: ch, users: MapSet.new([pid])}
    {:ok, state}
  end

  def handle_call({:use, pid}, _, state = %__MODULE__{users: users}) do
    Process.monitor(pid)
    state = %__MODULE__{state | users: MapSet.put(users, pid)}
    {:reply, nil, state}
  end

  def handle_info({:DOWN, _, _, pid, _}, state = %__MODULE__{users: users}) do
    users = MapSet.delete(users, pid)
    state = %__MODULE__{state | users: users}

    if MapSet.size(users) == 0 do
      {:stop, :normal, state}
    else
      {:noreply, state}
    end
  end

  def terminate(_, state = %__MODULE__{ch: ch}) do
    AMQP.Channel.close(ch)
  end
end
