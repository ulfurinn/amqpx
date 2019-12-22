defmodule AMQPX.SharedChannel do
  @moduledoc """
  A reference-counted lifetime tracker for AMQP channels.

  A wrapper around channels that allows several processes to share it and
  disposes of it when the last registered user exits.

  ## Motivation

  Applications are normally encouraged to allocate as many AMQP channels as
  they require, but it is a finite resource and can be exceeded and lead to
  fatal broker errors if an application frequently creates channels and does
  not dispose of them.

  One such scenario is when you want to spawn off tasks that publish to a
  centrally owned channel, but the tasks can outlive the owner, so it is
  unsafe for the owner to close the channel when it itself terminates, as the
  publishers might not get a chance to finish their work.

  `AMQPX.SharedChannel` takes over the responsibility of making sure that the
  channel is eventually disposed of, but not before everyone is done using it.

  """
  use GenServer
  require Logger

  defstruct [
    :ch,
    :users
  ]

  @doc false
  def child_spec(_), do: raise("#{__MODULE__} is not supposed to be supervised")

  @doc "Start the wrapper process."
  def start(ch),
    do: GenServer.start(__MODULE__, {ch, self()})

  @doc "Add the current process as a user of the shared channel."
  def share(shared_channel_pid),
    do: GenServer.call(shared_channel_pid, {:use, self()})

  @doc "Add the specified process as a user of the shared channel."
  def share(shared_channel_pid, user_pid),
    do: GenServer.call(shared_channel_pid, {:use, user_pid})

  @impl GenServer
  def init({ch, pid}) do
    Process.flag(:trap_exit, true)
    Process.monitor(pid)
    state = %__MODULE__{ch: ch, users: MapSet.new([pid])}
    {:ok, state}
  end

  @impl GenServer
  def handle_call({:use, pid}, _, state = %__MODULE__{users: users}) do
    Process.monitor(pid)
    state = %__MODULE__{state | users: MapSet.put(users, pid)}
    {:reply, nil, state}
  end

  @impl GenServer
  def handle_info({:DOWN, _, _, pid, _}, state = %__MODULE__{users: users}) do
    users = MapSet.delete(users, pid)
    state = %__MODULE__{state | users: users}

    if MapSet.size(users) == 0 do
      {:stop, :normal, state}
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def terminate(_, %__MODULE__{ch: ch}) do
    AMQP.Channel.close(ch)
  end
end
