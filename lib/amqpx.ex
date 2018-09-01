defmodule AMQPX do
  @doc """
  Sets up a cleanup link between the caller and the channel.

  If the caller crashes, the channel will be closed to avoid leaks.

  If the channel crashes, the caller will receive `{:channel_died, channel, reason}`.
  """
  @spec link_channel(AMQP.Channel.t()) :: any()
  def link_channel(ch = %AMQP.Channel{pid: pid}) do
    watcher = self()

    on_exit(pid, fn reason ->
      send(watcher, {:channel_died, ch, reason})
    end)

    on_exit(watcher, fn _reason ->
      AMQP.Channel.close(ch)
    end)
  end

  defp on_exit(pid, fun) do
    ref = make_ref()
    watcher = self()

    spawn(fn ->
      Process.monitor(pid)
      send(watcher, {:monitor_set_up, ref})

      receive do
        {:DOWN, _, _, ^pid, reason} ->
          fun.(reason)
      end
    end)

    receive do
      {:monitor_set_up, ^ref} -> :ok
    end
  end

end
