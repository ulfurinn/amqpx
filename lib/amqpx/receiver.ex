defmodule AMQPX.Receiver do
  use Supervisor

  @moduledoc """
  A message handler with transparent connection recovery.
  """

  def start_link(args),
    do: Supervisor.start_link(__MODULE__, args)

  @impl Supervisor
  def init(args) do
    worker_args = Keyword.get(args, :worker, [])

    children = [
      {AMQPX.Watchdog, args},
      worker_spec(args, worker_args)
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

  defp worker_spec(args, module) when is_atom(module) do
    {module, [connection: args[:connection]]}
  end

  defp worker_spec(args, {module, worker_args}) when is_atom(module) do
    {module, Keyword.put(worker_args, :connection, args[:connection])}
  end

  defp worker_spec(args, worker_args) when is_list(worker_args) do
    {AMQPX.Receiver.Standard, Keyword.put(worker_args, :connection, args[:connection])}
  end
end
