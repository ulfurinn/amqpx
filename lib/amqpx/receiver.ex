defmodule AMQPX.Receiver do
  use Supervisor

  def start_link(args),
    do: Supervisor.start_link(__MODULE__, args)

  def init(args) do
    worker_args = Keyword.get(args, :worker, [])
    children = [
      {AMQPX.Watchdog, args},
      worker_spec(worker_args)
    ]
    Supervisor.init(children, strategy: :one_for_all)
  end

  defp worker_spec(module) when is_atom(module) do
    module
  end
  defp worker_spec(spec = {module, _args}) when is_atom(module) do
    spec
  end
  defp worker_spec(args) do
    {AMQPX.Receiver.Standard, args}
  end
end
