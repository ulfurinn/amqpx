defmodule AMQPX.Receiver do
  use Supervisor

  @moduledoc """
  Provides message handling with transparent connection recovery.

  Starts a supervisor with a connection watchdog and a handler process.
  In case of connection loss the process group will be shutdown and restarted until it can reconnect.

  The message handler is responsible for setting up all its AMQP entities.
  `AMQPX.Receiver.Standard` should be used for simple cases as it provides some sane default behaviours and takes care of some common pitfalls.
  If you're implementing your own receiver, read the documentation of `AMQPX.Receiver.Standard` to be aware of them.
  """

  @type option ::
          {:connection, name :: atom()}
          | {:worker, module() | args :: Keyword.t() | {module(), args :: Keyword.t()}}
          | {:reconnect, seconds :: integer()}
          | {:max_restarts, integer()}
          | {:max_seconds, integer()}

  def child_spec(args) do
    %{
      id: Keyword.get(args, :id, __MODULE__),
      start: {__MODULE__, :start_link, [args]},
      type: :supervisor
    }
  end

  @doc """
  Starts the process group.

  ## Options

  * `:connection` – the connection name as registered with `AMQPX.ConnectionPool`
  * `:worker` – the handler module; see below
  * `:reconnect` – the interval between recovery attempts in case of connection loss

  `max_restarts` and `max_seconds` are used to set the supervisor's crash tolerance if both are set.
  Otherwise it is inferred from `:reconnect` to be able to keep retrying undefinitely until the broker comes back up.

  ## Worker settings

  If the worker module is not set, `AMQPX.Receiver.Standard` is used.

  Custom worker modules must export `start_link/1`, which will receive `args`.
  The `:connection` option will be added to `args` automatically.
  """
  @spec start_link([option]) :: {:ok, pid()}
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
