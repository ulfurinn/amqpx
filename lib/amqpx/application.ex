defmodule AMQPX.Application do
  use Application

  @moduledoc false

  def start(_type, _args) do
    children = [
      {Task.Supervisor, name: task_supervisor(), shutdown: :infinity},
      AMQPX.ConnectionPool
    ]

    opts = [strategy: :one_for_one, name: AMQPX.Supervisor]
    Supervisor.start_link(children, opts)
  end

  def task_supervisor,
    do: AMQPX.DefaultTaskSupervisor
end
