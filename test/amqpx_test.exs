defmodule AMQPX.Test do
  use ExUnit.Case

  test "can receive a message and send back a response" do
    {:ok, _} = AMQPX.Receiver.start_link(receiver_args())

    {:ok, %{queue: queue}} = AMQP.Queue.declare(ch(), "", auto_delete: true)
    {:ok, _} = AMQP.Basic.consume(ch(), queue)

    :ok =
      AMQP.Basic.publish(
        ch(),
        "test",
        "ok",
        "[5]",
        reply_to: queue,
        correlation_id: "dead-beef",
        content_type: "application/json"
      )

    receive do
      {:basic_deliver, "[50]",
       meta = %{content_type: "application/json", correlation_id: "dead-beef"}} ->
        AMQP.Basic.ack(ch(), meta.delivery_tag)
    after
      1000 ->
        flunk("timeout")
    end
  end

  test "can receive a message with a wildcard key" do
    {:ok, _} =
      AMQPX.Receiver.start_link(
        receiver_args(worker: [keys: %{{"test", "a.#"} => __MODULE__.Handler}])
      )

    {:ok, %{queue: queue}} = AMQP.Queue.declare(ch(), "", auto_delete: true)
    {:ok, _} = AMQP.Basic.consume(ch(), queue)

    :ok =
      AMQP.Basic.publish(
        ch(),
        "test",
        "a.b.c",
        "[5]",
        reply_to: queue,
        correlation_id: "dead-beef",
        content_type: "application/json"
      )

    receive do
      {:basic_deliver, "[50]",
       meta = %{content_type: "application/json", correlation_id: "dead-beef"}} ->
        AMQP.Basic.ack(ch(), meta.delivery_tag)
    after
      1000 ->
        flunk("timeout")
    end
  end

  test "can send a message using the confirming publisher with the memory backend" do
    {:ok, _} = AMQPX.Receiver.start_link(receiver_args())

    {:ok, _} =
      AMQPX.Publisher.start_link(
        name: :publisher,
        connection: :publish,
        storage: AMQPX.Publisher.Storage.Memory
      )

    {:ok, %{queue: queue}} = AMQP.Queue.declare(ch(), "", auto_delete: true)
    {:ok, _} = AMQP.Basic.consume(ch(), queue)

    message_count = 100

    spawn(fn ->
      1..message_count
      |> Enum.each(fn i ->
        :ok =
          AMQPX.Publisher.publish(
            :publisher,
            "test",
            "ok",
            "[#{i}]",
            reply_to: queue,
            correlation_id: "dead-beef",
            content_type: "application/json"
          )

        Process.sleep(100)
      end)
    end)

    Process.sleep(3000)
    {:ok, %{pid: pid}} = AMQPX.ConnectionPool.get(:publish)
    :erlang.exit(pid, :kill_connection)

    Process.sleep(3000)
    {:ok, %{pid: pid}} = AMQPX.ConnectionPool.get(:publish)
    :erlang.exit(pid, :kill_connection)

    1..message_count
    |> Enum.each(fn i ->
      receive do
        {:basic_deliver, payload,
         meta = %{content_type: "application/json", correlation_id: "dead-beef"}} ->
          AMQP.Basic.ack(ch(), meta.delivery_tag)

          if payload != "[#{i * 10}]" do
            flunk("out of order")
          end
      end
    end)
  end

  test "can send a message using the confirming publisher with the DETS backend" do
    {:ok, _} = AMQPX.Receiver.start_link(receiver_args())

    {:ok, _} =
      AMQPX.Publisher.start_link(
        name: :publisher,
        connection: :publish,
        storage: {AMQPX.Publisher.Storage.DETS, "publisher.dets"}
      )

    {:ok, %{queue: queue}} = AMQP.Queue.declare(ch(), "", auto_delete: true)
    {:ok, _} = AMQP.Basic.consume(ch(), queue)

    message_count = 100

    spawn(fn ->
      1..message_count
      |> Enum.each(fn i ->
        :ok =
          AMQPX.Publisher.publish(
            :publisher,
            "test",
            "ok",
            "[#{i}]",
            reply_to: queue,
            correlation_id: "dead-beef",
            content_type: "application/json"
          )

        Process.sleep(100)
      end)
    end)

    Process.sleep(3000)
    {:ok, %{pid: pid}} = AMQPX.ConnectionPool.get(:publish)
    :erlang.exit(pid, :kill_connection)

    Process.sleep(3000)
    {:ok, %{pid: pid}} = AMQPX.ConnectionPool.get(:publish)
    :erlang.exit(pid, :kill_connection)

    1..message_count
    |> Enum.each(fn i ->
      receive do
        {:basic_deliver, payload,
         meta = %{content_type: "application/json", correlation_id: "dead-beef"}} ->
          AMQP.Basic.ack(ch(), meta.delivery_tag)

          if payload != "[#{i * 10}]" do
            flunk("out of order: expected #{i * 10}, got #{payload}")
          end
      end
    end)
  end

  test "can make RPC calls" do
    {:ok, _} = AMQPX.Receiver.start_link(receiver_args())

    {:ok, _} = AMQPX.RPC.start_link(rpc_args())

    assert {:ok, [50]} = AMQPX.RPC.call(RPCTest, [5], 10000)
  end

  test "can limit retries" do
    {:ok, _} = __MODULE__.StatefulHandler.start_link()

    {:ok, _} =
      AMQPX.Receiver.start_link(
        receiver_args(
          worker: [
            keys: %{{"test", "ok"} => __MODULE__.StatefulHandler},
            retry: [limit: 5, identity: [:message_id]]
          ]
        )
      )

    {:ok, _} = AMQPX.RPC.start_link(rpc_args())

    catch_exit(AMQPX.RPC.call(RPCTest, [5], [message_id: "qwerty"], 500))

    assert 6 = __MODULE__.StatefulHandler.count()
  end

  test "can deduplicate messages" do
    {:ok, _} = __MODULE__.Deduplicator.start_link()

    {:ok, _} =
      AMQPX.Receiver.start_link(
        receiver_args(
          worker: [
            keys: %{{"test", "ok"} => __MODULE__.Deduplicator},
            deduplicate: [identity: [:message_id], callback: __MODULE__.Deduplicator]
          ]
        )
      )

    uuid = UUID.uuid4()

    :ok =
      AMQP.Basic.publish(ch(), "test", "ok", "true",
        content_type: "application/json",
        message_id: uuid
      )

    :ok =
      AMQP.Basic.publish(ch(), "test", "ok", "true",
        content_type: "application/json",
        message_id: uuid
      )

    Process.sleep(500)
    assert 1 = __MODULE__.Deduplicator.count()
  end

  setup _ do
    {:ok, conn} = AMQPX.ConnectionPool.get(:test)
    {:ok, ch} = AMQP.Channel.open(conn)
    AMQPX.link_channel(ch)
    :erlang.put(:channel, ch)
    :ok
  end

  defp ch,
    do: :erlang.get(:channel)

  defp receiver_args(custom \\ []) do
    {custom_worker, custom} = Keyword.pop(custom, :worker, [])

    [
      connection: :test,
      worker:
        [
          declare_exchanges: [[name: "test", type: :topic, options: [durable: true]]],
          codecs: %{"application/json" => Jason},
          keys: %{
            {"test", "ok"} => AMQPX.Test.Handler
          }
        ]
        |> Keyword.merge(custom_worker)
    ]
    |> Keyword.merge(custom)
  end

  defp rpc_args(custom \\ []) do
    [
      name: RPCTest,
      connection: :test,
      exchange: "test",
      routing_key: "ok",
      codecs: %{"application/json" => Jason},
      mime_type: "application/json"
    ]
    |> Keyword.merge(custom)
  end

  defmodule Handler do
    @behaviour AMQPX.Receiver.Standard

    def handle([input], _meta), do: [input * 10]

    def format_response(resp, _meta), do: {{:mime_type, :json}, resp}
  end

  defmodule StatefulHandler do
    @behaviour AMQPX.Receiver.Standard
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

    def init(_), do: {:ok, 0}

    def handle(_, _) do
      GenServer.call(__MODULE__, :increment)
      raise "test"
    end

    def format_response(body, _), do: %{error: inspect(body)}

    def requeue?(), do: true

    def count, do: GenServer.call(__MODULE__, :count)

    def handle_call(:increment, _, count), do: {:reply, nil, count + 1}
    def handle_call(:count, _, count), do: {:reply, count, count}
  end

  defmodule Deduplicator do
    @behaviour AMQPX.Receiver.Standard.Deduplicate
    @behaviour AMQPX.Receiver.Standard
    use GenServer

    def start_link, do: GenServer.start_link(__MODULE__, nil, name: __MODULE__)

    def already_seen?(id), do: GenServer.call(__MODULE__, {:already_seen?, id})
    def remember_message(id), do: GenServer.call(__MODULE__, {:remember_message, id})
    def increment, do: GenServer.call(__MODULE__, :increment)
    def count, do: GenServer.call(__MODULE__, :count)

    def init(_) do
      table = :ets.new(__MODULE__, [:set])
      {:ok, {table, 0}}
    end

    def handle_call({:already_seen?, id}, _, state = {table, _}) do
      seen =
        case :ets.lookup(table, id) do
          [] -> false
          _ -> true
        end

      {:reply, seen, state}
    end

    def handle_call({:remember_message, id}, _, state = {table, _}) do
      true = :ets.insert(table, {id})
      {:reply, nil, state}
    end

    def handle_call(:increment, _, {table, count}) do
      {:reply, nil, {table, count + 1}}
    end

    def handle_call(:count, _, state = {_, count}) do
      {:reply, count, state}
    end

    def handle(_, _) do
      increment()
      true
    end

    def format_response(_, _), do: nil
  end
end
