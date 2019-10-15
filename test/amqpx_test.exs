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

    assert {:ok, [50]} = AMQPX.RPC.call(RPCTest, [5], 1000)
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

  defp receiver_args do
    [
      connection: :test,
      worker: [
        declare_exchanges: [[name: "test", type: :topic, options: [durable: true]]],
        codecs: %{"application/json" => Jason},
        keys: %{
          {"test", "ok"} => AMQPX.Test.Handler
        }
      ]
    ]
  end

  defp rpc_args do
    [
      name: RPCTest,
      connection: :test,
      exchange: "test",
      routing_key: "ok",
      codecs: %{"application/json" => Jason},
      mime_type: "application/json"
    ]
  end

  defmodule Handler do
    @behaviour AMQPX.Receiver.Standard

    def handle([input], _meta), do: [input * 10]

    def format_response(resp, _meta), do: {{:mime_type, :json}, resp}
  end
end
