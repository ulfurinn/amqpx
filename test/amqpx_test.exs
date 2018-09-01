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

  test "can send a message using the confirming publisher" do
    {:ok, _} = AMQPX.Receiver.start_link(receiver_args())

    {:ok, _} =
      AMQPX.Publisher.start_link(
        name: :publisher,
        connection: :test,
        storage: AMQPX.Publisher.Storage.Memory
      )

    {:ok, %{queue: queue}} = AMQP.Queue.declare(ch(), "", auto_delete: true)
    {:ok, _} = AMQP.Basic.consume(ch(), queue)

    :ok =
      AMQPX.Publisher.publish(
        :publisher,
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
        exchange: {:topic, "test", declare: true},
        codecs: %{"application/json" => Jason},
        keys: %{
          "ok" => AMQPX.Test.Handler
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

    def format_response(resp, _meta), do: {:json, resp}
  end
end
