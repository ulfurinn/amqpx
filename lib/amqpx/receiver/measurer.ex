defmodule AMQPX.Receiver.Measurer do
  @callback measure_packet_handler(fun :: function(), meta :: Map.t()) :: any()
end
