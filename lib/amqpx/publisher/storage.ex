defmodule AMQPX.Publisher.Storage do
  @callback init(opts :: any()) :: any()
  @callback store(integer(), %AMQPX.Publisher.Record{}, any()) :: any()
  @callback confirm(integer(), boolean(), any()) :: any()
  @callback pending(any()) :: list(%AMQPX.Publisher.Record{})
end
