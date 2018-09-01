defmodule AMQPX.Publisher.Storage do
  @callback init(opts :: any()) :: any()
  @callback append(integer(), {:publish, String.t(), String.t(), String.t(), list()}, any()) ::
              any()
  @callback confirm(integer(), boolean(), any()) :: any()
  @callback handoff(any()) ::
              {list({:publish, String.t(), String.t(), String.t(), list()}), any()}
end
