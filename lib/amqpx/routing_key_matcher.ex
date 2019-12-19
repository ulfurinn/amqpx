defmodule AMQPX.RoutingKeyMatcher do
  @doc """
  Checks if a routing key matches a topic-style pattern.

    iex> AMQPX.RoutingKeyMatcher.matches?("a", "a")
    true

    iex> AMQPX.RoutingKeyMatcher.matches?("a", "b")
    false

    iex> AMQPX.RoutingKeyMatcher.matches?("a.c", "a.c")
    true

    iex> AMQPX.RoutingKeyMatcher.matches?("a.c", "b.c")
    false

    iex> AMQPX.RoutingKeyMatcher.matches?("c.a", "c.b")
    false

    iex> AMQPX.RoutingKeyMatcher.matches?("a.b.c", "a.*.c")
    true

    iex> AMQPX.RoutingKeyMatcher.matches?("a.c", "a.*.c")
    false

    iex> AMQPX.RoutingKeyMatcher.matches?("a.b.c.d", "a.*.d")
    false

    iex> AMQPX.RoutingKeyMatcher.matches?("a", "#")
    true

    iex> AMQPX.RoutingKeyMatcher.matches?("a.b.c", "#")
    true

    iex> AMQPX.RoutingKeyMatcher.matches?("a", "a.#")
    true

    iex> AMQPX.RoutingKeyMatcher.matches?("a", "#.a")
    true

    iex> AMQPX.RoutingKeyMatcher.matches?("a.b", "a.#")
    true

    iex> AMQPX.RoutingKeyMatcher.matches?("a.c", "a.#.c")
    true

    iex> AMQPX.RoutingKeyMatcher.matches?("a.b.c", "a.#.c")
    true

    iex> AMQPX.RoutingKeyMatcher.matches?("a.b.c.d", "a.#.d")
    true

  """

  # all parts match
  def matches?([], []), do: true
  def matches?([], ["#"]), do: true

  def matches?(k = [_ | t1], p = ["#" | t2]) do
    # matches exactly one, or matches more than one, or matches zero
    matches?(t1, t2) || matches?(t1, p) || matches?(k, t2)
  end

  def matches?([_ | t1], ["*" | t2]), do: matches?(t1, t2)

  # literal head matches
  def matches?([x | t1], [x | t2]), do: matches?(t1, t2)

  # no match
  def matches?(key, pattern) when is_list(key) and is_list(pattern) and key != pattern, do: false

  # convert
  def matches?(key, pattern) when is_binary(key) and is_binary(pattern) do
    key = key |> String.split(".")
    pattern = pattern |> String.split(".")
    matches?(key, pattern)
  end

  def wildcard?(pattern) when is_binary(pattern), do: pattern |> String.split(".") |> wildcard?()
  def wildcard?(pattern) when is_list(pattern), do: pattern |> Enum.any?(&(&1 in ["*", "#"]))
end
