defmodule AMQPX.Codec do
  def codecs do
    default_codecs()
    |> Map.merge(global_codecs())
    |> expand_mime_shortcut()
  end

  def codecs(custom_codecs) do
    default_codecs()
    |> Map.merge(global_codecs())
    |> Map.merge(custom_codecs)
    |> expand_mime_shortcut()
  end

  def default_codecs() do
    %{
      "text/plain" => AMQPX.Codec.Text,
      "application/octet-stream" => AMQPX.Codec.Text
    }
  end

  def global_codecs() do
    Application.get_env(:ulfnet_amqpx, :codecs, %{})
  end

  def expand_mime_shortcut(codecs) when is_map(codecs),
    do: codecs |> Enum.into(%{}, &expand_mime_shortcut/1)

  def expand_mime_shortcut({mime, codec}), do: {expand_mime_shortcut(mime), codec}

  def expand_mime_shortcut(mime) when is_binary(mime), do: mime

  def expand_mime_shortcut(:json), do: "application/json"
  def expand_mime_shortcut(:text), do: "text/plain"
  def expand_mime_shortcut(:binary), do: "application/octet-stream"

  defp codec(%{content_type: content_type}, codecs),
    do: Map.get(codecs, expand_mime_shortcut(content_type))

  defp codec(content_type, codecs),
    do: Map.get(codecs, expand_mime_shortcut(content_type))

  def encode(payload, content_type, codecs, handler \\ nil) do
    case codec(content_type, codecs) |> or_handler(handler) do
      nil ->
        {:error, {:no_codec, content_type}}

      codec when is_atom(codec) ->
        codec.encode(payload)

      {codec, %{encode: args}} ->
        apply(codec, :encode, [payload] ++ args)

      {codec, _} ->
        codec.encode(payload)
    end
  end

  def decode(payload, content_type, codecs, handler \\ nil) do
    case codec(content_type, codecs) |> or_handler(handler) do
      nil ->
        {:error, {:no_codec, content_type}}

      codec when is_atom(codec) ->
        codec.decode(payload)

      {codec, %{decode: args}} ->
        apply(codec, :decode, [payload] ++ args)

      {codec, _} ->
        codec.decode(payload)
    end
  end

  defp or_handler(:handler, handler), do: handler
  defp or_handler(codec, _), do: codec
end
