defmodule Csvto.AssertionHelper do

  import ExUnit.Assertions

  def assert_only(fields, left, right) when is_list(left) and is_list(right)do
    fields = cast_fields(fields)
    Enum.zip(left, right) |> Enum.each(&assert_only(fields, elem(&1, 0), elem(&1, 1)))
  end

  def assert_only(fields, left, right) do
    fields = cast_fields(fields)
    left = maybe_cast_struct(left) |> cast_maps() |> Map.take(fields)
    right = maybe_cast_struct(right) |> cast_maps() |> Map.take(fields)
    assert left == right
  end

  defp maybe_cast_struct(%{__struct__: _} = struct) do
    Map.from_struct(struct)
  end

  defp maybe_cast_struct(map), do: map

  defp cast_fields(fields) do
    Enum.map(fields, &to_string(&1))
  end

  defp cast_maps(map) when is_map(map) do
    Enum.reduce_while(map, nil, fn
      {key, _}, nil when is_binary(key) -> {:cont, nil}
      {_, _}, _ -> {:halt, slow_cast_maps(map)}
    end) || map
  end

  defp slow_cast_maps(map) do
    for {key, value} <- map, into: %{} do
      {to_string(key), value}
    end
  end
end