defmodule Csvto.Type do

  @base      ~w(integer float boolean string binary decimal naive_datetime datetime date time)a
  @composite ~w(array)

  def primitive?({composite, _}) when composite in @composite, do: true
  def primitive?(base) when base in @base, do: true
  def primitive?(_), do: false

  def cast(:integer, value, _opts) do
    case Integer.parse(value) do
      {int, ""} -> {:ok, int}
      _ -> :error
    end
  end

  def cast(:float, value, _opts) do
    case Float.parse(value) do
      {float, ""} -> {:ok, float}
      _ -> :error
    end
  end

  @truely_value ~w(true 1 yes on)
  @falsely_value ~w(false 0 no off)

  def cast(:boolean, value, _opts) do
    case String.downcase(value) do
      truely when truely in @truely_value ->
        {:ok, true}
      falsely when falsely in @falsely_value ->
        {:ok, false}
      _ -> :error
    end
  end

  def cast(:string, value, _opts), do: {:ok, value}
  def cast(:binary, value, _opts), do: {:ok, value}
  def cast(:decimal, value, _opts), do: Decimal.parse(value)
  def cast(:naive_datetime, value, _opts) do
    cast_naive_datetime(value)
  end
  def cast(:datetime, value, _opts) do
    case cast_naive_datetime(value) do
      {:ok, %NaiveDateTime{year: year, month: month, day: day,
                           hour: hour, minute: minute, second: second, microsecond: microsecond}} ->
        {:ok, %DateTime{year: year, month: month, day: day,
                        hour: hour, minute: minute, second: second, microsecond: microsecond,
                        std_offset: 0, utc_offset: 0, zone_abbr: "UTC", time_zone: "Etc/UTC"}}
      {:ok, _} = ok ->
        ok
      :error ->
        :error
    end
  end

  def cast(:date, value, _opts) do
    case Date.from_iso8601(value) do
      {:ok, _} = ok -> ok
      {:error, _} -> :error
    end
  end

  def cast(:time, value, _opts) do
    case Time.from_iso8601(value) do
      {:ok, _} = ok -> ok
      {:error, _} -> :error
    end
  end

  def cast({:array, subtype}, value, opts) do
    with {:ok, elems} <- cast(:array, value, opts),
         {:ok, array} <- cast_children(subtype, elems, opts),
     do: {:ok, array |> Enum.reverse}
  end

  def cast(:array, value, opts) do
    separator = Map.get(opts, :separator, "|")
    {:ok, String.split(value, separator)}
  end

  def cast(_type, _value), do: :error

  defp cast_children(type, children, opts) do
    Enum.reduce_while(children, {:ok, []}, fn
      elem, {:ok, arr} ->
        case cast(type, elem, opts) do
          {:ok, value} -> {:cont, {:ok, [value | arr]}}
          :error -> {:halt, :error}
        end
    end)
  end

  defp cast_naive_datetime(binary) when is_binary(binary) do
    case NaiveDateTime.from_iso8601(binary) do
      {:ok, _} = ok -> ok
      {:error, _} -> :error
    end
  end
end