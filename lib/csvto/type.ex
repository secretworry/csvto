defmodule Csvto.Type do

  @base      ~w(integer float boolean string binary decimal naive_datetime datetime date time)a
  @composite ~w(array)a

  def base_types(), do: @base

  def primitive?({composite, _}) when composite in @composite, do: true
  def primitive?(base) when base in @base, do: true
  def primitive?(composite) when composite in @composite, do: true
  def primitive?(_), do: false

  def array?(:array), do: true
  def array?({:array, _}), do: true
  def array?(_), do: false

  def default(:integer), do: 0
  def default(:float), do: 0.0
  def default(:boolean), do: false
  def default(:string), do: ""
  def default(:binary), do: ""
  def default(:decimal), do: Decimal.new(0)
  def default(:array), do: []
  def default({:array, _}), do: []
  def default(_), do: nil

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
  def cast(:naive_datetime, value, opts) do
    cast_naive_datetime(value, opts)
  end
  def cast(:datetime, value, opts) do
    case cast_naive_datetime(value, opts) do
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

  def cast(:date, value, opts) do
    case Map.fetch(opts, :format) do
      {:ok, format} ->
        case do_parse_datetime(value, format) do
          {:ok, naive_datetime} ->
            {:ok, naive_datetime |> NaiveDateTime.to_date}
          {:error, _} ->
            :error
        end
      :error ->
        case Date.from_iso8601(value) do
          {:ok, _} = ok -> ok
          {:error, _} -> :error
        end
    end
  end

  def cast(:time, value, opts) do
    case Map.fetch(opts, :format) do
      {:ok, format} ->
        case do_parse_datetime(value, format) do
          {:ok, naive_datetime} ->
            {:ok, naive_datetime |> NaiveDateTime.to_time}
          {:error, _} ->
            :error
        end
      :error ->
        case Time.from_iso8601(value) do
          {:ok, _} = ok -> ok
          {:error, _} -> :error
        end
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

  defp cast_naive_datetime(binary, opts) when is_binary(binary) do
    case Map.fetch(opts, :format) do
      {:ok, format} ->
        case do_parse_datetime(binary, format) do
          {:ok, _} = ok ->
            ok
          {:error, _} ->
            :error
        end
      :error ->
        case NaiveDateTime.from_iso8601(binary) do
          {:ok, _} = ok -> ok
          {:error, _} -> :error
        end
    end
  end

  defp do_parse_datetime(binary, {parser, format}) do
    Timex.parse(binary, format, parser)
  end
  defp do_parse_datetime(binary, format) when is_binary(format) do
    Timex.parse(binary, format)
  end
end