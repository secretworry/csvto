defmodule Csvto.Type do

  @base      ~w(integer float boolean string binary decimal datetime utc_datetime naive_datetime date time)a
  @composite ~w(array)

  def primitive?({composite, _}) when composite in @composite, do: true
  def primitive?(base) when base in @base, do: true
  def primitive?(_), do: false
end