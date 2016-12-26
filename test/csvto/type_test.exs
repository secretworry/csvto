defmodule Csvto.TypeTest do
  use ExUnit.Case

  import Csvto.Type

  describe "cast/2" do

    test "should cast value" do
      assert cast(:integer, "5", %{}) == {:ok, 5}
      assert cast(:integer, "5.5", %{}) == :error
      assert cast(:integer, "other", %{}) == :error
      assert cast(:float, "5", %{}) == {:ok, 5.0}
      assert cast(:float, "5.5", %{}) == {:ok, 5.5}
      assert cast(:float, "other", %{}) == :error

      assert cast(:boolean, "true", %{}) == {:ok, true}
      assert cast(:boolean, "True", %{}) == {:ok, true}
      assert cast(:boolean, "TRUE", %{}) == {:ok, true}
      assert cast(:boolean, "1", %{}) == {:ok, true}
      assert cast(:boolean, "on", %{}) == {:ok, true}
      assert cast(:boolean, "ON", %{}) == {:ok, true}
      assert cast(:boolean, "yes", %{}) == {:ok, true}
      assert cast(:boolean, "Yes", %{}) == {:ok, true}
      assert cast(:boolean, "YES", %{}) == {:ok, true}

      assert cast(:boolean, "false", %{}) == {:ok, false}
      assert cast(:boolean, "False", %{}) == {:ok, false}
      assert cast(:boolean, "FALSE", %{}) == {:ok, false}
      assert cast(:boolean, "0", %{}) == {:ok, false}
      assert cast(:boolean, "off", %{}) == {:ok, false}
      assert cast(:boolean, "OFF", %{}) == {:ok, false}
      assert cast(:boolean, "no", %{}) == {:ok, false}
      assert cast(:boolean, "No", %{}) == {:ok, false}
      assert cast(:boolean, "NO", %{}) == {:ok, false}
      assert cast(:boolean, "other", %{}) == :error

      assert cast(:string, "string", %{}) == {:ok, "string"}
      assert cast(:binary, "string", %{}) == {:ok, "string"}

      assert cast(:decimal, "3.5", %{}) == {:ok, Decimal.new(3.5)}
      assert cast(:decimal, "other", %{}) == :error

      assert cast(:naive_datetime, "2016-12-26 09:14:26", %{}) == {:ok, ~N[2016-12-26 09:14:26]}
      assert cast(:naive_datetime, "other", %{}) == :error

      assert cast(:date, "2016-12-26", %{}) == {:ok, ~D[2016-12-26]}
      assert cast(:date, "other", %{}) == :error

      assert cast(:time, "09:22:40", %{}) == {:ok, ~T[09:22:40]}
      assert cast(:time, "other", %{}) == :error

      assert cast({:array, :integer}, "1|2|3|4", %{}) == {:ok, [1, 2, 3, 4]}
      assert cast({:array, :integer}, "1#2#3#4", %{separator: "#"}) == {:ok, [1, 2, 3, 4]}
      assert cast({:array, :integer}, "other", %{}) == :error

      assert cast(:array, "ab|cd|ef", %{}) == {:ok, ~w[ab cd ef]}
    end
  end
end