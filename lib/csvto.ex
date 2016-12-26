defmodule Csvto do
  @moduledoc """
  Behaviour for converting csv to & from a file

  A Csvto is a collection of csv schemas, which is used to map data from csv file into Elixir maps

  By default we provide `Csvto.Builder` to help with building a Csvto, see more details in `Csvto.Builder`
  """

  @type from_result_t :: [Map.t] | {:error, any} | no_return

  @doc """
  Convert a csv file specified by `path` to a list of map according to given schema
  """
  @callback from(path :: String.t, schema :: atom) :: from_result_t
  @callback from(path :: String.t, schema :: atom, options :: Keyword.t) :: from_result_t
end
