defmodule Csvto do
  @moduledoc """
  Behaviour for converting csv to & from a file
  """

  @callback from(path :: String.t, schema :: atom, options :: Keyword.t) :: Stream.t | {:error, any}
  @callback into(source :: Enumerable.t, path :: String.t, schema :: atom, options :: Keyword.t) :: :ok | {:error, any}
end
