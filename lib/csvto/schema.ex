defmodule Csvto.Schema do
  @moduledoc """
  Module defines the struct for a `Csvto.Schema`

  A `Csvto.Schema` generally is been defined through a `csv/1`, `csv/2` or `csv/3` directive defined in `Csvto.Builder`.
  It keeps the predefined meta required for decode a csv file into corresponding map or struct. Companing with the opts
  given by user, the `Csvto.Reader` can read and validate the given csv properly

  Its fields are:

  * `name` - The name of the schema, used to identify the schema
  * `fields` - The `Csvto.Field` defined for this schema
  * `index_mode` - the predefined mode for indexing fields for a specified csv file, which should be either :index or :name
  """

  @type index_mode :: :index | :name

  @type t :: %__MODULE__{
    name: atom,
    module: atom,
    fields: [Csvto.Field.t],
    index_mode: index_mode
  }

  @enforce_keys ~w{name module index_mode}a
  defstruct [:name, :module, :index_mode, fields: []]
end