defmodule Csvto.Field do

  @type validator :: {atom, any} | atom | nil

  @type t :: %__MODULE__{
    name: String.t,
    required?: boolean,
    field_name: String.t | nil,
    field_index: integer | nil,
    validator: validator,
    default: term,
    opts: Map.t,
    line: integer,
    file: String.t
  }

  @enforce_keys ~w{name}a
  defstruct [:name, required?: true, field_name: nil, field_index: 0, validator: nil, default: nil, opts: %{}, line: 0, file: ""]

end