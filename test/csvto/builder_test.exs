defmodule Csvto.BuilderTest do

  use ExUnit.Case
  import Csvto.AssertionHelper

  defmodule Sample do
    use Csvto.Builder
    csv :simple do
      field :string, :string, name: "String"
      field :number, :integer, name: "Number"
    end

    csv :index_schema do
      field :n0, :string
      field :n10, :string, index: 10
      field :n11, :string
    end
  end

  test "should export __csvto__(:schema, schema_name)" do
    schema = Sample.__csvto__(:schema, :simple)
    assert_only ~w{index_mode module name}a,
                schema,
                %{ index_mode: :name, module: Csvto.BuilderTest.Sample, name: :simple}

    assert_only ~w{name required? field_name field_index validator default opts},
                schema.fields,
                [%{name: :string, required?: true, field_name: "String", field_index: nil, validator: nil, default: nil, opts: %{}},
                 %{name: :number, required?: true, field_name: "Number", field_index: nil, validator: nil, default: nil, opts: %{}}]
  end

  test "should export __csvto__(:schemas)" do
    schemas = Sample.__csvto__(:schemas)
    assert_only ~w{index_mode module name}a,
                schemas,
                [%{ index_mode: :name, module: Csvto.BuilderTest.Sample, name: :simple},
                 %{ index_mode: :index, module: Csvto.BuilderTest.Sample, name: :index_schema}]
  end

end