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

  test "raise error for illegal field type" do
    assert_raise ArgumentError, ~r/invalid type :illegal_type for field :illegal_field defined on line \d+/, fn->
      defmodule IllegalType do
        use Csvto.Builder
        csv :illegal_type do
          field :illegal_field, :illegal_type
        end
      end
    end
  end

  test "raise error for mix declaring of index mode" do
    assert_raise ArgumentError, ~r/cannot define name option for field :name_mode defined on \d+, either all fields or none of them should declare name option/, fn->
      defmodule MixIndexMode do
        use Csvto.Builder
        csv :mix_mode do
          field :index_mode, :string
          field :name_mode, :string, name: "Name"
        end
      end
    end
  end

  test "raise error for declaring duplicate field" do
    assert_raise ArgumentError, ~r/duplicate field declaration for field :field on \d+ which has been defined on \d+/, fn->
      defmodule DuplicateField do
        use Csvto.Builder
        csv :duplicate_field do
          field :field, :string
          field :field, :string
        end
      end
    end
  end

  test "should define index in the specified sequence" do
    defmodule IndexMode do
      use Csvto.Builder
      csv :index_mode do
        field :n0, :string
        field :n1, :string
        field :n10, :string, index: 10
        field :n11, :string
      end
    end

    schema = IndexMode.__csvto__(:schema, :index_mode)

    assert_only ~w{index_mode},
                schema,
                %{index_mode: :index}
    assert_only ~w{field_index field_name},
                schema.fields,
                [%{field_index: 0, field_name: nil}, %{field_index: 1, field_name: nil}, %{field_index: 10, field_name: nil}, %{field_index: 11, field_name: nil}]
  end

  test "should define field names as specified" do
    defmodule NameMode do
      use Csvto.Builder
      csv :name_mode do
        field :name0, :string, name: "Name0"
        field :name1, :string, name: "Name1"
      end
    end

    schema = NameMode.__csvto__(:schema, :name_mode)

    assert_only ~w{index_mode},
                schema,
                %{index_mode: :name}
    assert_only ~w{field_index field_name},
                schema.fields,
                [%{field_index: nil, field_name: "Name0"}, %{field_index: nil, field_name: "Name1"}]
  end

  test "should define aggregate fields as field_type: :aggregate" do
    defmodule AggregateFieldsDefinition do
      use Csvto.Builder
      csv :name_mode do
        fields :name0, :array, name: "Name0"
        fields :name1, :array, name: "Name1"
      end
      csv :index_mode do
        fields :name, :array
      end
    end

    name_mode_schema = AggregateFieldsDefinition.__csvto__(:schema, :name_mode)

    assert_only ~w{field_type field_name},
                name_mode_schema.fields,
                [%{field_type: :aggregate, field_name: "Name0"}, %{field_type: :aggregate, field_name: "Name1"}]

    index_mode_schema = AggregateFieldsDefinition.__csvto__(:schema, :index_mode)

    assert_only ~w{field_type field_index},
                index_mode_schema.fields,
                [%{field_type: :aggregate, field_index: 0}]
  end

  test "should define field with validator" do
    defmodule Validator do
      use Csvto.Builder
      csv :with_validator do
        field :inline_validator, :integer, validator: &(&1 >= 0)
        field :validator_without_opts, :integer, validator: :validate_integer
        field :validator_with_opts, :integer, validator: {:validate_integer, 5}
      end

      def validate_integer(value), do: value >= 0

      def validate_integer(value, opts), do: value >= opts
    end
    schema = Validator.__csvto__(:schema, :with_validator)
    assert_only ~w{validator},
                schema.fields,
                [%{validator: :__csvto_validate_with_validator_inline_validator__},
                 %{validator: :validate_integer},
                 %{validator: {:validate_integer, 5}}]
  end

  test "should raise error for fields define as type other than {:array, type}" do
    assert_raise ArgumentError, ~r/invalid type :string for aggregate field defined on line \d+, expect {:array, type} but got :string/, fn ->
      defmodule IllegalFieldsType do
        use Csvto.Builder
        csv :illegal_fields_type do
          fields :fields, :string, name: "Category"
        end
      end
    end
  end

  test "should raise error for defining more than one aggregate field in index mode" do
    assert_raise ArgumentError, "more than one aggregate field in :too_many_aggregate_fields: only one aggrate field can be defined in the index mode", fn ->
      defmodule TooManyAggregateFields do
        use Csvto.Builder
        csv :too_many_aggregate_fields do
          fields :first_fields, {:array, :string}
          fields :second_fields, {:array, :integer}
        end
      end
    end
  end

  test "should raise error for defining two aggregate fields with their names overlap each other" do
    assert_raise ArgumentError, "the name option of field :second_fields conflicts with the field :first_fields: test_2 and test overlap each other", fn ->
      defmodule NamesOverlapAggregateFields do
        use Csvto.Builder
        csv :names_verlap_aggregate_fields do
          fields :first_fields, {:array, :string}, name: "test"
          fields :second_fields, {:array, :integer}, name: "test_2"
        end
      end
    end
  end

  test "should raise error for defining aggregate field as a non-last field in the index mode" do
    assert_raise ArgumentError, ":fields should be the last field: aggregate field can only be the last field in index mode", fn ->
      defmodule MiddleAggregateField do
        use Csvto.Builder
        csv :middle_aggregate_field do
          field :preceding, :integer
          fields :fields, {:array, :integer}
          field :following, :string
        end
      end
    end
  end
end