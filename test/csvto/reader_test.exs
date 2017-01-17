defmodule Csvto.ReaderTest do

  use ExUnit.Case

  alias __MODULE__

  defmodule TestCsvto do
    use Csvto.Builder

    csv :by_name do
      field :key, :string, name: "Key"
      field :value, :string, name: "Value"
      field :optional, :string, name: "Optional", required: false, default: "nil"
    end

    csv :by_index do
      field :key, :string
      field :value, :string
      field :optional, :string, required: false, default: "nil"
    end

    csv :by_index_with_interval do
      field :key, :string
      field :value, :string, index: 3
      field :optional, :string, required: false, default: "nil"
    end

    csv :by_name_with_aggregators do
      field :key, :string, name: "Key"
      field :value, :string, name: "Value"
      fields :aggs0, :array, name: "Aggs0"
      fields :aggs1, :array, name: "Aggs1"
    end

    csv :by_index_with_aggregator do
      field :key, :string
      field :value, :string
      fields :extra, {:array, :string}
    end

    csv :keep_string do
      field :key, :string, name: "Key", keep: true
      field :value, :string, name: "Value", keep: true
      fields :agg, :array, name: "Agg", keep: true
    end
  end

  def fixture_path(name) do
    "test/fixtures/reader_test/#{name}"
  end

  test "should reject illegal header option" do
    assert_raise ArgumentError, "the specified header :not_exist cannot be found on schema :by_name", fn->
      Csvto.Reader.from(fixture_path("exact_headers.csv"), ReaderTest.TestCsvto, :by_name, headers: ~w[not_exist]a)
    end

    assert_raise ArgumentError, "cannot find field :not_exist on schema :by_name", fn->
      Csvto.Reader.from(fixture_path("exact_headers.csv"), ReaderTest.TestCsvto, :by_name, headers: %{"NotExist" => :not_exist})
    end
  end

  test "should set default value for empty non-required fields" do
      assert Csvto.Reader.from(fixture_path("with_empty_fields.csv"), ReaderTest.TestCsvto, :by_name)
          == [%{key: "key0", value: "value0", optional: "nil"},
              %{key: "key1", value: "value1", optional: "nil"}]
  end

  test "should return __line__ in the result if line_number: true is given" do
      assert Csvto.Reader.from(fixture_path("exact_headers.csv"), ReaderTest.TestCsvto, :by_name, line_number: true)
          == [%{key: "key0", value: "value0", optional: "optional0", __line__: 2},
              %{key: "key1", value: "value1", optional: "optional1", __line__: 3}]

      assert Csvto.Reader.from(fixture_path("exact_headers.csv"), ReaderTest.TestCsvto, :by_name, line_number: :line_number)
          == [%{key: "key0", value: "value0", optional: "optional0", line_number: 2},
              %{key: "key1", value: "value1", optional: "optional1", line_number: 3}]
  end


  describe "by_name" do
    test "should convert csv with exact headers" do
      assert Csvto.Reader.from(fixture_path("exact_headers.csv"), ReaderTest.TestCsvto, :by_name) 
          == [%{key: "key0", value: "value0", optional: "optional0"},
              %{key: "key1", value: "value1", optional: "optional1"}]
    end

    test "should convert csv with extra headers" do
      assert Csvto.Reader.from(fixture_path("extra_headers.csv"), ReaderTest.TestCsvto, :by_name) 
          == [%{key: "key0", value: "value0", optional: "optional0"},
              %{key: "key1", value: "value1", optional: "optional1"}]
    end

    test "should convert csv without header for optional fields" do
      assert Csvto.Reader.from(fixture_path("without_optional_headers.csv"), ReaderTest.TestCsvto, :by_name) 
          == [%{key: "key0", value: "value0", optional: "nil"},
              %{key: "key1", value: "value1", optional: "nil"}]
    end

    test "should convert csv with disordered headers" do
      assert Csvto.Reader.from(fixture_path("disordered_headers.csv"), ReaderTest.TestCsvto, :by_name) 
          == [%{key: "key0", value: "value0", optional: "optional0"},
              %{key: "key1", value: "value1", optional: "optional1"}]
    end

    test "should convert csv with altered header" do
      assert Csvto.Reader.from(fixture_path("with_altered_headers.csv"), ReaderTest.TestCsvto, :by_name, headers: %{"key" => :key, "value" => :value}) 
          == [%{key: "key0", value: "value0", optional: "nil"},
              %{key: "key1", value: "value1", optional: "nil"}]
    end

    test "should convert csv without headers after specified headers option" do
      assert Csvto.Reader.from(fixture_path("headerless.csv"), ReaderTest.TestCsvto, :by_name, headers: ~w[key value optional]a) 
          == [%{key: "key0", value: "value0", optional: "optional0"},
              %{key: "key1", value: "value1", optional: "optional1"}]
    end

    test "should convert csv without headers and optional fields after specified headers option" do
      assert Csvto.Reader.from(fixture_path("headerless.csv"), ReaderTest.TestCsvto, :by_name, headers: ~w[key value]a) 
          == [%{key: "key0", value: "value0", optional: "nil"},
              %{key: "key1", value: "value1", optional: "nil"}]
    end

    test "should convert csv with spaces without error" do
      assert Csvto.Reader.from(fixture_path("with_spaces.csv"), ReaderTest.TestCsvto, :by_name)
          == [%{key: "key0", value: "value0", optional: "optional0"},
              %{key: "key1", value: "value1", optional: "optional1"}]
    end

    test "should reject csv without required header" do
      assert_raise Csvto.Error, ~r/required fields key,value cannot be found in file .*/, fn ->
        Csvto.Reader.from!(fixture_path("without_required_fields.csv"), ReaderTest.TestCsvto, :by_name)
      end
    end
  end

  describe "by_index" do
    test "should convert csv with exact fields" do
      assert Csvto.Reader.from(fixture_path("headerless.csv"), ReaderTest.TestCsvto, :by_index) 
          == [%{key: "key0", value: "value0", optional: "optional0"},
              %{key: "key1", value: "value1", optional: "optional1"}]
    end

    test "should convert csv with headers after specified headers option" do
      assert Csvto.Reader.from(fixture_path("exact_headers.csv"), ReaderTest.TestCsvto, :by_index, headers: %{"Key" => :key, "Value" => :value, "Optional" => :optional}) 
          == [%{key: "key0", value: "value0", optional: "optional0"},
              %{key: "key1", value: "value1", optional: "optional1"}]
    end

    test "should convert csv with extra fields" do
      assert Csvto.Reader.from(fixture_path("headerless_with_extra_fields.csv"), ReaderTest.TestCsvto, :by_index) 
          == [%{key: "key0", value: "value0", optional: "optional0"},
              %{key: "key1", value: "value1", optional: "optional1"}]
    end

    test "should convert csv with headers but without optional fields" do
      assert Csvto.Reader.from(fixture_path("headerless_without_optional_headers.csv"), ReaderTest.TestCsvto, :by_index) 
          == [%{key: "key0", value: "value0", optional: "nil"},
              %{key: "key1", value: "value1", optional: "nil"}]
    end

    test "should convert csv with sparse fields after specified headers option" do
      assert Csvto.Reader.from(fixture_path("headerless_with_sparse_fields.csv"), ReaderTest.TestCsvto, :by_index, headers: [:key, nil, nil, :value, :optional]) 
          == [%{key: "key0", value: "value0", optional: "optional0"},
              %{key: "key1", value: "value1", optional: "optional1"}]
    end
  end

  describe "by_index_with_interval" do
    test "should convert csv with sparse fields" do
      assert Csvto.Reader.from(fixture_path("headerless_with_sparse_fields.csv"), ReaderTest.TestCsvto, :by_index_with_interval) 
          == [%{key: "key0", value: "value0", optional: "optional0"},
              %{key: "key1", value: "value1", optional: "optional1"}]
    end
  end

  describe "validator tests" do
    test "report illegal value with inline validator" do

      defmodule InlineValidator do
        use Csvto.Builder
        csv :schema do
          field :key, :string
          field :value, :integer, validator: &(&1 > 0)
        end
      end

      assert_raise Csvto.Error, ~r/illegal value \"0\" in file [^ ]* at line 2, column 2: validation error for 0/, fn ->
        Csvto.Reader.from!(fixture_path("integer_validator_test.csv"), ReaderTest.InlineValidator, :schema)
      end
    end

    test "report illegal value with method validator" do
      defmodule MethodValidator do
        use Csvto.Builder
        csv :schema do
          field :key, :string
          field :value, :integer, validator: :validate_integer
        end

        def validate_integer(value), do: value > 0
      end

      assert_raise Csvto.Error, ~r/illegal value \"0\" in file [^ ]* at line 2, column 2: validation error for 0/, fn ->
        Csvto.Reader.from!(fixture_path("integer_validator_test.csv"), ReaderTest.MethodValidator, :schema)
      end
    end

    test "report illegal value with method validator with args" do
      defmodule MethodWithArgsValidator do
        use Csvto.Builder
        csv :schema do
          field :key, :string
          field :value, :integer, validator: {:validate_integer, 0}
        end

        def validate_integer(value, min), do: value > min
      end

      assert_raise Csvto.Error, ~r/illegal value \"0\" in file [^ ]* at line 2, column 2: validation error for 0/, fn ->
        Csvto.Reader.from!(fixture_path("integer_validator_test.csv"), ReaderTest.MethodWithArgsValidator, :schema)
      end
    end
  end

  describe "by_name_with_aggregators" do
    test "should aggregate aggregate fields" do
      assert Csvto.Reader.from(fixture_path("with_aggregate_fields.csv"), ReaderTest.TestCsvto, :by_name_with_aggregators)
          == [%{aggs0: ["aggs000", "aggs010", "aggs020"],
                aggs1: ["aggs100", "aggs110", "aggs120"], key: "key0", value: "value0"},
              %{aggs0: ["aggs001", "aggs011", "aggs021"],
                aggs1: ["aggs101", "aggs111", "aggs121"], key: "key1", value: "value1"}]
    end
  end

  describe "by_index_with_aggregator" do
    test "should aggregate remaining fields" do
      assert Csvto.Reader.from(fixture_path("headerless_with_extra_fields.csv"), ReaderTest.TestCsvto, :by_index_with_aggregator)
          == [%{extra: ["optional0", "extra0"], key: "key0", value: "value0"},
              %{extra: ["optional1", "extra1"], key: "key1", value: "value1"}]
    end
  end

  describe "keep_string" do
    test "should keep original string" do
      assert Csvto.Reader.from(fixture_path("keep_spaces.csv"), ReaderTest.TestCsvto, :keep_string)
          == [%{agg: ["    agg00   ", "    agg10"], key: "    key0    ",
                value: "       value0   "},
              %{agg: ["    agg01   ", "    agg11"], key: "    key1    ",
                value: "       value1   "}]
    end
  end
end