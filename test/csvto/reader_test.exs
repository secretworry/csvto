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
  end

  def fixture_path(name) do
    "test/fixtures/reader_test/#{name}"
  end

  test "should reject illegal header option" do
    assert_raise ArgumentError, "specified header :not_exist cannot be found on schema :by_name", fn->
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
end