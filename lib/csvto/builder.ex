defmodule Csvto.Builder do
  @moduledoc """
  Conveniences for building a Csvto

  This module can be `use`-d into a Module to build a Csvto

  ## Example
  ```
  defmodule MyCsvto do
    use Csvto.Builder

    csv :product do
      field :name, :string, name: "Name"
      field :number, :string, name: "Number"
      field :description, :string, name: "Desc"
      field :price, :float, name: "Price", validate: &(&1 >= 0)
      field :images, {:array, :string}, name: "Images", separator: "|"
    end
  end
  ```

  ## Types and casting

  When defining a schema, types of fields need to be given. The data comming from a csv file will be validated and casted
  according to specified type.
  Types are split into two categories, base type and compositional type

  ### Base types

  The base types are

  Type                      | Value example
  :-------------------------|:------------------------------------
  `:integer`                | 1, 2, 3
  `:float`                  | 1.0, 2.0, 3.0
  `:boolean`                | yes, no, no, off, 1, 0, true, false
  `:string`                 | string
  `:binary`                 | binary
  `:decimal`                | 1.0, 2.0, 3.0
  `:naive_datetime`         | `ISO8601` datetime
  `:datetime`               | `ISO8601` with timezone
  `date`                    | `ISO8601` date
  `time`                    | `ISO8601` time

  ### Compositional type

  There is only one compositional type: `{:array, subtype}`
  For the subtype, you can replace it with any valid simple types, such as `:string`

  While parsing array from csv field, a `:separator` option could be specified to define how should the subtype
  should be seperated, by default, it is `"|"`

  ## Reflection

  Any Csvto defined with `use #{__MODULE__}` will generate the `__csvto__` function that can be
  used for runtime introspection of the shcemas:

  * `__csvto__(:schemas)` - Lists all the schemas defined in this module
  * `__csvto__(:schema, name)` - Returns the `Csvto.Schema` identified by the given name on this module
  """

  @type t :: struct

  defmacro __using__(_opts) do
    Module.register_attribute(__CALLER__.module, :csvto_schemas, accumulate: true)
    quote do
      import Csvto.Builder, only: [csv: 2]
      @before_compile unquote(__MODULE__)
    end
  end

  defmacro __before_compile__(env) do
    schemas = Module.get_attribute(env.module, :csvto_schemas) |> Enum.reverse
    schema_reflections = Enum.map(schemas, &quote_define_schema_reflection/1)
    schema_froms = Enum.map(schemas, &quote_define_from/1)
    quote do
      def __csvto__(:schemas), do: unquote(schemas |> Macro.escape)
      unquote(schema_reflections)
      def __csvto__(:schema, schema) do
        raise ArgumentError, "undefined schema #{schema} for #{inspect __MODULE__}"
      end
      def from(path, schema, opts \\ [])
      unquote(schema_froms)
      def from(path, schema, opts) do
        raise ArgumentError, "undefined schema #{schema} for #{inspect __MODULE__}"
      end
    end
  end

  defp quote_define_schema_reflection(schema) do
    name = schema.name
    quote do
      def __csvto__(:schema, unquote(name)), do: unquote(schema |> Macro.escape)
    end
  end

  defp quote_define_from(schema) do
    name = schema.name
    quote do
      def from(path, unquote(name), opts), do: unquote(:"__from_#{name}__")(path, opts)
    end
  end

  defmacro csv(name, [do: block]) do
    module = __CALLER__.module
    Module.register_attribute(module, :csvto_fields, accumulate: false)
    Module.register_attribute(module, :csvto_field_index, accumulate: false)
    Module.register_attribute(module, :csvto_index_mode, accumulate: false)
    Module.register_attribute(module, :csvto_schema, accumulate: false)
    Module.put_attribute(module, :csvto_fields, [])
    Module.put_attribute(module, :csvto_field_index, -1)
    Module.put_attribute(module, :csvto_index_mode, nil)
    Module.put_attribute(module, :csvto_schema, name)
    {_, validators} = escape(module, __CALLER__.file, block)
    csvto_fields = Module.get_attribute(module, :csvto_fields) |> Enum.reverse
    index_mode = Module.get_attribute(module, :csvto_index_mode)
    schema = Csvto.Builder.build_schema(module, name, index_mode, csvto_fields)
    Module.put_attribute(module, :csvto_schemas, schema)
    [Csvto.Builder.__from__(name)] ++
    validators
  end

  defp escape(module, file, block) do
    Macro.prewalk(block, [], fn
      {:field, env, args} = node, acc ->
        {node, [apply(__MODULE__, :__define_field__, [module, file, Keyword.get(env, :line), :single | args]) | acc]}
      {:fields, env, args} = node, acc ->
        {node, [apply(__MODULE__, :__define_field__, [module, file, Keyword.get(env, :line), :aggregate | args]) | acc]}
      node, acc ->
        {node, acc}
    end)
  end

  def build_schema(module, name, _index_mode, []), do: raise ArgumentError, "no field are defined for schema #{name} in #{inspect module}"

  def build_schema(module, name, index_mode, fields) do
    index_mode = case index_mode do
      :name -> :name
      {:index, _} -> :index
    end
    %Csvto.Schema{module: module, name: name, index_mode: index_mode, fields: fields}
  end

  def __define_field__(module, file, line, field_type, name, type, opts \\ []) do
    meta = %{
      module: module,
      field_type: field_type,
      field_index: Module.get_attribute(module, :csvto_field_index) + 1,
      index_mode: Module.get_attribute(module, :csvto_index_mode),
      schema: Module.get_attribute(module, :csvto_schema),
      fields: Module.get_attribute(module, :csvto_fields),
      file: file,
      line: line
    }
    check_type!(meta, name, type)
    meta = check_index_mode!(meta, name, opts)
    check_duplicate_declaration!(meta, name)
    check_aggregate_field!(meta, name, type, opts)
    {validator, code} = convert_validator(meta, name, opts)
    field = build_field(field_type, name, type, meta[:index_mode], validator, opts)
    Module.put_attribute(module, :csvto_fields, [field|meta[:fields]])
    Module.put_attribute(module, :csvto_field_index, meta[:field_index])
    Module.put_attribute(module, :csvto_index_mode, meta[:index_mode])
    code
  end

  defp build_field(field_type, name, type, index_mode, validator, opts) do
    default = default_for_type(type, opts)
    field_opts = opts |> Enum.into(%{}) |> Map.drop(~w{required name validator}a)
    field_index = case index_mode do
      :name -> nil
      {:index, index} -> index
    end
    %Csvto.Field{
      name: name,
      type: type,
      field_type: field_type,
      required?: Keyword.get(opts, :required, true),
      field_name: Keyword.get(opts, :name),
      field_index: field_index,
      validator: validator,
      default: default,
      opts: field_opts
    }
  end

  defp check_duplicate_declaration!(meta, name) do
    case Enum.find(meta[:fields], &(&1.name == name)) do
      nil ->
        :ok
      field ->
        raise ArgumentError, "duplicate field declaration for field #{inspect name} on #{meta[:line]} which has been defined on #{field.line}"
    end
  end

  defp convert_validator(meta, name, opts) do
    case Keyword.get(opts, :validator) do
      nil ->
        {nil, nil}
      {:&, _, _} = validator ->
        do_define_validator_fun_1(meta[:schema], name, validator)
      validator when is_atom(validator) ->
        {validator, nil}
      {validator, opts} when is_atom(validator) ->
        {{validator, opts}, nil}
      validator ->
        raise ArgumentError, "illegal validator for field #{name} defined on line #{meta[:line]}, validator should be anonymous function with 1 capture, atom or {atom, any} but got #{inspect validator}"
    end
  end

  defp do_define_validator_fun_1(schema, name, validator) do
    validator_name = "__csvto_validate_#{schema}_#{name}__" |> String.to_atom
    {validator_name, quote do
      def unquote(validator_name)(value), do: (unquote(validator)).(value)
    end}
  end

  defp check_index_mode!(%{index_mode: nil} = meta, _name, opts) do
    index_mode = case Keyword.get(opts, :name) do
      nil ->
        {:index, 0}
      _ ->
        :name
    end
    %{meta | index_mode: index_mode}
  end

  defp check_index_mode!(%{index_mode: {:index, index}} = meta, field_name, opts) do
    index_mode = case Keyword.get(opts, :name) do
      nil ->
        index = Keyword.get(opts, :index, index + 1)
        {:index, index}
      _name ->
        raise ArgumentError, "cannot define name option for field #{inspect field_name} defined on #{meta.line}, either all fields or none of them should declare name option"
    end
    %{meta | index_mode: index_mode}
  end

  defp check_index_mode!(%{index_mode: :name} = meta, field_name, opts) do
    index_mode = case Keyword.get(opts, :name) do
      nil ->
        raise ArgumentError, "forget to define name option for field #{inspect field_name} defined on #{meta.line}, either all fields or none of them should declare name option"
      _name ->
        :name
    end
    %{meta | index_mode: index_mode}
  end

  defp check_type!(meta, field_name, type) do
    if Csvto.Type.primitive?(type) do
      case meta[:field_type] do
        :single ->
          type
        :aggregate ->
          cond do
            Csvto.Type.array?(type) ->
              type
            true ->
              raise ArgumentError, "invalid type #{inspect type} for aggregate field defined on line #{meta[:line]}, expect {:array, type} but got #{inspect type}"
          end
      end
    else
      raise ArgumentError, "invalid type #{inspect type} for field #{inspect field_name} defined on line #{meta[:line]}"
    end
  end

  defp check_aggregate_field!(%{field_type: :aggregate} = meta, field_name, _type, opts) do
    case meta[:index_mode] do
      {:index, _} ->
        if _another_aggrate_field = Enum.find(meta[:fields], &(&1.field_type == :aggregate)) do
          raise ArgumentError, "more than one aggregate field in #{inspect meta[:schema]}: only one aggrate field can be defined in the index mode"
        end
        :ok
      :name ->
        name = case Keyword.fetch(opts, :name) do
          {:ok, name} ->
            name
          :error ->
            raise ArgumentError, "name option is required for the aggregate field #{inspect field_name}"
        end
        if name_conflict_field = Enum.find(meta[:fields], &has_conflict_name?(name, &1)) do
          raise ArgumentError, "the name option of field #{inspect field_name} conflicts with the field #{inspect name_conflict_field.name}: #{name} and #{name_conflict_field.field_name} overlap each other"
        end
        :ok
    end
  end

  defp check_aggregate_field!(meta, _field_name, _type, _opts) do
    case meta[:index_mode] do
      {:index, _} ->
        if preceding_aggregate_field = Enum.find(meta[:fields], &(&1.field_type == :aggregate)) do
          raise ArgumentError, "#{inspect preceding_aggregate_field.name} should be the last field: aggregate field can only be the last field in index mode"
        end
        :ok
      _ ->
        :ok
    end
  end

  defp has_conflict_name?(name, %{field_type: :aggregate, field_name: another_name}) do
    String.starts_with?(name, another_name) || String.starts_with?(another_name, name)
  end

  defp has_conflict_name?(_name, _field), do: false

  defp default_for_type(_, opts) do
    Keyword.get(opts, :default)
  end

  def __schema__(schema_name, schema) do
    quote do
      def __csvto__(:schema, unquote(schema_name)) do
        unquote(schema |> Macro.escape)
      end

    end
  end

  def __from__(schema_name) do
    quote do
      def unquote(:"__from_#{schema_name}__")(path, opts) do
        Csvto.Reader.from(path, __MODULE__, unquote(schema_name), opts)
      end
    end
  end
end
