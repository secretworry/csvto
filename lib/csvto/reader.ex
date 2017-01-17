defmodule Csvto.Reader do

  @type column_def :: nil | Csvto.Field.t

  @type context :: %{path: String.t, columns: [column_def], column_count: integer, aggregate_column: column_def, schema: Csvto.Schema.t, fields: %{String.t => Csvto.Field.t}, aggregate_fields: %{String.t => Csvto.Field.t}, unspecified: [Csvto.Field.t], opts: Map.t}

  @csv_errors [CSV.Parser.SyntaxError, CSV.Lexer.EncodingError, CSV.Decoder.RowLengthError, CSV.LineAggregator.CorruptStreamError]

  @doc """
  Read from csv specified by path and convert to stream of map accoriding to given schema
  """
  def from(path, module, schema_name, opts \\ []) do
    try do
      do_from!(path, module, schema_name, opts) |> Enum.to_list
    rescue
      x in Csvto.Error ->
        {:error, x.message}
      x in @csv_errors ->
        {:error, x.message}
    end

  end

  def from!(path, module, schema_name, opts \\ []) do
    try do
      do_from!(path, module, schema_name, opts) |> Enum.to_list
    rescue
      x in @csv_errors ->
        stacktrace = System.stacktrace
        reraise Csvto.Error, [message: x.message], stacktrace
    end
  end

  defp do_from!(path, module, schema_name, opts) do
    schema = validate_schema_name!(module, schema_name)
    stream = build_stream!(path)
    stream
    |> CSV.decode
    |> add_index_and_context!(path, schema, opts)
    |> convert_row
  end

  defp raise_error(message) do
    raise Csvto.Error, message
  end

  defp validate_schema_name!(module, schema_name) do
    case module.__csvto__(:schema, schema_name) do
      nil -> raise_error("schema #{inspect schema_name} is undefined for #{inspect module}")
      schema -> schema
    end
  end

  defp build_stream!(path) do
    case File.open(path, [:read, :utf8]) do
      {:ok, file} ->
        IO.stream(file, :line)
      {:error, reason} ->
        raise_error("cannot open file #{path} for #{inspect reason}")
    end
  end

  defp add_index_and_context!(stream, path, schema, opts) do
    stream
    |> Stream.with_index()
    |> Stream.transform(init_context!(path, schema, opts), &do_add_context!/2)
  end

  defp init_context!(path, schema, opts) do
    context = %{path: path, schema: schema, columns: nil, column_count: 0, fields: nil, aggregate_column: nil, aggregate_fields: %{}, opts: Map.new(opts), unspecified: []}
    case Keyword.get(opts, :headers) do
      nil ->
        case schema.index_mode do
          :index ->
            context
            |> build_index_mode_context
          :name ->
            context
            |> build_name_mode_context
        end
      headers when is_list(headers) ->
        context
        |> build_context_from_list_headers!(headers)
      headers when is_map(headers) ->
        context
        |> build_context_from_map_headers!(headers)
      headers ->
        raise ArgumentError, "headers should either be an [atom] or a %{String.t => atom}, but got #{inspect headers}"
    end
  end

  defp build_index_mode_context(context) do
    schema = context[:schema]
    {column_defs, {_, aggregate_column}} = Enum.flat_map_reduce(schema.fields, {-1, nil}, fn
      %{field_type: :aggregate} = aggregate_field, {last_index, nil} ->
        if aggregate_field.field_index - last_index <= 1 do
          {[], {aggregate_field.field_index, aggregate_field}}
        else
          {List.duplicate(nil, aggregate_field.field_index - last_index - 1), {aggregate_field.field_index, aggregate_field}}
        end
      field, {last_index, nil} ->
        if field.field_index - last_index <= 1 do
          {[field], {field.field_index, nil}}
        else
          {List.duplicate(nil, field.field_index - last_index - 1) ++ [field], {field.field_index, nil}}
        end
    end)
    %{context | columns: column_defs, column_count: Enum.count(column_defs), aggregate_column: aggregate_column}
  end

  defp build_name_mode_context(context) do
    schema = context[:schema]
    {fields, aggregate_fields} = Enum.reduce(schema.fields, {Map.new, Map.new}, fn
      %{field_type: :aggregate} = field, {fields, aggregate_fields} ->
        {fields, Map.put(aggregate_fields, field.field_name, field)}
      field, {fields, aggregate_fields} ->
        {Map.put(fields, field.field_name, field), aggregate_fields}
    end)
    %{context | fields: fields, aggregate_fields: aggregate_fields}
  end

  defp build_context_from_map_headers!(context, headers) do
    schema = context[:schema]
    fields_and_usage_by_name = schema.fields |> Enum.reduce(Map.new, &(Map.put(&2, &1.name, {&1, false})))
    # Try to associate header with fields and checking for duplicate
    {name_and_fields, fields_and_usage_by_name} = Enum.map_reduce(headers, fields_and_usage_by_name, fn
      {field_name, name}, fields_and_usage_by_name ->
        Map.get_and_update(fields_and_usage_by_name, name, fn
          nil ->
            raise ArgumentError, "cannot find field #{inspect name} on schema #{inspect schema.name}"
          {%{field_type: :aggregate} = field, true} ->
            {{field_name, field}, {field, true}}
          {_field, true} ->
            raise ArgumentError, "field #{inspect name} has been mapped more than once in given headers #{inspect headers}"
          {field, false} ->
            {{field_name, field}, {field, true}}
        end)
    end)
    # Try to assign default field_names to remaining fields
    {fields_and_usage_by_name, name_and_fields} = Enum.map_reduce(fields_and_usage_by_name, name_and_fields, fn
      {name, {field, false}}, name_and_fields ->
        if field.field_name do
          {{name, {field, true}}, [{field.field_name, field} | name_and_fields]}
        else
          {{name, {field, false}}, name_and_fields}
        end
      field_and_usage, name_and_fields ->
        {field_and_usage, name_and_fields}
    end)
    # Extract out unspecified fields and try to raise errors
    unspecified = Enum.reduce(fields_and_usage_by_name, [], fn
      {_, {field, false}}, acc ->
        [field | acc]
      _, acc ->
        acc
    end)
    case extract_name_of_required(unspecified) do
      [] ->
        :ok
      required_fields ->
        required_fields_missing_error(required_fields)
    end
    {fields, aggregate_fields} = Enum.partition(name_and_fields, fn
      {_, %{field_type: :aggregate}} -> false
      _ -> true
    end)

    %{context | fields: fields |> Enum.into(%{}), aggregate_fields: aggregate_fields |> Enum.into(%{}), unspecified: unspecified}
  end

  defp build_context_from_list_headers!(context, headers) do
    schema = context[:schema]
    fields_and_usage_by_name = Enum.reduce(schema.fields, Map.new, &(Map.put(&2, &1.name, {&1, false})))
    {columns, fields_and_usage_by_name} = Enum.map_reduce(headers, fields_and_usage_by_name, fn
      nil, fields_and_usage_by_name ->
        {nil, fields_and_usage_by_name}
      header, fields_and_usage_by_name ->
        Map.get_and_update(fields_and_usage_by_name, header, fn
          nil -> raise ArgumentError, "the specified header #{inspect header} cannot be found on schema #{inspect schema.name}"
          {%{field_type: :aggregate} = field, true} ->
            {field, {field, true}}
          {_field, true} ->
            raise ArgumentError, "non-aggregate field #{inspect header} has been defined more than once in the specified headers #{inspect headers}"
          {field, false} ->
            {field, {field, true}}
        end)
    end)
    unspecified = fields_and_usage_by_name |> filter_out_unused
    case extract_name_of_required(unspecified) do
      [] ->
        %{context | columns: columns, column_count: Enum.count(columns), unspecified: unspecified}
      required_fields ->
        required_fields_missing_error(required_fields)
    end
  end

  defp required_fields_missing_error(required_fields) do
    raise ArgumentError, "required fields #{Enum.join(required_fields, ",")} are not specified in the given header options"
  end

  defp do_add_context!({row, 0}, %{columns: nil, fields: fields, aggregate_fields: aggregate_fields, unspecified: unspecified_in_opts} = context) do
    row = preprocess_row(row)
    fields_and_usage = Enum.reduce(fields, Map.new, fn
      {field_name, field}, map ->
        Map.put(map, field_name, {field, false})
    end)
    {column_defs, fields_and_usage} = Enum.map_reduce(row, fields_and_usage, fn
      column_name, fields_and_usage ->
        Map.get_and_update(fields_and_usage, column_name, fn
          nil ->
            case find_by_prefix(aggregate_fields, column_name) do
              nil ->
                :pop
              field ->
                {field, {field, true}}
            end
          {%{field_type: :aggregate} = field, true} ->
            {field, {field, true}}
          {_field, true} ->
            raise_error("duplicate non aggregate field #{column_name} found in file #{context[:path]}")
          {field, false} ->
            {field, {field, true}}
        end)
    end)
    unspecified = fields_and_usage |> filter_out_unused
    case extract_name_of_required(unspecified) do
      [] ->
        context = %{context | columns: column_defs, column_count: Enum.count(column_defs), unspecified: unspecified ++ unspecified_in_opts}
        {[], context}
      required_fields ->
        raise_error("required fields #{Enum.join(required_fields, ",")} cannot be found in file #{context[:path]}")
    end
  end
  defp do_add_context!({row, index}, context), do: {[{row, index, context}], context}

  defp convert_row(stream) do
    stream
    |> Stream.map(&do_convert_row!/1)
  end

  def extract_name_of_required([]), do: []
  def extract_name_of_required(fields) do
    Enum.filter_map(fields, &(&1.required?), &(&1.name))
  end

  defp filter_out_unused(fields_and_usage) do
    Enum.flat_map(fields_and_usage, fn
      {_, {field, false}}->
        [field]
      _->
        []
    end)
  end

  defp do_convert_row!({row, index, %{columns: columns, column_count: column_count, aggregate_column: aggregate_column, unspecified: unspecified} = context}) do
    value_count = Enum.count(row)
    {value_and_fields, unspecified_fields, extra_values} = cond do
      value_count <= column_count ->
        {specified, unspecified_fields} = Enum.split(columns, value_count)
        {Enum.zip(row, specified), unspecified_fields, []}
      true ->
        {matched_values, unmatched_values} = Enum.split(row, column_count)
        {Enum.zip(matched_values, columns), [], unmatched_values}
    end
    case extract_name_of_required(unspecified_fields) do
      [] ->
        :ok
      required_fields ->
        raise_error("required fields #{Enum.join(required_fields, ",")} is missing on file #{context[:path]}, line #{index + 1}")
    end
    result = value_and_fields |> Enum.with_index |> Enum.reduce(init_result(row, index, context), fn
      {{_raw_value, nil}, _}, map ->
        map
      {{raw_value, field}, column_index}, map ->
        with {:ok, value} <- do_cast_value(context, field, raw_value),
             {:ok, value} <- do_validate_value(context[:schema].module, field, value) do
          update_map_value(map, field, value)
        else
          {:error, reason} ->
            raise_illegal_value_error(context, raw_value, index, column_index, reason)
        end
    end)
    result = Enum.reduce(unspecified ++ unspecified_fields, result, &(Map.put(&2, &1.name, &1.default)))
    if extra_values != [] && aggregate_column do
      Map.put(result, aggregate_column.name, cast_aggregate_value!(context, index, aggregate_column, extra_values))
    else
      result
    end
  end

  defp init_result(_row, index, context) do
    if key = Map.get(context.opts, :line_number, false) do
      key = case key do
        true -> :__line__
        key -> key
      end
      %{key => index + 1}
    else
      Map.new
    end
  end

  defp update_map_value(map, %{field_type: :aggregate} = field, value) do
    Map.update(map, field.name, [value], &(&1 ++ [value]))
  end

  defp update_map_value(map, field, value) do
    Map.put(map, field.name, value)
  end

  defp do_cast_value(context, %{field_type: :aggregate} = field, raw_value) do
    case field.type do
      :array ->
        {:ok, raw_value}
      {:array, subtype} ->
        do_cast_value(subtype, raw_value, default_value(context, subtype, nil), field.opts)
    end
  end

  defp do_cast_value(context, field, raw_value) do
    do_cast_value(field.type, raw_value, default_value(context, field.type, field.default), field.opts)
  end

  defp do_cast_value(type, raw_value, default, opts) do
    raw_value = maybe_trim(raw_value, type, Map.get(opts, :keep, false))
    if raw_value == "" do
      {:ok, default}
    else
      case Csvto.Type.cast(type, raw_value, opts) do
        :error ->
          {:error, "cast to #{inspect type} error"}
        {:ok, _} = ok -> ok
      end
    end
  end

  defp default_value(context, type, nil) do
    case Map.get(context.opts, :nilable, false) do
      true ->
        nil
      false ->
        Csvto.Type.default(type)
    end
  end

  defp default_value(_context, _type, default), do: default

  @keepable_types ~w{binary string}a

  defp maybe_trim(raw_value, keepable_type, true) when keepable_type in @keepable_types, do: raw_value
  defp maybe_trim(raw_value, _type, _keep), do: String.trim(raw_value)

  defp cast_aggregate_value!(context, index, aggregate_field, values) do
    values |> Enum.with_index(index) |> Enum.map(fn
      {raw_value, column_index} ->
        case do_cast_value(context, aggregate_field, raw_value) do
          {:ok, value} ->
            value
          {:error, reason} ->
            raise_illegal_value_error(context, raw_value, index, column_index, reason)
        end
    end)
  end

  defp find_by_prefix(map, name) do
    Enum.find_value(map, fn
      {prefix, value} ->
        if String.starts_with?(name, prefix) do
          value
        else
          nil
        end
    end)
  end

  defp do_validate_value(_module, %{validator: nil}, value), do: {:ok, value}

  defp do_validate_value(module, %{validator: method}, value) when is_atom(method) do
    apply(module, method, [value]) |> process_validate_result(value)
  end

  defp do_validate_value(module, %{validator: {method, opts}}, value) when is_atom(method) do
    apply(module, method, [value, opts]) |> process_validate_result(value)
  end

  defp raise_illegal_value_error(context, raw_value, index, column_index, reason) do
    raise_error("illegal value #{inspect raw_value} in file #{context[:path]} at line #{index + 1}, column #{column_index + 1}: #{reason}")
  end

  defp preprocess_row(row), do: row |> Enum.map(&(String.trim(&1)))

  defp process_validate_result({:ok, value}, _), do: {:ok, value}
  defp process_validate_result(:ok, value), do: {:ok, value}
  defp process_validate_result(true, value), do: {:ok, value}
  defp process_validate_result({:error, reason}, _value), do: {:error, reason}
  defp process_validate_result(false, value), do: {:error, "validation error for #{inspect value}"}
  defp process_validate_result(nil, value), do: {:error, "validation error for #{inspect value}"}
  defp process_validate_result(_truely, value), do: {:ok, value}
end