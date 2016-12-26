defmodule Csvto.Reader do

  @type column_def :: nil | Csvto.Field.t

  @type context :: %{path: String.t, columns: [column_def], schema: Csvto.Schema.t, fields: %{String.t => Csvto.Field.t}, unspecified: [Csvto.Field.t], opts: Map.t}

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
    context = %{path: path, schema: schema, columns: nil, fields: nil, opts: Map.new(opts), unspecified: []}
    case Keyword.get(opts, :headers) do
      nil ->
        case schema.index_mode do
          :index ->
            %{context | columns: build_index_mode_column_defs(schema)}
          :name ->
            %{context | fields: Enum.reduce(schema.fields, Map.new, &(Map.put(&2, &1.field_name, &1)))}
        end
      headers when is_list(headers) ->
        build_context_from_list_headers!(headers, schema, context)
      headers when is_map(headers) ->
        build_context_from_map_headers!(headers, schema, context)
      headers ->
        raise ArgumentError, "headers should either be a [atom] or a %{String.t => atom}, but got #{inspect headers}"
    end
  end

  defp build_context_from_map_headers!(headers, schema, context) do
    fields_by_name = schema.fields |> Enum.reduce(Map.new, &(Map.put(&2, &1.name, &1)))
    {fields, unspecified} = Enum.reduce(headers, {Map.new, fields_by_name}, fn
      {field_name, name}, {map, fields_by_name} ->
        case Map.get(fields_by_name, name) do
          nil -> raise ArgumentError, "cannot find field #{inspect name} on schema #{inspect schema.name}"
          field ->
            fields_by_name = Map.drop(fields_by_name, [name])
            {Map.put(map, field_name, field), fields_by_name}
        end
    end)
    unspecified = Map.values(unspecified)
    case extract_name_of_required(unspecified) do
      [] ->
        %{context | fields: fields, unspecified: unspecified}
      required_fields ->
        required_fields_missing_error(required_fields)
    end
  end

  defp build_context_from_list_headers!(headers, schema, context) do
    fields = Enum.reduce(schema.fields, Map.new, &(Map.put(&2, &1.name, &1)))
    {columns, unspecified} = Enum.reduce(headers, {[], fields}, fn
      nil, {arr, fields} ->
        {[nil|arr], fields}
      header, {arr, fields} ->
        case Map.get_and_update(fields, header, fn _ -> :pop end) do
          {nil, _} -> raise ArgumentError, "specified header #{inspect header} cannot be found on schema #{inspect schema.name}"
          {field, fields} ->
            {[field|arr], fields}
        end
    end)
    unspecified = Map.values(unspecified)
    case extract_name_of_required(unspecified) do
      [] ->
        %{context | columns: columns |> Enum.reverse, unspecified: unspecified}
      required_fields ->
        required_fields_missing_error(required_fields)
    end
  end

  defp required_fields_missing_error(required_fields) do
    raise ArgumentError, "required fields #{Enum.join(required_fields, ",")} are not specified in the given header options"
  end

  defp build_index_mode_column_defs(schema) do
    {column_defs, _} = Enum.flat_map_reduce(schema.fields, -1, fn
      field, last_index ->
        if field.field_index - last_index <= 1 do
          {[field], field.field_index}
        else
          {List.duplicate(nil, field.field_index - last_index - 1) ++ [field], field.field_index}
        end
    end)
    column_defs
  end

  defp do_add_context!({row, 0}, %{columns: nil, fields: fields, unspecified: unspecified_in_opts} = context) do
    row = preprocess_row(row)
    {column_defs, missing} = Enum.map_reduce(row, fields, fn
      column_name, fields ->
        Map.get_and_update(fields, column_name, fn _ -> :pop end)
    end)
    unspecified = missing |> Map.values
    case extract_name_of_required(unspecified) do
      [] ->
        context = %{context | columns: column_defs, unspecified: unspecified ++ unspecified_in_opts}
        {[], context}
      required_fields ->
        raise_error("required fields #{Enum.join(required_fields, ",")} cannot be found in file #{context[:path]}")
    end
  end
  defp do_add_context!({row, index}, context), do: {[{row |> preprocess_row, index, context}], context}

  defp convert_row(stream) do
    stream
    |> Stream.map(&do_convert_row!/1)
  end

  def extract_name_of_required([]), do: []
  def extract_name_of_required(fields) do
    Enum.filter_map(fields, &(&1.required?), &(&1.name))
  end

  defp do_convert_row!({row, index, %{columns: columns, unspecified: unspecified} = context}) do
    column_count = Enum.count(row)
    {columns, unspecified_in_values} = if column_count < Enum.count(columns) do
      {specified, unspecified_in_values} = Enum.split(columns, column_count)
      case extract_name_of_required(unspecified_in_values) do
        [] ->
          {specified, unspecified_in_values}
        required_fields ->
          raise_error("required fields #{Enum.join(required_fields, ",")} is missing on file #{context[:path]}, line #{index + 1}")
      end
    else
      {columns, []}
    end
    result = Enum.zip(row, columns) |> Enum.with_index |> Enum.reduce(Map.new, fn
      {{_raw_value, nil}, _}, map -> map
      {{raw_value, field}, column_index}, map ->
        with {:ok, value} <- do_cast_value(field, raw_value, context[:opts]),
             {:ok, value} <- do_validate_value(context[:schema].module, field, value) do
          Map.put(map, field.name, value)
        else
          {:error, reason} ->
            raise_error("illegal value #{inspect raw_value} in file #{context[:path]} at line #{index + 1}, column #{column_index + 1}: #{reason}")
        end
    end)
    Enum.reduce(unspecified ++ unspecified_in_values, result, &(Map.put(&2, &1.name, &1.default)))
  end

  defp do_cast_value(field, raw_value, opts) do
    case Csvto.Type.cast(field.type, raw_value, opts) do
      :error ->
        {:error, "cast to #{inspect field.type} error"}
      {:ok, _} = ok -> ok
    end
  end

  defp do_validate_value(_module, %{validator: nil}, value), do: {:ok, value}

  defp do_validate_value(module, %{validator: method}, value) when is_atom(method) do
    apply(module, method, [value]) |> process_validate_result(value)
  end

  defp do_validate_value(module, %{validator: {method, opts}}, value) when is_atom(method) do
    apply(module, method, [value, opts]) |> process_validate_result(value)
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