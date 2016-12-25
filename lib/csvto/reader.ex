defmodule Csvto.Reader do

  def from(module, path, schema_name, opts) do
    with {:ok, schema} <- validate_schema_name!(module, schema_name),
         {:ok, stream} <- build_stream(path),
         {:ok, meta}   <- maybe_read_header(path, stream, schema, opts),
         {:ok, stream} <- build_each_item_stream(module, stream, schema, meta, opts),
     do: {:ok, stream}
  end

  defp validate_schema_name!(module, schema_name) do
    case module.__csvto__(:schema, schema_name) do
      nil -> {:error, "schema #{inspect schema_name} is undefined for #{inspect module}"}
      schema -> schema
    end
  end

  defp build_stream(path) do
    # TODO
  end

  defp maybe_read_header(path, stream, schema, opts) do
    # TODO
  end

  defp build_each_item_stream(module, stream, schema, meta, opts) do
  end
end