# Csvto

Convert csv file to elixir map with ease

# Blueprint

```elixir
  defmodule MyCsvto do
    use Csvto.Builder

    csv "product" do
      field :name, :string, name: "Name"
      field :number, :string, name: "Number"
      field :description, :string, name: "Desc"
      field :price, :float, name: "Price"
      field :images, {:array, :string}, name: "Images", separator: "|"
    end
  end

  MyCsvto.from(path) # returns [Map.t]
  MyCsvto.from(path, into: MyApp.Product) # returns [MyApp.Product.t]
  MyCsvto.from(path, headers: ~w{number name description price images}a) # returns {:error, reason}
  MyCsvto.from(illegal_file) # returns {:error, reason}
```
