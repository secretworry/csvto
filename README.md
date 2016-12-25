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
      field :price, :float, name: "Price", validate: &(&1 >= 0)
      field :images, {:array, :string}, name: "Images", separator: "|"
    end
  end

  MyCsvto.from(path, :product) # returns [Map.t]
  MyCsvto.from(path, :product, into: MyApp.Product) # returns [MyApp.Product.t]
  MyCsvto.from(path, :product, headers: ~w{number name description price images}a) # returns {:error, reason}
  MyCsvto.from(illegal_file, :product) # returns {:error, reason}
```
