# Csvto

**Convert csv file to elixir map with ease**

## Installation

Add csvto to your list of dependencies in `mix.exs`

  ```
  # use the stable version
  def deps do
    [{:csvto, "~> 0.1.3"}]
  end

  # use the latest version
  def deps do
    [{:csvto, github: "secretworry/csvto.git", branch: :master}]
  end

  # start csvto
  defp application do
    [applications: [:csvto]]
  end
  ```

## Quick Example

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
```

## Documentation

You can read more from our [Hexdocs](https://hexdocs.pm/csvto/api-reference.html)

* Types that support can be found [here](https://hexdocs.pm/csvto/Csvto.Builder.html)
