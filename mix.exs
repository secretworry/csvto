defmodule Csvto.Mixfile do
  use Mix.Project

  @version "0.1.0"

  def project do
    [app: :csvto,
     version: @version,
     elixir: "~> 1.3",
     elixirc_paths: elixirc_paths(Mix.env),
     description: description,
     package: package,
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps()]
  end

  defp description do
    """
    Convert csv file to elixir maps with ease
    """
  end

  defp package do
    [
      name: :csvto,
      files: ["lib", "mix.exs", "README*", "LICENSE*"],
      maintainers: ["dusiyh@gmail.com"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/secretworry/csvto"}
    ]
  end

  # Configuration for the OTP application
  #
  # Type "mix help compile.app" for more information
  def application do
    [applications: [:logger, :timex, :csv]]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: elixirc_paths ++ ["test/support"]
  defp elixirc_paths(_), do: elixirc_paths
  defp elixirc_paths, do: ["lib"]

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type "mix help deps" for more examples and options
  defp deps do
    [{:csv, github: "beatrichartz/csv", branch: :master},
     {:decimal, "~> 1.3.1"},
     {:timex, "~> 3.1.5"},
     {:ex_doc, ">= 0.0.0", only: :dev}]
  end
end
