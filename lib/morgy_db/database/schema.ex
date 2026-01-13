defmodule MorgyDb.Database.Schema do
  @moduledoc """
  Defines column types and table schemas for MorgyDb.
  
  This module provides the foundational data structures for defining
  database tables with typed columns and constraints.
  """

  @type column_type :: :integer | :text | :boolean | :float
  @type constraint :: :primary_key | :unique | :not_null

  defstruct [:name, :columns, :primary_key, :unique_keys]

  @doc """
  Defines a column with its type and constraints.
  """
  defmodule Column do
    @moduledoc """
    Represents a single column in a table schema.
    """

    defstruct [:name, :type, :constraints]

    @doc """
    Creates a new column definition.
    
    ## Examples
    
        iex> Column.new("id", :integer, [:primary_key])
        %Column{name: "id", type: :integer, constraints: [:primary_key]}
    """
    def new(name, type, constraints \\ []) do
      %__MODULE__{
        name: name,
        type: type,
        constraints: constraints
      }
    end

    @doc "Checks if column is a primary key"
    def primary_key?(%__MODULE__{constraints: constraints}) do
      :primary_key in constraints
    end

    @doc "Checks if column has unique constraint"
    def unique?(%__MODULE__{constraints: constraints}) do
      :unique in constraints or :primary_key in constraints
    end

    @doc "Checks if column is not null"
    def not_null?(%__MODULE__{constraints: constraints}) do
      :not_null in constraints or :primary_key in constraints
    end
  end

  @doc """
  Creates a new table schema.
  
  ## Examples
  
      iex> columns = [
      ...>   Column.new("id", :integer, [:primary_key]),
      ...>   Column.new("name", :text, [:not_null])
      ...> ]
      iex> schema = Schema.new("users", columns)
      iex> schema.name
      "users"
  """
  def new(name, columns) do
    primary_key = Enum.find(columns, &Column.primary_key?/1)
    unique_keys = Enum.filter(columns, &Column.unique?/1)

    %__MODULE__{
      name: name,
      columns: columns,
      primary_key: primary_key,
      unique_keys: unique_keys
    }
  end

  @doc """
  Validates a row against the schema.
  
  Returns `:ok` if valid, or `{:error, reason}` if invalid.
  
  ## Examples
  
      iex> schema = Schema.new("users", [Column.new("id", :integer)])
      iex> Schema.validate_row(schema, %{"id" => 1})
      :ok
  
      iex> schema = Schema.new("users", [Column.new("id", :integer)])
      iex> Schema.validate_row(schema, %{"id" => "not a number"})
      {:error, "Invalid type for column id: expected integer"}
  """
  def validate_row(%__MODULE__{columns: columns}, row) do
    with :ok <- validate_column_count(columns, row),
         :ok <- validate_types(columns, row),
         :ok <- validate_constraints(columns, row) do
      :ok
    end
  end

  defp validate_column_count(columns, row) do
    if length(columns) == map_size(row) do
      :ok
    else
      {:error, "Column count mismatch: expected #{length(columns)}, got #{map_size(row)}"}
    end
  end

  defp validate_types(columns, row) do
    Enum.reduce_while(columns, :ok, fn col, _acc ->
      value = Map.get(row, col.name)

      if valid_type?(value, col.type) do
        {:cont, :ok}
      else
        {:halt, {:error, "Invalid type for column #{col.name}: expected #{col.type}"}}
      end
    end)
  end

  defp validate_constraints(columns, row) do
    Enum.reduce_while(columns, :ok, fn col, _acc ->
      value = Map.get(row, col.name)

      cond do
        Column.not_null?(col) and is_nil(value) ->
          {:halt, {:error, "Column #{col.name} cannot be null"}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  # Type validation helpers
  defp valid_type?(value, :integer) when is_integer(value), do: true
  defp valid_type?(value, :text) when is_binary(value), do: true
  defp valid_type?(value, :boolean) when is_boolean(value), do: true
  defp valid_type?(value, :float) when is_float(value), do: true
  # null is valid for nullable columns
  defp valid_type?(nil, _type), do: true
  defp valid_type?(_value, _type), do: false

  @doc """
  Gets column by name from the schema.
  
  ## Examples
  
      iex> columns = [Column.new("id", :integer), Column.new("name", :text)]
      iex> schema = Schema.new("users", columns)
      iex> col = Schema.get_column(schema, "name")
      iex> col.type
      :text
  """
  def get_column(%__MODULE__{columns: columns}, column_name) do
    Enum.find(columns, fn col -> col.name == column_name end)
  end
end
