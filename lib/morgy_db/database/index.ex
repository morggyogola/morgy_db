defmodule MorgyDb.Database.Index do
  @moduledoc """
  Manages indexes on table columns for faster lookups.
  
  Uses ETS tables to maintain hash-based indexes that map column values
  to row IDs, enabling O(1) lookups instead of full table scans.
  
  ## Example
  
      # Create an index on the 'email' column of 'users' table
      index = Index.new("users", "email")
  
      # Add entries to the index
      Index.add(index, "alice@example.com", 1)
      Index.add(index, "bob@example.com", 2)
  
      # Fast lookup by email
      Index.lookup(index, "alice@example.com")
      # => [1]
  """

  defstruct [:name, :column, :table_ref, :type]

  @type t :: %__MODULE__{
          name: atom(),
          column: String.t(),
          table_ref: :ets.tid(),
          type: :hash
        }

  @doc """
  Creates a new index on a column.
  
  The index is stored in an ETS table with a name derived from
  the table and column names.
  
  ## Parameters
  
    - `table_name` - Name of the table being indexed
    - `column_name` - Name of the column to index
    - `type` - Index type (currently only `:hash` is supported)
  
  ## Examples
  
      iex> index = Index.new("users", "email")
      iex> is_atom(index.name)
      true
  
      iex> index = Index.new("posts", "user_id")
      iex> index.column
      "user_id"
  """
  def new(table_name, column_name, type \\ :hash) do
    index_name = :"#{table_name}_#{column_name}_idx"

    # Create ETS table for the index
    # :bag allows multiple rows with same value (for non-unique indexes)
    # :public allows access from any process
    # :named_table allows lookup by name
    table_ref = :ets.new(index_name, [:bag, :public, :named_table])

    %__MODULE__{
      name: index_name,
      column: column_name,
      table_ref: table_ref,
      type: type
    }
  end

  @doc """
  Adds an entry to the index.
  
  Maps a column value to a row ID. Multiple row IDs can be associated
  with the same value (bag semantics).
  
  ## Parameters
  
    - `index` - The index struct
    - `value` - The column value to index
    - `row_id` - The ID of the row containing this value
  
  ## Examples
  
      iex> index = Index.new("users", "email")
      iex> Index.add(index, "alice@example.com", 1)
      :ok
  
      iex> index = Index.new("posts", "user_id")
      iex> Index.add(index, 5, 101)
      iex> Index.add(index, 5, 102)  # Same user_id, different posts
      :ok
  """
  def add(%__MODULE__{table_ref: table_ref}, value, row_id) do
    :ets.insert(table_ref, {value, row_id})
    :ok
  end

  @doc """
  Looks up row IDs by value in the index.
  
  Returns a list of all row IDs that have the specified value
  for the indexed column.
  
  ## Parameters
  
    - `index` - The index struct
    - `value` - The value to look up
  
  ## Returns
  
  A list of row IDs (may be empty if value not found)
  
  ## Examples
  
      iex> index = Index.new("users", "email")
      iex> Index.add(index, "alice@example.com", 1)
      iex> Index.lookup(index, "alice@example.com")
      [1]
  
      iex> index = Index.new("posts", "user_id")
      iex> Index.add(index, 5, 101)
      iex> Index.add(index, 5, 102)
      iex> Index.lookup(index, 5)
      [101, 102]
  
      iex> index = Index.new("users", "email")
      iex> Index.lookup(index, "nonexistent@example.com")
      []
  """
  def lookup(%__MODULE__{table_ref: table_ref}, value) do
    case :ets.lookup(table_ref, value) do
      [] -> []
      results -> Enum.map(results, fn {_value, row_id} -> row_id end)
    end
  end

  @doc """
  Removes an entry from the index.
  
  Removes the mapping from a specific value to a specific row ID.
  
  ## Parameters
  
    - `index` - The index struct
    - `value` - The column value
    - `row_id` - The row ID to remove
  
  ## Examples
  
      iex> index = Index.new("users", "email")
      iex> Index.add(index, "alice@example.com", 1)
      iex> Index.remove(index, "alice@example.com", 1)
      :ok
      iex> Index.lookup(index, "alice@example.com")
      []
  """
  def remove(%__MODULE__{table_ref: table_ref}, value, row_id) do
    :ets.match_delete(table_ref, {value, row_id})
    :ok
  end

  @doc """
  Checks if a value exists in the index.
  
  Useful for enforcing unique constraints.
  
  ## Parameters
  
    - `index` - The index struct
    - `value` - The value to check
  
  ## Examples
  
      iex> index = Index.new("users", "email")
      iex> Index.exists?(index, "alice@example.com")
      false
  
      iex> Index.add(index, "alice@example.com", 1)
      iex> Index.exists?(index, "alice@example.com")
      true
  """
  def exists?(%__MODULE__{table_ref: table_ref}, value) do
    case :ets.lookup(table_ref, value) do
      [] -> false
      _ -> true
    end
  end

  @doc """
  Clears all entries from the index.
  
  Removes all mappings but keeps the index structure intact.
  
  ## Examples
  
      iex> index = Index.new("users", "email")
      iex> Index.add(index, "alice@example.com", 1)
      iex> Index.clear(index)
      :ok
      iex> Index.lookup(index, "alice@example.com")
      []
  """
  def clear(%__MODULE__{table_ref: table_ref}) do
    :ets.delete_all_objects(table_ref)
    :ok
  end

  @doc """
  Destroys the index completely.
  
  Deletes the underlying ETS table. The index cannot be used after this.
  
  ## Examples
  
      iex> index = Index.new("users", "email")
      iex> Index.destroy(index)
      :ok
  """
  def destroy(%__MODULE__{table_ref: table_ref}) do
    :ets.delete(table_ref)
    :ok
  end

  @doc """
  Returns the number of unique values in the index.
  
  ## Examples
  
      iex> index = Index.new("users", "country")
      iex> Index.add(index, "USA", 1)
      iex> Index.add(index, "USA", 2)
      iex> Index.add(index, "UK", 3)
      iex> Index.size(index)
      2
  """
  def size(%__MODULE__{table_ref: table_ref}) do
    # Get unique keys (first element of each tuple)
    :ets.tab2list(table_ref)
    |> Enum.map(fn {value, _row_id} -> value end)
    |> Enum.uniq()
    |> length()
  end
end
