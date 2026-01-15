defmodule MorgyDb.Database.Table do
  @moduledoc """
  Represents a database table with CRUD operations.
  
  Uses ETS for in-memory storage and maintains indexes on unique/primary key columns
  for fast lookups.
  
  ## Storage Structure
  
  - Main table: ETS table storing {row_id, row_data}
  - Indexes: Separate ETS tables for each indexed column
  - Schema: Validates all operations
  
  ## Example
  
      # Create a table
      columns = [
        Schema.Column.new("id", :integer, [:primary_key]),
        Schema.Column.new("name", :text, [:not_null]),
        Schema.Column.new("email", :text, [:unique])
      ]
      schema = Schema.new("users", columns)
      table = Table.new(schema)
  
      # Insert data
      {:ok, table} = Table.insert(table, %{"id" => 1, "name" => "Alice", "email" => "alice@example.com"})
  
      # Query data
      rows = Table.select(table, fn row -> row["id"] == 1 end)
  
      # Update data
      {:ok, table, count} = Table.update(table, fn row -> row["id"] == 1 end, %{"name" => "Alice Smith"})
  
      # Delete data
      {:ok, table, count} = Table.delete(table, fn row -> row["id"] == 1 end)
  """

  alias MorgyDb.Database.{Schema, Index}

  defstruct [:name, :schema, :table_ref, :indexes, :next_id]

  @type t :: %__MODULE__{
          name: String.t(),
          schema: Schema.t(),
          table_ref: :ets.tid(),
          indexes: %{String.t() => Index.t()},
          next_id: integer()
        }

  @doc """
  Creates a new table with the given schema.
  
  Automatically creates indexes for:
  - Primary key columns
  - Unique columns
  
  ## Parameters
  
    - `schema` - The table schema defining columns and constraints
  
  ## Examples
  
      iex> columns = [Schema.Column.new("id", :integer, [:primary_key])]
      iex> schema = Schema.new("users", columns)
      iex> table = Table.new(schema)
      iex> table.name
      "users"
  """
  def new(schema) do
    table_ref = :ets.new(String.to_atom(schema.name), [:set, :public, :named_table])

    # Create indexes for unique columns (including primary key)
    indexes =
      schema.unique_keys
      |> Enum.map(fn col -> {col.name, Index.new(schema.name, col.name)} end)
      |> Enum.into(%{})

    %__MODULE__{
      name: schema.name,
      schema: schema,
      table_ref: table_ref,
      indexes: indexes,
      next_id: 1
    }
  end

  @doc """
  Inserts a row into the table.
  
  - Validates the row against the schema
  - Assigns primary key if not provided
  - Checks unique constraints
  - Updates indexes
  
  ## Parameters
  
    - `table` - The table struct
    - `row` - Map of column_name => value
  
  ## Returns
  
    - `{:ok, updated_table}` on success
    - `{:error, reason}` on failure
  
  ## Examples
  
      iex> columns = [
      ...>   Schema.Column.new("id", :integer, [:primary_key]),
      ...>   Schema.Column.new("name", :text)
      ...> ]
      iex> schema = Schema.new("users", columns)
      iex> table = Table.new(schema)
      iex> {:ok, table} = Table.insert(table, %{"id" => 1, "name" => "Alice"})
      iex> Table.count(table)
      1
  
      # Auto-assign primary key
      iex> {:ok, table} = Table.insert(table, %{"name" => "Bob"})
      iex> Table.count(table)
      2
  
      # Unique constraint violation
      iex> columns = [Schema.Column.new("email", :text, [:unique])]
      iex> schema = Schema.new("users", columns)
      iex> table = Table.new(schema)
      iex> {:ok, table} = Table.insert(table, %{"email" => "alice@example.com"})
      iex> Table.insert(table, %{"email" => "alice@example.com"})
      {:error, "Unique constraint violation on column: email"}
  """
  def insert(%__MODULE__{} = table, row) do
    # Assign primary key if not provided
    row_with_id = assign_id(table, row)

    with :ok <- Schema.validate_row(table.schema, row_with_id),
         :ok <- check_unique_constraints(table, row_with_id) do
      row_id = get_row_id(table, row_with_id)

      # Insert into main table
      :ets.insert(table.table_ref, {row_id, row_with_id})

      # Update indexes
      update_indexes(table, row_with_id, row_id)

      # Increment next_id if we have a primary key
      new_next_id =
        if table.schema.primary_key do
          max(table.next_id, row_id + 1)
        else
          table.next_id
        end

      {:ok, %{table | next_id: new_next_id}}
    end
  end

  @doc """
  Selects rows from the table based on a condition.
  
  If no condition is provided, returns all rows.
  
  ## Parameters
  
    - `table` - The table struct
    - `condition` - Function that takes a row and returns true/false (optional)
  
  ## Returns
  
  List of rows that match the condition
  
  ## Examples
  
      iex> columns = [Schema.Column.new("id", :integer), Schema.Column.new("age", :integer)]
      iex> schema = Schema.new("users", columns)
      iex> table = Table.new(schema)
      iex> {:ok, table} = Table.insert(table, %{"id" => 1, "age" => 25})
      iex> {:ok, table} = Table.insert(table, %{"id" => 2, "age" => 30})
      iex> rows = Table.select(table, fn row -> row["age"] > 25 end)
      iex> length(rows)
      1
  """
  def select(%__MODULE__{table_ref: table_ref}, condition \\ nil) do
    all_rows =
      :ets.tab2list(table_ref)
      |> Enum.map(fn {_id, row} -> row end)

    case condition do
      nil -> all_rows
      condition -> Enum.filter(all_rows, condition)
    end
  end

  @doc """
  Selects rows using an index for optimized lookup.
  
  Falls back to full table scan if no index exists on the column.
  
  ## Parameters
  
    - `table` - The table struct
    - `column_name` - Name of the column to search
    - `value` - Value to search for
  
  ## Returns
  
  List of rows where column equals value
  
  ## Examples
  
      iex> columns = [Schema.Column.new("email", :text, [:unique])]
      iex> schema = Schema.new("users", columns)
      iex> table = Table.new(schema)
      iex> {:ok, table} = Table.insert(table, %{"email" => "alice@example.com"})
      iex> rows = Table.select_by_index(table, "email", "alice@example.com")
      iex> length(rows)
      1
  """
  def select_by_index(%__MODULE__{} = table, column_name, value) do
    case Map.get(table.indexes, column_name) do
      nil ->
        # No index, fall back to full scan
        select(table, fn row -> Map.get(row, column_name) == value end)

      index ->
        # Use index for fast lookup - O(1)
        row_ids = Index.lookup(index, value)

        Enum.map(row_ids, fn row_id ->
          case :ets.lookup(table.table_ref, row_id) do
            [{^row_id, row}] -> row
            [] -> nil
          end
        end)
        |> Enum.reject(&is_nil/1)
    end
  end

  @doc """
  Updates rows that match a condition.
  
  - Validates updated rows against schema
  - Updates indexes accordingly
  - Returns count of rows updated
  
  ## Parameters
  
    - `table` - The table struct
    - `condition` - Function that takes a row and returns true/false
    - `updates` - Map of column_name => new_value
  
  ## Returns
  
    - `{:ok, updated_table, count}` on success
    - `{:error, reason}` on validation failure
  
  ## Examples
  
      iex> columns = [Schema.Column.new("id", :integer), Schema.Column.new("name", :text)]
      iex> schema = Schema.new("users", columns)
      iex> table = Table.new(schema)
      iex> {:ok, table} = Table.insert(table, %{"id" => 1, "name" => "Alice"})
      iex> {:ok, table, count} = Table.update(table, fn row -> row["id"] == 1 end, %{"name" => "Alice Smith"})
      iex> count
      1
  """
  def update(%__MODULE__{} = table, condition, updates) do
    rows_to_update = select(table, condition)

    updated_table =
      Enum.reduce(rows_to_update, table, fn row, acc_table ->
        updated_row = Map.merge(row, updates)
        row_id = get_row_id(acc_table, row)

        # Validate updated row
        case Schema.validate_row(acc_table.schema, updated_row) do
          :ok ->
            # Remove old index entries
            remove_from_indexes(acc_table, row, row_id)

            # Update main table
            :ets.insert(acc_table.table_ref, {row_id, updated_row})

            # Add new index entries
            update_indexes(acc_table, updated_row, row_id)

            acc_table

          {:error, _reason} ->
            # Skip invalid updates
            acc_table
        end
      end)

    {:ok, updated_table, length(rows_to_update)}
  end

  @doc """
  Deletes rows that match a condition.
  
  - Removes rows from main table
  - Removes entries from indexes
  - Returns count of rows deleted
  
  ## Parameters
  
    - `table` - The table struct
    - `condition` - Function that takes a row and returns true/false
  
  ## Returns
  
    - `{:ok, updated_table, count}`
  
  ## Examples
  
      iex> columns = [Schema.Column.new("id", :integer)]
      iex> schema = Schema.new("users", columns)
      iex> table = Table.new(schema)
      iex> {:ok, table} = Table.insert(table, %{"id" => 1})
      iex> {:ok, table} = Table.insert(table, %{"id" => 2})
      iex> {:ok, table, count} = Table.delete(table, fn row -> row["id"] == 1 end)
      iex> count
      1
      iex> Table.count(table)
      1
  """
  def delete(%__MODULE__{} = table, condition) do
    rows_to_delete = select(table, condition)

    Enum.each(rows_to_delete, fn row ->
      row_id = get_row_id(table, row)

      # Remove from indexes
      remove_from_indexes(table, row, row_id)

      # Delete from main table
      :ets.delete(table.table_ref, row_id)
    end)

    {:ok, table, length(rows_to_delete)}
  end

  @doc """
  Returns the number of rows in the table.
  
  ## Examples
  
      iex> columns = [Schema.Column.new("id", :integer)]
      iex> schema = Schema.new("users", columns)
      iex> table = Table.new(schema)
      iex> Table.count(table)
      0
  
      iex> {:ok, table} = Table.insert(table, %{"id" => 1})
      iex> Table.count(table)
      1
  """
  def count(%__MODULE__{table_ref: table_ref}) do
    :ets.info(table_ref, :size)
  end

  @doc """
  Destroys the table and all its indexes.
  
  ## Examples
  
      iex> columns = [Schema.Column.new("id", :integer)]
      iex> schema = Schema.new("users", columns)
      iex> table = Table.new(schema)
      iex> Table.destroy(table)
      :ok
  """
  def destroy(%__MODULE__{table_ref: table_ref, indexes: indexes}) do
    # Destroy all indexes
    Enum.each(indexes, fn {_name, index} -> Index.destroy(index) end)

    # Destroy main table
    :ets.delete(table_ref)

    :ok
  end

  # Private helper functions

  defp assign_id(table, row) do
    case table.schema.primary_key do
      nil ->
        # No primary key, use internal counter
        Map.put(row, :_internal_id, table.next_id)

      pk_col ->
        # Use provided primary key value or auto-generate
        case Map.get(row, pk_col.name) do
          nil -> Map.put(row, pk_col.name, table.next_id)
          _ -> row
        end
    end
  end

  defp get_row_id(table, row) do
    case table.schema.primary_key do
      nil -> Map.get(row, :_internal_id)
      pk_col -> Map.get(row, pk_col.name)
    end
  end

  defp check_unique_constraints(table, row) do
    Enum.reduce_while(table.indexes, :ok, fn {col_name, index}, _acc ->
      value = Map.get(row, col_name)

      if value && Index.exists?(index, value) do
        {:halt, {:error, "Unique constraint violation on column: #{col_name}"}}
      else
        {:cont, :ok}
      end
    end)
  end

  defp update_indexes(table, row, row_id) do
    Enum.each(table.indexes, fn {col_name, index} ->
      value = Map.get(row, col_name)
      if value, do: Index.add(index, value, row_id)
    end)
  end

  defp remove_from_indexes(table, row, row_id) do
    Enum.each(table.indexes, fn {col_name, index} ->
      value = Map.get(row, col_name)
      if value, do: Index.remove(index, value, row_id)
    end)
  end
end
