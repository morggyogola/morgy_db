defmodule MorgyDb.Database.QueryExecutor do
  @moduledoc """
  Executes parsed SQL commands against database tables.
  
  Takes command tuples from the Parser and performs the corresponding
  operations using the Table module.
  
  ## Execution Flow
  
  1. Parser produces command tuple
  2. Executor validates the command
  3. Executor calls appropriate Table operations
  4. Returns results or error messages
  
  ## Examples
  
      iex> tables = %{}
      iex> command = {:create_table, "users", [Column.new("id", :integer)]}
      iex> {:ok, message, new_tables} = QueryExecutor.execute(command, tables)
      iex> message
      "Table 'users' created"
  
      iex> command = {:insert, "users", %{"id" => 1}}
      iex> {:ok, message, new_tables} = QueryExecutor.execute(command, tables)
      iex> message
      "1 row inserted"
  """

  alias MorgyDb.Database.{Table, Schema}

  @doc """
  Executes a parsed command against the database tables.
  
  ## Parameters
  
    - `command` - Tuple representing the parsed SQL command
    - `tables` - Map of table_name => Table struct
  
  ## Returns
  
    - `{:ok, result, updated_tables}` on success
    - `{:error, message, tables}` on failure
  
  Where result is either:
    - A message string for DDL/DML operations
    - A list of rows for SELECT queries
  """
  def execute(command, tables) do
    case command do
      {:create_table, table_name, columns} ->
        execute_create_table(table_name, columns, tables)

      {:insert, table_name, values} ->
        execute_insert(table_name, values, tables)

      {:select, table_name, columns, where} ->
        execute_select(table_name, columns, where, tables)

      {:join, table1, table2, join_col1, join_col2, columns, where} ->
        execute_join(table1, table2, join_col1, join_col2, columns, where, tables)

      {:update, table_name, updates, where} ->
        execute_update(table_name, updates, where, tables)

      {:delete, table_name, where} ->
        execute_delete(table_name, where, tables)

      _ ->
        {:error, "Unknown command type", tables}
    end
  end

  # ============================================================================
  # CREATE TABLE Execution
  # ============================================================================

  defp execute_create_table(table_name, columns, tables) do
    if Map.has_key?(tables, table_name) do
      {:error, "Table '#{table_name}' already exists", tables}
    else
      schema = Schema.new(table_name, columns)
      table = Table.new(schema)
      new_tables = Map.put(tables, table_name, table)
      {:ok, "Table '#{table_name}' created", new_tables}
    end
  end

  # ============================================================================
  # INSERT Execution
  # ============================================================================

  defp execute_insert(table_name, values, tables) do
    case Map.get(tables, table_name) do
      nil ->
        {:error, "Table '#{table_name}' does not exist", tables}

      table ->
        # Convert list of values to map if necessary
        row = values_to_row(table, values)

        case Table.insert(table, row) do
          {:ok, updated_table} ->
            new_tables = Map.put(tables, table_name, updated_table)
            {:ok, "1 row inserted", new_tables}

          {:error, reason} ->
            {:error, reason, tables}
        end
    end
  end

  defp values_to_row(table, values) when is_map(values), do: values

  defp values_to_row(table, values) when is_list(values) do
    # Map positional values to column names
    table.schema.columns
    |> Enum.zip(values)
    |> Enum.map(fn {col, val} -> {col.name, val} end)
    |> Enum.into(%{})
  end

  # ============================================================================
  # SELECT Execution
  # ============================================================================

  defp execute_select(table_name, columns, where, tables) do
    case Map.get(tables, table_name) do
      nil ->
        {:error, "Table '#{table_name}' does not exist", tables}

      table ->
        # Build condition function from WHERE clause
        condition = build_condition(where)

        # Execute query - use index if possible
        rows =
          if where && tuple_size(where) == 2 do
            {col_name, value} = where
            # Remove table prefix if present (e.g., "users.id" -> "id")
            col_name = extract_column_name(col_name)
            Table.select_by_index(table, col_name, value)
          else
            Table.select(table, condition)
          end

        # Filter columns if not selecting all
        filtered_rows = select_columns(rows, columns)

        {:ok, filtered_rows, tables}
    end
  end

  # ============================================================================
  # JOIN Execution
  # ============================================================================

  defp execute_join(table1_name, table2_name, join_col1, join_col2, columns, where, tables) do
    table1 = Map.get(tables, table1_name)
    table2 = Map.get(tables, table2_name)

    cond do
      is_nil(table1) ->
        {:error, "Table '#{table1_name}' does not exist", tables}

      is_nil(table2) ->
        {:error, "Table '#{table2_name}' does not exist", tables}

      true ->
        # Get all rows from both tables
        rows1 = Table.select(table1)
        rows2 = Table.select(table2)

        # Parse join columns (handle "table.column" format)
        col1 = extract_column_name(join_col1)
        col2 = extract_column_name(join_col2)

        # Perform nested loop join
        joined_rows = perform_nested_loop_join(rows1, rows2, col1, col2, table1_name, table2_name)

        # Apply WHERE condition
        filtered_rows =
          if where do
            condition = build_condition(where)
            Enum.filter(joined_rows, condition)
          else
            joined_rows
          end

        # Select specified columns
        selected_rows = select_columns(filtered_rows, columns)

        {:ok, selected_rows, tables}
    end
  end

  defp perform_nested_loop_join(rows1, rows2, col1, col2, table1_name, table2_name) do
    for row1 <- rows1,
        row2 <- rows2,
        Map.get(row1, col1) == Map.get(row2, col2) do
      # Merge rows with table prefixes to avoid column name conflicts
      merged_row = %{}

      # Add columns from table1 with prefix
      merged_row =
        Enum.reduce(row1, merged_row, fn {key, value}, acc ->
          acc
          |> Map.put("#{table1_name}.#{key}", value)
          # Also add without prefix for convenience
          |> Map.put(key, value)
        end)

      # Add columns from table2 with prefix
      merged_row =
        Enum.reduce(row2, merged_row, fn {key, value}, acc ->
          prefixed_key = "#{table2_name}.#{key}"
          # Only add unprefixed key if it doesn't conflict
          if Map.has_key?(acc, key) do
            Map.put(acc, prefixed_key, value)
          else
            acc
            |> Map.put(prefixed_key, value)
            |> Map.put(key, value)
          end
        end)

      merged_row
    end
  end

  # ============================================================================
  # UPDATE Execution
  # ============================================================================

  defp execute_update(table_name, updates, where, tables) do
    case Map.get(tables, table_name) do
      nil ->
        {:error, "Table '#{table_name}' does not exist", tables}

      table ->
        # Build condition function - update all if no WHERE clause
        condition = if where, do: build_condition(where), else: fn _ -> true end

        case Table.update(table, condition, updates) do
          {:ok, updated_table, count} ->
            new_tables = Map.put(tables, table_name, updated_table)
            {:ok, "#{count} row(s) updated", new_tables}

          {:error, reason} ->
            {:error, reason, tables}
        end
    end
  end

  # ============================================================================
  # DELETE Execution
  # ============================================================================

  defp execute_delete(table_name, where, tables) do
    case Map.get(tables, table_name) do
      nil ->
        {:error, "Table '#{table_name}' does not exist", tables}

      table ->
        # Build condition function - delete all if no WHERE clause
        condition = if where, do: build_condition(where), else: fn _ -> true end

        case Table.delete(table, condition) do
          {:ok, updated_table, count} ->
            new_tables = Map.put(tables, table_name, updated_table)
            {:ok, "#{count} row(s) deleted", new_tables}

          {:error, reason} ->
            {:error, reason, tables}
        end
    end
  end

  # ============================================================================
  # Helper Functions
  # ============================================================================

  @doc """
  Builds a condition function from a WHERE clause tuple.
  
  ## Examples
  
      iex> condition = build_condition({"id", 1})
      iex> condition.(%{"id" => 1})
      true
  
      iex> condition.(%{"id" => 2})
      false
  """
  def build_condition(nil), do: nil

  def build_condition({column, value}) do
    # Handle both plain column names and table.column format
    fn row ->
      # Try to get value with full column name first, then without table prefix
      column_name = extract_column_name(column)

      row_value = Map.get(row, column) || Map.get(row, column_name)
      row_value == value
    end
  end

  @doc """
  Extracts the column name from a potentially qualified name.
  
  ## Examples
  
      iex> extract_column_name("users.id")
      "id"
  
      iex> extract_column_name("id")
      "id"
  """
  def extract_column_name(column_str) do
    case String.split(column_str, ".") do
      [_table, column] -> column
      [column] -> column
    end
  end

  @doc """
  Selects specific columns from rows.
  
  ## Examples
  
      iex> rows = [%{"id" => 1, "name" => "Alice", "email" => "alice@example.com"}]
      iex> select_columns(rows, ["name", "email"])
      [%{"name" => "Alice", "email" => "alice@example.com"}]
  
      iex> select_columns(rows, :all)
      [%{"id" => 1, "name" => "Alice", "email" => "alice@example.com"}]
  """
  def select_columns(rows, :all), do: rows

  def select_columns(rows, columns) do
    Enum.map(rows, fn row ->
      Enum.map(columns, fn col ->
        # Try with full column name first, then extract just the column part
        value =
          case String.split(col, ".") do
            [_table, column_name] ->
              # Try prefixed first, then unprefixed
              Map.get(row, col) || Map.get(row, column_name)

            [column_name] ->
              Map.get(row, column_name)
          end

        {col, value}
      end)
      |> Enum.into(%{})
    end)
  end
end
