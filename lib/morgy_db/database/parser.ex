defmodule MorgyDb.Database.Parser do
  @moduledoc """
  Parses SQL-like queries into executable commands.
  
  Supports:
  - CREATE TABLE
  - INSERT INTO
  - SELECT (with WHERE and JOIN)
  - UPDATE
  - DELETE
  
  ## Parsing Strategy
  
  Uses regex-based parsing for simplicity. This is suitable for educational
  purposes but a production database would use a proper parser generator
  (like ANTLR or yecc).
  
  ## Examples
  
      iex> Parser.parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
      {:ok, {:create_table, "users", [%Column{name: "id", type: :integer, ...}]}}
  
      iex> Parser.parse("INSERT INTO users VALUES (1, 'Alice')")
      {:ok, {:insert, "users", [1, "Alice"]}}
  
      iex> Parser.parse("SELECT * FROM users WHERE id = 1")
      {:ok, {:select, "users", :all, {"id", 1}}}
  
      iex> Parser.parse("UPDATE users SET name = 'Bob' WHERE id = 1")
      {:ok, {:update, "users", %{"name" => "Bob"}, {"id", 1}}}
  
      iex> Parser.parse("DELETE FROM users WHERE id = 1")
      {:ok, {:delete, "users", {"id", 1}}}
  """

  alias MorgyDb.Database.Schema

  @doc """
  Parses a SQL query string into a command tuple.
  
  Returns `{:ok, command}` on success or `{:error, reason}` on failure.
  """
  def parse(sql) do
    sql
    |> String.trim()
    |> String.trim_trailing(";")
    |> do_parse()
  end

  # CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE)
  defp do_parse("CREATE TABLE " <> rest) do
    parse_create_table(rest)
  end

  # INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')
  # INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')
  defp do_parse("INSERT INTO " <> rest) do
    parse_insert(rest)
  end

  # SELECT * FROM users
  # SELECT name, email FROM users WHERE id = 1
  # SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id
  defp do_parse("SELECT " <> rest) do
    parse_select(rest)
  end

  # UPDATE users SET name = 'Alice Smith' WHERE id = 1
  defp do_parse("UPDATE " <> rest) do
    parse_update(rest)
  end

  # DELETE FROM users WHERE id = 1
  defp do_parse("DELETE FROM " <> rest) do
    parse_delete(rest)
  end

  defp do_parse(_) do
    {:error, "Unsupported SQL command"}
  end

  # ============================================================================
  # CREATE TABLE Parser
  # ============================================================================

  defp parse_create_table(rest) do
    # CREATE TABLE table_name (col1 TYPE CONSTRAINTS, col2 TYPE, ...)
    case Regex.run(~r/^(\w+)\s*\((.*)\)$/s, rest) do
      [_, table_name, columns_str] ->
        case parse_column_definitions(columns_str) do
          {:ok, columns} -> {:ok, {:create_table, table_name, columns}}
          error -> error
        end

      _ ->
        {:error, "Invalid CREATE TABLE syntax"}
    end
  end

  defp parse_column_definitions(columns_str) do
    columns_str
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reduce_while({:ok, []}, fn col_def, {:ok, acc} ->
      case parse_column_definition(col_def) do
        {:ok, column} -> {:cont, {:ok, acc ++ [column]}}
        error -> {:halt, error}
      end
    end)
  end

  defp parse_column_definition(col_def) do
    parts = String.split(col_def, ~r/\s+/)
    [name | type_and_constraints] = parts

    case type_and_constraints do
      [] ->
        {:error, "Column #{name} missing type"}

      [type_str | constraint_strs] ->
        col_type = parse_type(type_str)
        constraints = parse_constraints(constraint_strs)
        {:ok, Schema.Column.new(name, col_type, constraints)}
    end
  end

  defp parse_type(type_str) do
    case String.upcase(type_str) do
      "INTEGER" -> :integer
      "INT" -> :integer
      "TEXT" -> :text
      "VARCHAR" -> :text
      "STRING" -> :text
      "BOOLEAN" -> :boolean
      "BOOL" -> :boolean
      "FLOAT" -> :float
      "REAL" -> :float
      "DOUBLE" -> :float
      # Default to text for unknown types
      _ -> :text
    end
  end

  defp parse_constraints(constraint_strs) do
    constraint_str = Enum.join(constraint_strs, " ") |> String.upcase()

    []
    |> add_constraint_if(String.contains?(constraint_str, "PRIMARY KEY"), :primary_key)
    |> add_constraint_if(String.contains?(constraint_str, "UNIQUE"), :unique)
    |> add_constraint_if(String.contains?(constraint_str, "NOT NULL"), :not_null)
  end

  defp add_constraint_if(list, true, constraint), do: list ++ [constraint]
  defp add_constraint_if(list, false, _constraint), do: list

  # ============================================================================
  # INSERT Parser
  # ============================================================================

  defp parse_insert(rest) do
    # INSERT INTO table_name VALUES (val1, val2, ...)
    # INSERT INTO table_name (col1, col2) VALUES (val1, val2)

    # Pattern with column names
    pattern_with_cols = ~r/^(\w+)\s*\((.*?)\)\s+VALUES\s*\((.*)\)$/si
    # Pattern without column names
    pattern_no_cols = ~r/^(\w+)\s+VALUES\s*\((.*)\)$/si

    cond do
      match = Regex.run(pattern_with_cols, rest) ->
        [_, table_name, columns_str, values_str] = match
        columns = String.split(columns_str, ",") |> Enum.map(&String.trim/1)
        values = parse_values(values_str)

        if length(columns) != length(values) do
          {:error, "Column count doesn't match value count"}
        else
          row = Enum.zip(columns, values) |> Enum.into(%{})
          {:ok, {:insert, table_name, row}}
        end

      match = Regex.run(pattern_no_cols, rest) ->
        [_, table_name, values_str] = match
        values = parse_values(values_str)
        {:ok, {:insert, table_name, values}}

      true ->
        {:error, "Invalid INSERT syntax"}
    end
  end

  defp parse_values(values_str) do
    values_str
    # Split by comma not in quotes
    |> String.split(~r/,(?=(?:[^']*'[^']*')*[^']*$)/)
    |> Enum.map(&String.trim/1)
    |> Enum.map(&parse_value/1)
  end

  defp parse_value(str) do
    str = String.trim(str)

    cond do
      # String literal with single quotes
      String.starts_with?(str, "'") and String.ends_with?(str, "'") ->
        String.slice(str, 1..-2//1)

      # String literal with double quotes
      String.starts_with?(str, "\"") and String.ends_with?(str, "\"") ->
        String.slice(str, 1..-2//1)

      # NULL
      String.upcase(str) == "NULL" ->
        nil

      # Boolean
      String.upcase(str) == "TRUE" ->
        true

      String.upcase(str) == "FALSE" ->
        false

      # Integer
      Regex.match?(~r/^-?\d+$/, str) ->
        String.to_integer(str)

      # Float
      Regex.match?(~r/^-?\d+\.\d+$/, str) ->
        String.to_float(str)

      # Default to string
      true ->
        str
    end
  end

  # ============================================================================
  # SELECT Parser
  # ============================================================================

  defp parse_select(rest) do
    # Check for JOIN
    if String.match?(rest, ~r/JOIN/i) do
      parse_select_with_join(rest)
    else
      parse_select_simple(rest)
    end
  end

  defp parse_select_simple(rest) do
    # SELECT columns FROM table [WHERE condition]
    pattern = ~r/^(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$/si

    case Regex.run(pattern, rest) do
      [_, columns_str, table_name] ->
        columns = parse_select_columns(columns_str)
        {:ok, {:select, table_name, columns, nil}}

      [_, columns_str, table_name, where_str] ->
        columns = parse_select_columns(columns_str)
        where = parse_where(where_str)
        {:ok, {:select, table_name, columns, where}}

      _ ->
        {:error, "Invalid SELECT syntax"}
    end
  end

  defp parse_select_with_join(rest) do
    # SELECT cols FROM t1 JOIN t2 ON t1.col1 = t2.col2 [WHERE condition]
    pattern =
      ~r/^(.*?)\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+([\w.]+)\s*=\s*([\w.]+)(?:\s+WHERE\s+(.+))?$/si

    case Regex.run(pattern, rest) do
      [_, columns_str, table1, table2, join_col1, join_col2] ->
        columns = parse_select_columns(columns_str)
        {:ok, {:join, table1, table2, join_col1, join_col2, columns, nil}}

      [_, columns_str, table1, table2, join_col1, join_col2, where_str] ->
        columns = parse_select_columns(columns_str)
        where = parse_where(where_str)
        {:ok, {:join, table1, table2, join_col1, join_col2, columns, where}}

      _ ->
        {:error, "Invalid JOIN syntax"}
    end
  end

  defp parse_select_columns(columns_str) do
    case String.trim(columns_str) do
      "*" ->
        :all

      cols ->
        cols
        |> String.split(",")
        |> Enum.map(&String.trim/1)
    end
  end

  defp parse_where(nil), do: nil

  defp parse_where(where_str) do
    # Simple equality check: column = value
    # Supports: id = 1, name = 'Alice', users.id = 1
    case Regex.run(~r/^([\w.]+)\s*=\s*(.+)$/i, String.trim(where_str)) do
      [_, column, value_str] ->
        {String.trim(column), parse_value(String.trim(value_str))}

      _ ->
        nil
    end
  end

  # ============================================================================
  # UPDATE Parser
  # ============================================================================

  defp parse_update(rest) do
    # UPDATE table SET col1 = val1, col2 = val2 [WHERE condition]
    pattern = ~r/^(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.+))?$/si

    case Regex.run(pattern, rest) do
      [_, table_name, set_str] ->
        case parse_set_clause(set_str) do
          {:ok, updates} -> {:ok, {:update, table_name, updates, nil}}
          error -> error
        end

      [_, table_name, set_str, where_str] ->
        case parse_set_clause(set_str) do
          {:ok, updates} ->
            where = parse_where(where_str)
            {:ok, {:update, table_name, updates, where}}

          error ->
            error
        end

      _ ->
        {:error, "Invalid UPDATE syntax"}
    end
  end

  defp parse_set_clause(set_str) do
    assignments =
      set_str
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.map(&parse_assignment/1)

    if Enum.any?(assignments, &is_nil/1) do
      {:error, "Invalid SET clause"}
    else
      {:ok, Enum.into(assignments, %{})}
    end
  end

  defp parse_assignment(assignment) do
    case String.split(assignment, "=", parts: 2) do
      [col, val] ->
        {String.trim(col), parse_value(String.trim(val))}

      _ ->
        nil
    end
  end

  # ============================================================================
  # DELETE Parser
  # ============================================================================

  defp parse_delete(rest) do
    # DELETE FROM table [WHERE condition]
    pattern = ~r/^(\w+)(?:\s+WHERE\s+(.+))?$/si

    case Regex.run(pattern, rest) do
      [_, table_name] ->
        {:ok, {:delete, table_name, nil}}

      [_, table_name, where_str] ->
        where = parse_where(where_str)
        {:ok, {:delete, table_name, where}}

      _ ->
        {:error, "Invalid DELETE syntax"}
    end
  end
end
