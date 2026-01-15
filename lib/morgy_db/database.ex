defmodule MorgyDb.Database do
  @moduledoc """
  Main database coordinator implemented as a GenServer.
  
  This is the primary interface for interacting with MorgyDb. It maintains
  the database state (all tables) and coordinates operations between the
  Parser, Executor, and Table modules.
  
  ## Architecture
  ```
  User/Application
        ↓
    Database GenServer (this module)
        ↓
    Parser → Executor → Table → ETS
  ```
  
  ## State
  
  The GenServer maintains a map of table_name => Table struct:
  
      %{
        "users" => %Table{...},
        "posts" => %Table{...}
      }
  
  ## Usage
  
      # Start the database
      {:ok, pid} = Database.start_link()
  
      # Execute SQL queries
      Database.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
      Database.query("INSERT INTO users VALUES (1, 'Alice')")
      Database.query("SELECT * FROM users")
  
      # Get metadata
      Database.list_tables()
      Database.describe_table("users")
  
      # Reset database
      Database.reset()
  """

  use GenServer
  alias MorgyDb.Database.{Parser, QueryExecutor, Table}

  # ============================================================================
  # Client API
  # ============================================================================

  @doc """
  Starts the database GenServer.
  
  ## Options
  
    - `:name` - Name to register the process (default: `MorgyDb.Database`)
  
  ## Examples
  
      iex> {:ok, pid} = Database.start_link()
      iex> is_pid(pid)
      true
  
      iex> {:ok, _pid} = Database.start_link(name: :my_db)
  """
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, :ok, name: name)
  end

  @doc """
  Executes a SQL query string.
  
  Parses and executes the SQL query, returning results or error messages.
  
  ## Parameters
  
    - `sql` - SQL query string
    - `server` - GenServer name or pid (default: `MorgyDb.Database`)
  
  ## Returns
  
  For SELECT/JOIN queries:
    - `{:ok, rows}` - List of matching rows
  
  For DDL/DML operations:
    - `{:ok, message}` - Success message
  
  On error:
    - `{:error, reason}` - Error description
  
  ## Examples
  
      # CREATE TABLE
      iex> Database.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
      {:ok, "Table 'users' created"}
  
      # INSERT
      iex> Database.query("INSERT INTO users VALUES (1, 'Alice')")
      {:ok, "1 row inserted"}
  
      # SELECT
      iex> Database.query("SELECT * FROM users")
      {:ok, [%{"id" => 1, "name" => "Alice"}]}
  
      # UPDATE
      iex> Database.query("UPDATE users SET name = 'Alice Smith' WHERE id = 1")
      {:ok, "1 row(s) updated"}
  
      # DELETE
      iex> Database.query("DELETE FROM users WHERE id = 1")
      {:ok, "1 row(s) deleted"}
  
      # Error handling
      iex> Database.query("SELECT * FROM nonexistent")
      {:error, "Table 'nonexistent' does not exist"}
  """
  def query(sql, server \\ __MODULE__) do
    GenServer.call(server, {:query, sql})
  end

  @doc """
  Lists all table names in the database.
  
  ## Examples
  
      iex> Database.query("CREATE TABLE users (id INTEGER)")
      iex> Database.query("CREATE TABLE posts (id INTEGER)")
      iex> Database.list_tables()
      {:ok, ["posts", "users"]}
  """
  def list_tables(server \\ __MODULE__) do
    GenServer.call(server, :list_tables)
  end

  @doc """
  Gets detailed information about a specific table.
  
  Returns schema, row count, and index information.
  
  ## Parameters
  
    - `table_name` - Name of the table to describe
  
  ## Returns
  
      {:ok, %{
        name: "users",
        columns: [
          %{name: "id", type: "INTEGER", constraints: ["PRIMARY KEY"]},
          %{name: "name", type: "TEXT", constraints: []}
        ],
        indexes: ["id"],
        row_count: 5
      }}
  
  ## Examples
  
      iex> Database.describe_table("users")
      {:ok, %{name: "users", columns: [...], row_count: 0, indexes: [...]}}
  
      iex> Database.describe_table("nonexistent")
      {:error, "Table 'nonexistent' does not exist"}
  """
  def describe_table(table_name, server \\ __MODULE__) do
    GenServer.call(server, {:describe_table, table_name})
  end

  @doc """
  Resets the database by dropping all tables.
  
  This is destructive and cannot be undone.
  
  ## Examples
  
      iex> Database.reset()
      :ok
  """
  def reset(server \\ __MODULE__) do
    GenServer.call(server, :reset)
  end

  @doc """
  Gets the current database statistics.
  
  ## Returns
  
      %{
        table_count: 3,
        total_rows: 150,
        total_indexes: 5
      }
  
  ## Examples
  
      iex> Database.stats()
      %{table_count: 2, total_rows: 10, total_indexes: 3}
  """
  def stats(server \\ __MODULE__) do
    GenServer.call(server, :stats)
  end

  # ============================================================================
  # Server Callbacks
  # ============================================================================

  @impl true
  def init(:ok) do
    # Initial state: empty database (no tables)
    {:ok, %{tables: %{}}}
  end

  @impl true
  def handle_call({:query, sql}, _from, state) do
    # Parse the SQL query
    case Parser.parse(sql) do
      {:ok, command} ->
        # Execute the parsed command
        case QueryExecutor.execute(command, state.tables) do
          {:ok, result, new_tables} ->
            # Update state with new tables
            new_state = %{state | tables: new_tables}
            {:reply, {:ok, result}, new_state}

          {:error, reason, new_tables} ->
            # Even on error, some state might have changed
            new_state = %{state | tables: new_tables}
            {:reply, {:error, reason}, new_state}
        end

      {:error, reason} ->
        # Parse error - state unchanged
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:list_tables, _from, state) do
    table_names = Map.keys(state.tables) |> Enum.sort()
    {:reply, {:ok, table_names}, state}
  end

  @impl true
  def handle_call({:describe_table, table_name}, _from, state) do
    case Map.get(state.tables, table_name) do
      nil ->
        {:reply, {:error, "Table '#{table_name}' does not exist"}, state}

      table ->
        info = %{
          name: table.name,
          columns:
            Enum.map(table.schema.columns, fn col ->
              %{
                name: col.name,
                type: col.type |> Atom.to_string() |> String.upcase(),
                constraints: Enum.map(col.constraints, &constraint_to_string/1)
              }
            end),
          indexes: Map.keys(table.indexes) |> Enum.sort(),
          row_count: Table.count(table)
        }

        {:reply, {:ok, info}, state}
    end
  end

  @impl true
  def handle_call(:reset, _from, state) do
    # Destroy all tables and their ETS storage
    Enum.each(state.tables, fn {_name, table} ->
      Table.destroy(table)
    end)

    # Return empty state
    {:reply, :ok, %{tables: %{}}}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    stats = %{
      table_count: map_size(state.tables),
      total_rows: calculate_total_rows(state.tables),
      total_indexes: calculate_total_indexes(state.tables)
    }

    {:reply, stats, state}
  end

  # ============================================================================
  # Private Helper Functions
  # ============================================================================

  defp constraint_to_string(:primary_key), do: "PRIMARY KEY"
  defp constraint_to_string(:unique), do: "UNIQUE"
  defp constraint_to_string(:not_null), do: "NOT NULL"

  defp calculate_total_rows(tables) do
    tables
    |> Map.values()
    |> Enum.map(&Table.count/1)
    |> Enum.sum()
  end

  defp calculate_total_indexes(tables) do
    tables
    |> Map.values()
    |> Enum.map(fn table -> map_size(table.indexes) end)
    |> Enum.sum()
  end
end
