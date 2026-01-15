defmodule MorgyDb.Repl do
  @moduledoc """
  Interactive REPL (Read-Eval-Print-Loop) for MorgyDb.
  
  Provides a command-line interface for interacting with the database.
  Supports SQL commands and meta-commands for database management.
  
  ## Usage
  
      iex> MorgyDb.Repl.start()
  
      # Or from command line:
      # elixir -S mix run -e "MorgyDb.Repl.start()"
  """

  alias MorgyDb.Database

  @doc """
  Starts the interactive REPL.
  """
  def start do
    IO.puts(banner())
    IO.puts("Type 'help' for commands, 'exit' to quit\n")

    # Start the database GenServer
    {:ok, _pid} = Database.start_link()

    # Start the REPL loop
    loop()
  end

  defp loop do
    # Read user input
    input = IO.gets("morgydb> ") |> String.trim()

    case input do
      # Exit commands
      "" ->
        loop()

      "exit" ->
        IO.puts("Goodbye!")
        :ok

      "quit" ->
        IO.puts("Goodbye!")
        :ok

      # Meta commands
      "help" ->
        print_help()
        loop()

      "tables" ->
        list_tables()
        loop()

      "reset" ->
        reset_database()
        loop()

      "stats" ->
        show_stats()
        loop()

      # Describe table command
      "describe " <> table_name ->
        describe_table(String.trim(table_name))
        loop()

      # SQL commands
      sql ->
        execute_query(sql)
        loop()
    end
  end

  # ============================================================================
  # Command Handlers
  # ============================================================================

  defp execute_query(sql) do
    case Database.query(sql) do
      {:ok, result} when is_binary(result) ->
        # DDL/DML message
        IO.puts("✓ #{result}")

      {:ok, rows} when is_list(rows) ->
        # SELECT query results
        print_table(rows)

      {:error, reason} ->
        IO.puts("✗ Error: #{reason}")
    end
  end

  defp list_tables do
    case Database.list_tables() do
      {:ok, []} ->
        IO.puts("\nNo tables found\n")

      {:ok, tables} ->
        IO.puts("\nTables:")
        Enum.each(tables, fn table -> IO.puts("  • #{table}") end)
        IO.puts("")
    end
  end

  defp describe_table(table_name) do
    case Database.describe_table(table_name) do
      {:ok, info} ->
        IO.puts("\nTable: #{info.name}")
        IO.puts("Rows: #{info.row_count}")
        IO.puts("\nColumns:")

        Enum.each(info.columns, fn col ->
          constraints = format_constraints(col.constraints)
          IO.puts("  #{col.name} #{col.type}#{constraints}")
        end)

        unless Enum.empty?(info.indexes) do
          IO.puts("\nIndexes: #{Enum.join(info.indexes, ", ")}")
        end

        IO.puts("")

      {:error, reason} ->
        IO.puts("✗ Error: #{reason}")
    end
  end

  defp reset_database do
    IO.puts("⚠️  Are you sure you want to reset the database? (yes/no)")
    confirmation = IO.gets("") |> String.trim() |> String.downcase()

    if confirmation == "yes" do
      Database.reset()
      IO.puts("✓ Database reset")
    else
      IO.puts("Cancelled")
    end
  end

  defp show_stats do
    stats = Database.stats()
    IO.puts("\nDatabase Statistics:")
    IO.puts("  Tables: #{stats.table_count}")
    IO.puts("  Total Rows: #{stats.total_rows}")
    IO.puts("  Total Indexes: #{stats.total_indexes}")
    IO.puts("")
  end

  # ============================================================================
  # Display Helpers
  # ============================================================================

  defp print_table([]), do: IO.puts("0 rows returned\n")

  defp print_table(rows) do
    IO.puts("")

    # Get all unique keys from all rows
    all_keys =
      rows
      |> Enum.flat_map(&Map.keys/1)
      |> Enum.uniq()
      |> Enum.sort()

    # Calculate column widths
    widths =
      Enum.map(all_keys, fn key ->
        max_width =
          rows
          |> Enum.map(fn row ->
            value = Map.get(row, key)
            String.length(format_value(value))
          end)
          |> Enum.max()

        max(max_width, String.length(key))
      end)

    # Print header
    header =
      all_keys
      |> Enum.zip(widths)
      |> Enum.map(fn {key, width} -> String.pad_trailing(key, width) end)
      |> Enum.join(" | ")

    IO.puts(header)
    IO.puts(String.duplicate("-", String.length(header)))

    # Print rows
    Enum.each(rows, fn row ->
      row_str =
        all_keys
        |> Enum.zip(widths)
        |> Enum.map(fn {key, width} ->
          value = Map.get(row, key)
          String.pad_trailing(format_value(value), width)
        end)
        |> Enum.join(" | ")

      IO.puts(row_str)
    end)

    IO.puts("\n#{length(rows)} row(s) returned\n")
  end

  defp format_value(nil), do: "NULL"
  defp format_value(value) when is_binary(value), do: value
  defp format_value(value), do: inspect(value)

  defp format_constraints([]), do: ""

  defp format_constraints(constraints) do
    constraints
    |> Enum.join(" ")
    |> then(&(" " <> &1))
  end

  # ============================================================================
  # Banner and Help
  # ============================================================================

  defp banner do
    """
    ╔══════════════════════════════════════╗
    ║         MorgyDb RDBMS v1.0          ║
    ║   A Simple Relational Database      ║
    ╚══════════════════════════════════════╝
    """
  end

  defp print_help do
    IO.puts("""
    
    Available Commands:
    -------------------
    SQL Commands:
      CREATE TABLE <name> (<columns>)    - Create a new table
      INSERT INTO <table> VALUES (...)   - Insert a row
      SELECT <cols> FROM <table>         - Query data
      UPDATE <table> SET ... WHERE ...   - Update rows
      DELETE FROM <table> WHERE ...      - Delete rows
      SELECT ... FROM t1 JOIN t2 ON ...  - Join tables
    
    Meta Commands:
      tables                             - List all tables
      describe <table>                   - Show table structure
      stats                              - Show database statistics
      reset                              - Drop all tables
      help                               - Show this help
      exit/quit                          - Exit REPL
    
    Examples:
      CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE)
      INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')
      SELECT * FROM users WHERE id = 1
      UPDATE users SET name = 'Alice Smith' WHERE id = 1
      DELETE FROM users WHERE id = 1
      SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id
    """)
  end
end
