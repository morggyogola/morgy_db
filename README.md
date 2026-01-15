# MorgyDb
An educational RDBMS implementation in Elixir demonstrating core database concepts including schemas, indexes, SQL parsing, query execution, and CRUD operations.

## Features

* CREATE TABLE with column types and constraints.

* INSERT, SELECT, UPDATE, DELETE.

* INNER JOIN support.

* Primary keys and unique constraints.

* Hash-based indexing for fast lookups.
  
* REPL - Interactive command-line interface.
  
* Web App - Phoenix-based CRUD application


### Installation
  ##### Prerequisites

  * Elixir 1.14 or higher

  * Erlang/OTP 25 or higher
  
  ##### Setup
  1. Clone the Project
  2. Install dependencies - mix deps.get
  3. Compile the project - mix compile

  ##### Running the Application on REPL
  1. Start the application from IEX - iex -S mix
  2. Then start the REPL on IEX - MorgyDb.Repl.start()
  3. Example Session
  
      morgydb> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, age INTEGER)
     
      ✓ Table 'users' created

      morgydb> INSERT INTO users VALUES (1, 'Morgan', 'user@example.com', 28)
     
      ✓ 1 row inserted

     morgydb> exit
     
     Goodbye!

### Limitations

* No persistence (data is lost when app stops)
* No concurrent access control
* Limited SQL support (no subqueries, GROUP BY, ORDER BY, etc.)
* Only supports equality in WHERE clauses (no >, <, LIKE, etc.)
* Only INNER JOIN (no LEFT, RIGHT, OUTER joins)

### Credits

* Built with Elixir and Phoenix Framework
* Uses ETS (Erlang Term Storage) for in-memory storage
* Inspired by SQLite's simplicity and PostgreSQL's architecture


  

