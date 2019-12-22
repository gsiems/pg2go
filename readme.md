# pg2go

My exploration of generating Go source code using Postgresql database metadata.

## pg2go

Generates structures for tables, views, user defined types, and set-returning functions.


    Usage of ./pg2go:
      -U string
            The database user to connect as when generating code (required).

      -app-user string
            The name of the application user (required). Only code for those objects that this user has privileges for will be generated.

      -database string
            The name of the database to connect to (required).

      -host string
            The database host to connect to. (default "localhost")

      -no-nulls
            Use only go datatypes in structures.

      -objects string
            The comma-separated list of the database objects to generate a structs for (defaults to all).

      -package string
            The package name (defaults to main). (default "main")

      -port int
            The port to connect to. (default 5432)

      -schema string
            The database schema to generate structs for (defaults to all).
