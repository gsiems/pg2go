# pg2go

A project for generating Go source code using Postgresql database metadata.

One should ideally be able to generate more than just structs for accessing
Postgresql databases from Go.

## But, but, but

* Really?

    * Yes. While this is mostly for learning purposes I do hope it will
      prove useful.

* These kinds of utilities already exist!

    * For table/view structures, yes... and there is [xo](https://github.com/xo/xo)
      which appears to handle functions and composite types (I haven't
      looked at it closely enough to be sure).

* Why not use one of the many Go ORMs like [gorm](https://github.com/jinzhu/gorm) or
    [gorp](https://github.com/coopernurse/gorp) or an ORM builder like
    [sqlboiler](https://github.com/volatiletech/sqlboiler)?

    * I'm not a big fan of ORMs. I'm also exploring the use of
      views/functions for selecting data and functions for all create,
      update, and delete operations and past experience with ORMs
      didn't fit well with that approach.

## Tools

So far, only pg2struct.

### pg2struct

The make_structs utility generates structures for tables, views, user defined types, and set-returning functions.

  | Flag      | Description                                     |
  | --------- | ----------------------------------------------- |
  | -U string | The database user to connect as. (required)     |
  | -d string | The the database name to connect to. (required) |
  | -h string | The database host to connect to. (required)     |
  | -u string | The name of the application user that will connect to the database (required). Only those objects that the application user has privs for will be processed. |
  | -o string | The comma-separated list of the database objects to generate a structs for (defaults to all). |
  | -s string | The database schema to generate structs for (defaults to all). |
