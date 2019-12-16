# pg2go

A project for generating Go source code using Postgresql database metadata.

One should ideally be able to generate more than just structs for accessing
Postgresql databases from Go.

## But, but, but

* Really?

    * Yes. While this is mostly for learning purposes I do hope it will
      prove useful.

* These kinds of utilities already exist!

    * For table/view structures, yes. However I am not aware of any that
      also generate structs for user defined types and/or set-returning
      functions (not that I've looked that closely).

* Why not use one of the many Go ORMs like [gorm](https://github.com/jinzhu/gorm) or
    [gorp](https://github.com/coopernurse/gorp) or an ORM builder like
    [sqlboiler](https://github.com/volatiletech/sqlboiler)?

    * I'm not a big fan of ORMs. I'm also exploring the use of
      views/functions for selecting data and functions for all create,
      update, and delete operations and past experience with ORMs
      didn't fit well with that approach.

## Tools

So far, only make_structs.

### make_structs

The make_structs utility generates structures for tables, views, user defined types, and set-returning functions.

  | Flag      | Description                                     |
  | --------- | ----------------------------------------------- |
  | -U string | The database user to connect as. (required)     |
  | -d string | The the database name to connect to. (required) |
  | -h string | The database host to connect to. (required)     |
  | -r        | Generate structs for tables and views.          |
  | -t        | Generate structs for user defined composite types. |
  | -f        | Generate structs for result-set returning functions. |
  | -n        | Use null datatypes in structures.               |
  | -o string | The comma-separated list of the database objects to generate a structs for (defaults to all). |
  | -s string | The database schema to generate structs for (defaults to all). |
  | -u string | The name of the application user. If specified then only structs for those objects that the user has privileges for will be generated. |

Note that one of -r, -t, -f must be supplied and any combination of -r, -t, and -f may be supplied.
