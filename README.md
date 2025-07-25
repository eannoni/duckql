<p align="center">
   <img width="300" height="300" alt="platypus" src="https://github.com/user-attachments/assets/bb4f5aff-c1fa-4e85-ba03-8235a883df02" />
</p>

# DuckQL

[![Go](https://github.com/dburkart/duckql/actions/workflows/go.yml/badge.svg)](https://github.com/dburkart/duckql/actions/workflows/go.yml)

If it looks like a table, and quacks like SQL...

TL;DR: Allow LLMs to interface (safely) with arbitrary data via SQLite syntax

## Background

LLMs are great at writing code, including SQL, to accomplish tasks. Using SQL as a
data description and querying format allows a perfect blend of flexibility to service
novel requests, while still being able to enforce "bounds" on the LLM.

There are problems with opening up an existing API or app to this kind of interface though:

1) LLMs are susceptible to prompt injection, so if you're allowing 1:1 access to a database,
   you could run into a data-loss or data-injection scenario.
2) You may need to restrict access to a slice of data (accounts matching some ID, for example)
3) You may not have direct access to a database, but perhaps you have some rest apis, and want
   the power of SQL

This project is an attempt to solve the above generally in a way that is easy to "glue in" to
existing APIs or data layers.

## How it works

DuckQL operates as the glue between a `BackingStore` interface and raw SQLite queries. Depending on the
kind of data you are accessing, you might choose to use a different type of backing store. Below, we can
demonstrate the concept with a `SliceFilter`, which allows querying a slice of structs as if it were a
SQLite database.

First, point `duckql` at your "schema" structs, and export the resulting DDL to your llm of choice:

```go
type User struct {
    ID           string
    Email        string
    Name         string `ddl:"comment='First and last name of the user (space separated)'"`
    PasswordHash []byte `ddl:"-"`
}

...

s := duckql.Initialize(&User{})
s.SetPermissions(duckql.AllowSelectStatements)

ddl := s.DDL()
// Should result in:
//
// CREATE TABLE users
// (
//   id TEXT,
//   email TEXT,
//   name TEXT, -- First and last name of the user (space separated)
// )

```

In the above snippet, we call `SetPermissions()` to restrict which types of SQL queries are allowed.
Fields which should not be accessible by an LLM can be marked as private with a 'ddl' tag value of "-".
Comments can be set via the comment field. These are purely to help the LLM understand the schema.

Construct your prompt with the DDL to explain to the model how it can query information, and have it
write a query. The next step is running the query against your data. We initialize our `SliceFilter`
backing store with some hardcoded data, which we can then execute against:

```go
...

s.SetBacking(duckql.NewSliceFilter(
    s, []any{
        &User{
            ID: "a",
            Email: "a@gmail.com",
            Name: "Cindy Lou",
        },
        &User{
            ID: "b",
            Email: "b@aol.com",
            Name: "Bob Bert",
        },
    }
))

result, err := s.Execute("select name from users where email like '%aol.com")
if err != nil {
    panic(err)
}

fmt.Println(result.String())

// Bob Bert
```

In the above, `duckql` will do the following:

1) Check that users is a valid table that it knows about
2) Check that the "name" field is valid on the users table
3) Match the where clause
4) Only copy selected fields into return value

## Installing

```
go get github.com/dburkart/duckql
```

## Examples

You can find more examples in the ./examples directory. Examples so far:

 * [./examples/slice](./examples/slice): Uses a SliceFilter backing store to demonstrate using DuckQL to "emulate" a SQLite database using preset data.
 * [./examples/sqlite](./examples/sqlite): Uses the SQLite backing store to demonstrate how to use DuckQL as a "permission" layer on top of a SQLite database.
