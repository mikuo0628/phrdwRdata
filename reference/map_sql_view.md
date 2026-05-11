# Retrieve full mapping of views and columns using *sys.schema*.

Retrieve full mapping of views and columns using *sys.schema*.

## Usage

``` r
map_sql_view(
  conn = NULL,
  catalog = NULL,
  schema = NULL,
  include_datatype = F,
  tbl_vw_dependencies = F
)
```

## Arguments

- conn:

  An `odbc` connection object. Defaults to `NULL`. If `NULL`, will
  connect to `CD mart`.

- catalog:

  Catalog name if known. Default to `NULL`.

- include_datatype:

  If `TRUE`, will make an additional join with `types` in **sys**
  schema. Defaults to `FALSE`.

## Value

A named list: `map` is a `data.frame` of SQL schema, view, and columns;
`dep` is `data.frame` of dependencies.
