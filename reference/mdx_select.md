# MDX Builder: builds the SELECT statement on `columns` and `rows`.

MDX Builder: builds the SELECT statement on `columns` and `rows`.

## Usage

``` r
mdx_select(columns, rows, dim_props, .head = NULL)
```

## Arguments

- columns:

  Character vector of measures, with name of dimension as the name of
  the list. Name defaults to `Measures`.

- rows:

  Accepts `data.frame` with columns `dim`, `attr_hier`, and `lvl_memb`,
  or character list of hierarchies, with name of dimension as the name
  of the list.

- dim_props:

  Must be `data.frame` with columns `dim`, `attr_hier`, and `lvl_memb`.

- .head:

  **\[experimental\]**

  Optional. Single integer vector to indicate how many rows from the top
  to return. Note: `tail` is not supported on database backends.

## Value

A `sql`/`character` object.

## Examples

``` r
if (FALSE) { # \dontrun{
mdx_select(
  'Case Count',
  set_names(
    list(
      c(
        "Age Group",
        "Age Group 04",
        "Age Group 05",
        "Age Group 09",
        "Age Group 10",
        "Age Group 11",
        "Age Group 17",
        "Age Group 20",
        "Age Group 24",
        "Age Years"
      )
    ),
    'Case - Age at Earliest Date'
  )
)
} # }
```
