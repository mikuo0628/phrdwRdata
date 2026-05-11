# MDX builder: takes all MDX functions and build query.

MDX builder: takes all MDX functions and build query.

## Usage

``` r
mdx_build(
  cube_name,
  columns,
  rows,
  dim_props,
  .head,
  discrete = NULL,
  range = NULL
)
```

## Arguments

- cube_name:

  Cube name.

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

- discrete:

  A `data.frame` object with 3 columns: `dim`, `attr`, and `memb`, for
  "dimension", "attribute", and "member". The attribute must belong to
  dimension, and member must belong to the attribute hierarchy. Each
  member to filter for should have its own row in this `data.frame`.

- range:

  A `data.frame` object with 3 columns: `dim`, `attr`, and `memb`, for
  "dimension", "attribute", and "member". The attribute must belong to
  dimension, and member must belong to the attribute hierarchy. Two rows
  must be provided here with two different member values as the `from`
  and `to`.

  If no bounds, use `NULL` or "null".

## Value

A `sql`/`character` object.

## Examples

``` r
if (FALSE) { # \dontrun{
mdx_build(
  cube_name = 'StibbiDM',
  columns = 'Case Count',
  rows =
    set_names(
      list(
        c(
          "Age Group 10",
          "Age Group 24",
          "Age Years"
        )
      ),
      'Case - Age at Earliest Date'
    ),
  discrete =
    tibble(
      dim = 'LIS - Test',
      attr = 'Test Code',
      memb = c('TPE1', 'RPR')
    ),
  range =
    tibble(
      dim = 'LIS - Date - Collection',
      attr = 'Date',
      memb = c('2019-01-01', '2019-02-02')
    )
)
} # }
```
