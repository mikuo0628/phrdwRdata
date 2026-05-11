# MDX Builder: process filters, discrete or range, by date or other data types.

MDX Builder: process filters, discrete or range, by date or other data
types.

## Usage

``` r
mdx_filter(discrete = NULL, range = NULL, ..., .as_lines = T)
```

## Arguments

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

- ...:

  Reserved for future development.

- .as_lines:

  Boolean value that if `TRUE` (default), returns a character vector of
  properly spaced MDX filter clauses. This is needed as input to
  `mdx_from` for formatting purposes. If `FALSE`, returns a single
  element character vector, for printing purposes.

## Value

Character vector.

## Examples

``` r
if (FALSE) { # \dontrun{
mdx_filter(
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
    ),
  .as_lines = T
)
} # }
```
