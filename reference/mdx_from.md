# MDX Builder: builds the `FROM` clause, and incorporate any filters if needed.

MDX Builder: builds the `FROM` clause, and incorporate any filters if
needed.

## Usage

``` r
mdx_from(cube_name, ...)
```

## Arguments

- cube_name:

  Cube name.

- ...:

  Character vector of lines, which makes up the filter query build from
  `mdx_filters`.

## Value

A `sql`/`character` object.

## Examples

``` r
if (FALSE) { # \dontrun{
mdx_from(
  'StibbiDM',
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
)
} # }
```
