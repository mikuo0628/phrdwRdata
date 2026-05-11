# Wrapper to explore cube names, dimensions, hierarchies, and levels of cube connection.

Given `olapCnn` OLAP connection string, returns info on cubes under the
OLAP server. If no cube information is supplied, it will return all the
information of the highest level.

## Usage

``` r
explore(olapCnn, cube = NULL, dimension = NULL, hierarchy = NULL, level = NULL)
```

## Arguments

- olapCnn:

  Required. An OLAP connection string. Can be created with
  [`connect_to_phrdw()`](https://mikuo0628.github.io/phrdwRdata/reference/connect_to_phrdw.md).

- cube:

  Optional. Cube name.

- dimension:

  Optional. Dimension name.

- hierarchy:

  Optional. Hierarchy name.

- level:

  Optional. Level name.

## Value

Character vector of cube(s) information.
