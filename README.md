
<!-- README.md is generated from README.Rmd. Please edit that file -->

# `phrdwRdata`

<!-- badges: start -->
<!-- badges: end -->

[`phrdwRdata`](https://mikuo0628.github.io/phrdwRdata/) is a package
designed specifically for BCCDC users to easily query and pull pre-built
datasets by abstracting away server connections, handling data sources,
writing specific MDX or SQL queries, and joining different fact tables,
etc.

Users can consistently pull data without in-depth domain knowledge
pertaining to specific data sources or flows, nor the need to explore
backend tables first follow by testing and validations.

Users just need to provide parameters, if they wish, such as query
dates, disease name, health regions, and various other built-in filters
specific to each dataset, and be able to proceed with their analyses or
pipeline.

## Prerequisites

0.  `Windows` operating system (only required if accessing data cubes)

1.  PHSA network: `PHSABC.EHCNET.CA`

    - This can be checked with `Sys.getenv('USERDNSDOMAIN')`.

2.  Access approval

    - Users would need to submit access
      [here](https://healthbc.sharepoint.com/sites/panda/SitePages/PANDA-User-Request.aspx)
      and be approved by the mart-designated data stewards.

3.  R, version 4+

    - For Windows, see
      [here](https://cran.r-project.org/bin/windows/base/).

4.  Necessary ODBC drivers that match installed R’s architecture (32- or
    64-bit)

    - Check `Drivers` tab by running one of the following:

      - 32-bit: `%windir%\syswow64\odbcad32.exe`
      - 64-bit: `%windir%\system32\odbcad32.exe`

## Installation

The **best** way to install is via Github. This ensures up-to-date
functionality and bug fixes.

To install from Github, you may require additional packages, such as
`devtools` or `renv`.

``` r
# with devtools
install.packages('devtools')
devtools::install_github(
  'mikuo0628/phrdwRdata',
  # if installation runs into issue with test load
  # INSTALL_opts = '--no-test-load',
  # to upgrade dependencies: use if dependencies out of date
  # upgrade = 'ask',
  dependencies = T,
  force = T
)

# with renv
install.packages('renv')
renv::install(
  'mikuo0628/phrdwRdata',
  rebuild = T,
  dependencies = T,
  prompt = F
)
```

Installation from Gitlab will be available soon.

Alternatively, you can install from a pre-compiled tarball that could be
found on the network drive. Although this may not guarantee newest
version. Users should take note of the version number in the file name.

``` r
install.packages(
  file.path(
    "O:/BCCDC/Groups/Analytics_Resources/Coding",
    "Git_Repository/phrdwRdata_1.0.0.tar.gz" # version number may be different
  ),
  # if installation runs into issue with test load
  # INSTALL_opts = '--no-test-load', 
  repos = NULL
)
```

## Usages

All legacy functionality is retained. Please read `vignette('Legacy')`.

### What’s new?

- `phrdwRdata` can now be used outside the Microsoft R client (ver
  3.5.2), in newer R versions, and some previously required but
  deprecated (and thus could no longer be found) packages are no longer
  needed (`olapR`, `RODBCext`, etc).
- *Installation* of this package is available and preferred, as opposed
  to “side-loading” a pre-compiled version stored on the network drive,
  which can lead to unexpected issues (because the package is not
  compiled for your specific architecture).
- Datasets retrieved from SQL now leverages
  [`dbplyr`](https://dbplyr.tidyverse.org/).
- Datasets retrieved from SQL are pre-optimized by performing `WHERE` on
  the joining tables first before `JOIN`. This improves performance on
  the initial call (ie. before internal caching).

``` r
# Legacy
system.time(
 phrdwRdata::get_phrdw_data(
   phrdw_datamart_connection = connect_to_phrdw('CD Mart'),
   phrdw_datamart            = 'CD Mart',
   dataset_name              = 'Investigation',
   query_start_date          = '2021-01-01',
   query_end_date            = '2022-01-01'
 )
)

# Stable
system.time(
 phrdwRdata::get_phrdw_data(
   mart             = 'CD',
   dataset_name     = 'Investigation',
   query_start_date = '2021-01-01',
   query_end_date   = '2022-01-01'
 )
)
```

    # Legacy
       user  system elapsed 
       1.35    0.67   22.58 
       
    # Stable
       user  system elapsed 
       1.84    0.56   10.86 

- There is also an experimental `common table expression` (CTE)
  parameter which will attempt to execute SQL queries as CTEs using
  `WITH` instead as subqueries.

- Using the function is now more streamlined with less repetition.

  - No need to establish connection first.
  - No need to repeat `phrdw_datamart`.

``` r
# Legacy
phrdwRdata::get_phrdw_data(
  phrdw_datamart_connection = connect_to_phrdw('CD Mart'),
  phrdw_datamart            = 'CD Mart',
  dataset_name              = 'Investigation',
  query_start_date          = '2021-01-01',
  query_end_date            = '2022-01-01'
)

# Stable
phrdwRdata::get_phrdw_data(
  mart             = 'CD',
  dataset_name     = 'Investigation',
  query_start_date = '2021-01-01',
  query_end_date   = '2022-01-01'
)
```

- More forgiving user inputs.

  - Not case-sensitive.

  ``` r
  phrdwRdata::get_phrdw_data(
    mart             = 'cd', 
    dataset_name     = 'investigation',
    query_start_date = '2021-01-01',
    query_end_date   = '2022-01-01'
  )
  ```

  - Dates can be character strings, and handles open bounds gracefully.

- Troubleshooting messages and tools.

  - Meaningful error messages for users to troubleshoot.

  ``` r
  phrdwRdata::get_phrdw_data(
    mart             = 'xyz', 
    dataset_name     = 'investigation',
    query_start_date = '2021-01-01',
    query_end_date   = '2022-01-01'
  )
  ```

      Error: Please check argument `mart` spelling.
      It should be one of the following (non case-sensitive):

        - CD
        - CDI
        - Enteric
        - Respiratory
        - STIBBI
        - TAT
        - VPD

  ``` r
  phrdwRdata::get_phrdw_data(
    mart             = 'cd', 
    dataset_name     = 'investigations',
    query_start_date = '2021-01-01',
    query_end_date   = '2022-01-01'
  )
  ```

      Error: Please check `dataset_name` spelling.
      It should be one of the following (case-sensitive if legacy):

        - Investigation
        - Client
        - Risk Factor
        - Symptom
        - Observation
        - UDF
        - Lab
        - Transmission Events
        - Contacts
        - Outbreaks
        - Complication
        - TB Contacts
        - TB Investigation
        - TB Transmission Events
        - TB Client
        - TB TST Investigation
        - TB TST Client
        - TB Lab

  - Return query instead of data.
  - Examine dataset metadata.

- Beyond the default built-in filters: users can leverage metadata info
  and dynamic dots to query with additional filters. See Examples in
  [phrdwRdata::get_phrdw_data()](https://mikuo0628.github.io/phrdwRdata/reference/get_phrdw_data.html).

## Troubleshooting

Please see `vignette('FAQ')`.
