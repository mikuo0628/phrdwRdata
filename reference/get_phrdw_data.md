# Retrieve pre-built datasets from specific datamarts.

Helps users retrieve pre-built datasets from specific datamarts, with
the capacity to supply values to some default filters, such as dates and
diseases. In addition to retrieving data, user can perform preliminary
self-diagnostics to troubleshoot issues, return query instead of data,
and even supply filters outside the predefined default filters.

This function retains the legacy form, which can be invoked by providing
arguments with `phrdw_` prefix, such as `phrdw_datamart_connection` and
`phrdw_datamart`.

The modern form, on the other hand, is invoked by a simple `mart` and/or
`type`, which streamlines the process, allows additional features, and
avoids redundant inputs such as `phrdw_datamart` in both
[`connect_to_phrdw()`](https://mikuo0628.github.io/phrdwRdata/reference/connect_to_phrdw.md)
and `get_phrdw_data()`.

## Usage

``` r
get_phrdw_data(
  phrdw_datamart_connection = NULL,
  phrdw_datamart = NULL,
  dataset_name = NULL,
  query_start_date = NULL,
  query_end_date = NULL,
  include_patient_identifiers = F,
  include_indigenous_identifiers = F,
  retrieve_system_ids = "Yes",
  disease = NULL,
  surveillance_condition = NULL,
  classification = NULL,
  surveillance_region_ha = NULL,
  infection_group = NULL,
  ordering_provider_ha = NULL,
  lis_status = NULL,
  episode_status = NULL,
  test_type = NULL,
  episode_testing_pattern = NULL,
  testing_region_ha = NULL,
  case_status = NULL,
  case_source = NULL,
  ucd_3_char_code = NULL,
  ccd_3_char_code = NULL,
  residential_location_ha = NULL,
  death_location_ha = NULL,
  mart = NULL,
  type = c("prod", "su", "sa")[1],
  .head = NULL,
  .check_params = F,
  .return_query = F,
  .return_data = !(.return_query || isTRUE(.check_params) || is.character(.check_params)),
  .clean_data = F,
  .query_df = NULL,
  .query_str = NULL,
  .cte = F,
  ...
)
```

## Arguments

- phrdw_datamart_connection:

  **\[superseded\]**

  Legacy function design: supply a connection object created by
  [`connect_to_phrdw()`](https://mikuo0628.github.io/phrdwRdata/reference/connect_to_phrdw.md).
  Recommend using `mart` and `type` instead for flexibility (see
  [`connect_to_phrdw()`](https://mikuo0628.github.io/phrdwRdata/reference/connect_to_phrdw.md)).
  The function takes care of connecting to the appropriate PHRDW
  database and disconnect after performing the requested data filtering
  and retrieving.

- phrdw_datamart:

  **\[superseded\]**

  Legacy mart designations provided by previous package authors. This
  backward-compatibility is meant to minimize changes on the user end.
  The `stable` approach is to reference `mart` and `type`.

- dataset_name:

  The name of the pre-built dataset to retrieve.

- query_start_date:

  Start date of your dataset. Can accept character date. Defaults to
  `NULL`, which indicates no lower bound.

- query_end_date:

  End date of your dataset. Can accept character date. Defaults to
  `NULL`, which indicates no upper bound.

- include_patient_identifiers:

  Whether to include patient identifier information. Accepts Boolean
  values. Defaults to `FALSE`. Note: user needs to have access,
  otherwise data restriction may return unintended results.

- include_indigenous_identifiers:

  Whether to include indigenous identifier information. Accepts Boolean
  values. Defaults to `FALSE`. Note: user needs to have access,
  otherwise data restriction may return unintended results.

- retrieve_system_ids:

  Whether to include systems IDs in the dataset. Currently only applies
  to `Enteric` datamart. Defaults to legacy value "Yes", but can accept
  Boolean, and no longer case-specific.

- disease:

  Optional. Character vector of diseases. Only applicable to some
  datasets.

- surveillance_condition:

  Optional. Character vector of surveillance conditions. Only for
  applicable datasets.

- classification:

  Optional. Character vector of classifications. Only for applicable
  datasets.

- surveillance_region_ha:

  Optional. Character vector of Health Region Authorities where the
  Patient lives/lived. Only for applicable datasets.

- infection_group:

  Optional. Character vector of infection groups. Only for applicable
  datasets.

- ordering_provider_ha:

  Optional. Character vector of Health Region Authorities where the
  Ordering Provide resides. Only for applicable datasets.

- lis_status:

  Optional. Character vector of case level statuses about the LIS data
  in a Case. Only for applicable datasets.

- episode_status:

  Optional. Character vector of episode statuses from the LIS result
  processing and rule engine. Only for applicable datasets.

- test_type:

  Optional. Character vector of the types of tests. Only for applicable
  datasets.

- episode_testing_pattern:

  Optional. Character vector of testing patterns. Only for applicable
  datasets.

- testing_region_ha:

  Optional. Character vector of testing Health Region Authorities. Only
  for applicable datasets.

- case_status:

  Optional. Character vector of case status. Only for applicable
  datasets.

- case_source:

  Optional. Character vector of case status. Only for applicable
  datasets.

- ucd_3_char_code:

  Optional. Character vector of UCD 3-character codes. Only for
  applicable CDI datasets.

- ccd_3_char_code:

  Optional. Character vector of CCD 3-character codes. Only for
  applicable CDI datasets.

- residential_location_ha:

  Optional. Character vector of BC Health Authorities associated with
  decedant's usual residence. Only for applicable CDI datasets.

- death_location_ha:

  Optional. Character vector of BC Health Authorities associated with
  decedant's location of death. Only for applicable CDI datasets.

- mart:

  **\[stable\]**

  Provide an appropriate mart name (non-case specific). Must be one of
  "CDI", "CD", "Respiratory", "Enteric", "STIBBI", and "VPD". Non
  case-sensitive.

- type:

  **\[stable\]**

  Provide an appropriate mart type (non-case specific). Must be one of
  "prod" (default), "su", or "sa". Non case-sensitive. See `Details`.

- .head:

  **\[experimental\]**

  Optional. Single integer vector to indicate how many rows from the top
  to return. Note: `tail` is not supported on database backends.

- .check_params:

  **\[stable\]**

  Can accept Boolean or character values.

  Boolean `TRUE` will return general info of the dataset: For MDX
  queries: dimensions, hierarchies, and levels if possible, and
  hierarchies for default filters. For SQL queries: column names as they
  are in the source tables, and what they are renamed to, and columns
  for default filters.

  On the other hand, user can supply hierarchy or column names retrieved
  from the above as character vector, and a list of the cardinal levels
  will return. Useful to check for typos, or available names to filter
  for.

- .return_query:

  Boolean value. Whether to return query or not.

- .return_data:

  Boolean value. Whether to return data or not.

- .clean_data:

  Boolean value. Whether to attempt cleaning the dates in data or not.

- .query_df:

  **\[experimental\]**

  Accepts a named list of a single element, where names can either be
  `sql` or `olap`, and the element is a `data.frame` object similar in
  structure to `phrdwRdata:::list_query_info$olap`. User is responsible
  for syntax validity and compatibility (ie. OLAP or SQL, appropriate
  access, etc) of the query.

- .query_str:

  **\[experimental\]**

  Accepts a named list of a single element, where names can either be
  `sql` or `olap`, and the element is a `character` vector of query.
  User is responsible for syntax validity and compatibility (ie. OLAP or
  SQL, appropriate access, etc) of the query.

- .cte:

  **\[experimental\]**

  Experimental support for common table expressions (CTEs). Defaults to
  `FALSE`. This is the equivalent of `pipe`, which in essence allows
  writing subqueries in the order in which they are evaluated. Supported
  in
  [`dplyr::show_query()`](https://dplyr.tidyverse.org/reference/explain.html),
  [`dplyr::compute()`](https://dplyr.tidyverse.org/reference/compute.html),
  and
  [`dplyr::collect()`](https://dplyr.tidyverse.org/reference/compute.html).
  This is one of the ways to optimize SQL execution: using CTEs `WITH`
  instead of subqueries.

- ...:

  User can supply named vector: names being the column or hierarchy
  name, and elements of the vector being the value to filter for. See
  `Details`.

## Value

Depending on user input, a `data.frame` or `tibble` or character string.

## Details

**\[superseded\]** List of values to supply `phrdw_datamart`. Case
sensitive.

- `CDI`: Chronic Disease & Injury; links data from Vital Statistics
  death records and census-based socio-economic data.

- `CD Mart`: Communicable Diseases; contains communicable disease public
  health investigation data from the Panorama public health system.

- `Enteric`: Enteric; links data from the Panorama public health system
  and the Sunquest laboratory information system at PHSA.

- `Respiratory`: Respiratory diseases; includes data from the Sunquest
  laboratory information system at PHSA.

- `STIBBI`: Sexually Transmitted Blood Borne Infections; links data from
  the Panorama public health system, the Sunquest laboratory information
  system at PHSA, STIIS, HAISYS, and legacy laboratory systems.

- `VPD`: Vaccine Preventable Disease; links data from the Panorama
  public health system and the Sunquest laboratory information system at
  PHSA.

- `TAT`: TBD.

- `Enteric SU`: UAT server of Enteric.

- `STIBBI SU`: UAT server of STIBBI.

- `STIBBI SA`: PROD copy/Staging server of STIBBI.

- `VPD SU`: UAT server of VPD.

**\[stable\]** Using `mart` and `type` is preferred. They are not
case-sensitive and more readable.

For now, PHRDW data architecture is either data warehouse/relational
table or data cubes, depending on which mart. Connection to data
warehouse returns an
[`odbc::dbConnect()`](https://dbi.r-dbi.org/reference/dbConnect.html)
connection object, whereas connection to data cube returns an
`OLAP_Conn` object, which is just a character string under the hood that
will be executed by a back-end C routine.

This means that connection to data warehouse allows for memory-efficient
tools like `dbplyr` where data is read lazily rather than loaded into
memory.

Users can supply their own connection string using `.conn_str`. To
distinguish between the different architectures, this parameter needs to
be named list, as either `sql` or `cube`. See `Examples`.

## Examples

``` r

if (FALSE) { # \dontrun{

# The bare minimum required user inputs are `mart` and `dataset_name`.
# User input is simpler and not case-sensitive.
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation'
)

# Unlike the legacy approach, which would require more user input.
# User need to be wary of spelling and case.
# User also need to provide dates, even if the intent is to retrieve
# the entire dataset. Without date boundaries, unexpected output may return.
# Dates would need to go beyond what exist in the database, but user would not
# have known this info.
get_phrdw_data(
  phrdw_datamart_connection = connect_to_phrdw(phrdw_datamart = 'CD Mart'),
  phrdw_datamart = 'CD Mart',
  dataset_name = 'Investigation',
  query_start_date = '1900-01-01',
  query_end_date   = Sys.Date()
)

# Incorrect spelling will generate helpful message to assist user.
get_phrdw_data(
  mart = 'dc',
  dataset_name = 'investigation'
)

get_phrdw_data(
  mart = 'stibbi',
  dataset_name = 'investigations'
)

# Filter
## Some arguments are reserved for filters. These remain unchanged from before.
## They are case- and spelling-sensitive.
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation',
  disease = 'Anthrax'
)

get_phrdw_data(
  phrdw_datamart_connection = connect_to_phrdw(phrdw_datamart = 'CD Mart'),
  phrdw_datamart = 'CD Mart',
  dataset_name = 'Investigation',
  query_start_date = '1900-01-01',
  query_end_date   = Sys.Date(),
  disease = 'Anthrax'
)

get_phrdw_data(
  mart = 'stibbi',
  dataset_name = 'investigation',
  disease = 'Chlamydia'
)

get_phrdw_data(
  phrdw_datamart_connection = connect_to_phrdw(phrdw_datamart = 'STIBBI'),
  phrdw_datamart = 'STIBBI',
  dataset_name = 'Investigation',
  query_start_date = '1900-01-01',
  query_end_date   = Sys.Date(),
  disease = 'Chlamydia'
)

# Troubleshooting
## User can opt to return query instead of data.
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation',
  .return_query = T
)

# User can use `.check_params` to examine basic dataset metainfo such as
# column names and what's being used to filter by default.
# Note: the output format may look different depending on data source being
# RDBMS or OLAP (ie. relational DB or data cube).
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation',
  .check_params = T
)

get_phrdw_data(
  mart = 'stibbi',
  dataset_name = 'investigation',
  .check_params = T
)

# When `.check_params` is supplied with the name of the column (or
# hierarchy), it will return (nominal) levels.
# Use with caution: it can return dates and street addresses and that would be
# a long list.
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation',
  .check_params = 'source_system'
)

get_phrdw_data(
  mart = 'stibbi',
  dataset_name = 'investigation',
  .check_params = 'Source System'
)

# Customize filtering (only available in stable usage)
## Knowing columns and levels can be helpful, as these values can be passed
## into the function for customized filtering:
## 1. Enclose the column name with backticks.
## 2. Pass the value to filter.
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation',
  `source_system` = 'EMR'
)

get_phrdw_data(
  mart = 'stibbi',
  dataset_name = 'investigation',
  `Source System` = 'EMR'
)

} # }

```
