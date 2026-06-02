# Retrieves data from SSRS URLs.

`read_ssrs` provides a high-level interface to SQL Server Reporting
Services (SSRS). It handles the complex lifecycle of an SSRS request:
discovering report metadata via GUID, resolving cascading (dependent)
parameters, and rendering the final output via the ReportServer engine.

## Usage

``` r
read_ssrs(
  url = "",
  ...,
  username = NULL,
  reset_pw = FALSE,
  format = c("CSV")[1],
  .explore = list(F, "default", "valid")[[1]],
  .check_params = TRUE,
  .resolve_dependents = FALSE,
  .skip = 0L,
  .in_memory = TRUE,
  .col_types = NULL,
  .req_options = list()
)
```

## Arguments

- url:

  Character. The full SSRS portal URL (e.g.,
  `https://reports.phsa.ca/reports/report/...`).

- ...:

  SSRS report filters. You can use the human-readable labels found in
  the web UI; the function will automatically map these to the technical
  MDX strings required by the back end.

- username:

  Character. Your PHSA/Network user ID. If NULL, uses current Windows
  session credentials via NTLM/Negotiate.

- reset_pw:

  Logical. If `TRUE`, will invoke and attempt to delete password
  associated with `username`.

- format:

  Character. The output format. Defaults to "CSV". For possible future
  expansion.

- .explore:

  Logical. If `TRUE`, will print useful information for user input, and
  return invisibly a `data.frame` of the full detail.

- .check_params:

  Logical. In SSRS UI, the prompt is meant for user readability (ie. The
  values in the parameters may not reflect the actual values to be
  filtered in the backend). Defaults to `TRUE`, where user can supply
  values seen in the UI, and the function will convert them to the
  backend-appropriate values. Note: this makes an extra call to the
  system to retrieve value mapping table.

- .resolve_dependents:

  Logical. If `TRUE`, the function will query the server to resolve
  cascading dependencies. This is necessary when one filter (e.g.,
  Health Authority) restricts the valid values of another (e.g.,
  Community). Note: this makes two extra calls to the system to retrieve
  value mapping table – once to retrieve the default mapping table, and
  second to retrieve the mapping table in accordance to user input.

- .skip:

  Integer. Number of lines to skip at the top of the CSV (e.g., for
  reports with headers/metadata).

- .in_memory:

  Logical or Character. If `TRUE`, processes data in RAM. If `FALSE` or
  a file path, streams the download to disk to handle large datasets.

- .col_types:

  One of `NULL`, a `readr::cols()` specification, or a string. Controls
  how the downloaded columns are parsed.

  - If `NULL` (default), column types are guessed based on the first
    1,000 rows.

  - To keep all columns as character data (recommended for manual
    cleaning), use `readr::cols(.default = "c")`.

  - See `readr::read_csv()` for full details on supported formats.

  It is highly recommended that users read data as `character` type and
  perform explicit cleaning actions instead of relying on heuristics.

- .req_options:

  List. Additional curl options passed to
  [`httr2::req_options`](https://httr2.r-lib.org/reference/req_options.html).

## Value

A `tibble` containing the report data, or a metadata `data.frame` if
`.explore` is active.

## Details

The function operates in three primary phases:

1.  **Discovery**: Converts the human-readable portal URL into a unique
    System GUID using the SSRS REST API.

2.  **Resolution**: If `.resolve_dependents = TRUE`, it calls the
    `Model.GetParameters` bound action. This server-side logic ensures
    that if you select a "Disease", the "Serotypes" are automatically
    filtered and filled in the background.

3.  **Execution**: Sends the final payload as
    `application/x-www-form-urlencoded` via a POST request to the
    ReportServer. This avoids the "URL too long" errors common with
    large MDX parameter sets.

It provides an interface to pull data into R environment by leveraging
the following packages/tools:

- `keyring`: handles user credential elegantly.

- `httr2`: handles HTTP requests and responses following Microsoft
  documentation on REST APIs for Reporting Services
  (<https://learn.microsoft.com/en-us/sql/reporting-services/developer/rest-api?view=sql-server-ver16>).

There are some helper parameters to assist users with the report's
built-in filters and output formats. However, how the report is set up
may be very different from one to another. Please always double check to
ensure what you get is what you intended.

The helper to determine filters is `.explore`. If you set it to `TRUE`,
you may get something similar to the following message printed in your
console:

    Report name: XYZ
    Report path: /bccdc/XYZ
    Default User Input (showing only 1):

      SurveillanceReportedStartDate : 2026-05-04T00:00:00.0000000
      SurveillanceReportedEndDate   : 2026-05-11T00:00:00.0000000

To use filters in this function, simply refer to what's printed above,
and add them as part of the function:

    read_ssrs(
      url             = YOUR_SSRS_URL,
      SurveillanceReportedStartDate = "1/1/2025",
      SurveillanceReportedEndDate   = "2/1/2025"
    )

Your user credential, if provided, is managed by `keyring` package. This
prevents you from entering your credentials in the console or saving it
in the script, which are both not ideal practices for security.

`keyring` will leverage your operating system's credential manager to
handle your saved credential.

## Author

[Brendan Bakos](mailto:brendan.bakos@vch.ca) contributed the
implementation of the crucial authentication/negotiation, and is
instrumental in the design of the API handling.
