# Changelog

## phrdwRdata 1.5.2

- Fix
  [`read_ssrs()`](https://mikuo0628.github.io/phrdwRdata/reference/read_ssrs.md)’s
  exploration pathway when there are no default values for report’s
  parameters.

## phrdwRdata 1.5.1

- Replace HTTP authentication CURLAUTH_NTLM with CURLAUTH_ANY for more
  robust handshake selection.
  - No longer need to pre-load browser.
  - Automation possible.
- Fix on.exit action: delete only if a tempfile is used.

## phrdwRdata 1.5.0

- Update `TB Client` dataset to use new correct field
  `immigration_arrival_date_accurate_partial_code` replacing
  `year_arrive_in_canada` (rename to `date_arrival_partial_code`).

## phrdwRdata 1.4.0

- Fully functional
  [`read_ssrs()`](https://mikuo0628.github.io/phrdwRdata/reference/read_ssrs.md)
  that can resolve cascading dependents.

## phrdwRdata 1.3.1

- Corrected `do.call` usage.

## phrdwRdata 1.3.0

- Added flexibility in `read_ssrs` for user-specified curl options.

## phrdwRdata 1.2.1

## phrdwRdata 1.2.0

- New feature: create Powershell commands for batch scripting.

## phrdwRdata 1.1.1

- Fixed bug due to mis-join of `surveillance_region_ha` vs
  `health_authority`.
  - This reconciles “col” and “param_name”.
- Fixed bug when pulling identifiers.
- Included a more verbose connection check message, indicating which SQL
  driver is being attempted.
- Added `year_arrived_in_canada` to `TB Client`.

## phrdwRdata 1.1.0

- Added `read_ssrs` function to support user’s R pipeline retrieving
  data from SSRS reports.
- Added new datasets for various service lines.
- Minor bug fixes in SQL joins when there are duplicate columns from
  both sides.

## phrdwRdata 1.0.0

- Integrated `olapR`.
- Removed deprecated packages like `RODBCext`.
- Rebuilt handling SQL and MDX queries to supercede legacy functions.
  - Instead of one (or two) scripts per datamart, and one function per
    dataset, query information is tabulated and parsed by a single
    function (one each for RDB and OLAP).
  - This makes the control of information much easier to manage. and can
    be leveraged for helpful error messages.
- Improved call time for SQL calls with `dbplyr` and optimization.
- Included troubleshooting tools.
- Updated documentations.
- Deployed Github IO page.

#### Future state

- Finish unit testing.
- Clean up `Legacy` vignette.
- Prepare for phasing out `PHRDW` nomenclature.
- Possiblity to include new tools/helper functions:
  - SSRS interface with `httr2` and `keyring`.
  - Centralize place for other functions?
- [Hex logo?](https://r-pkgs.org/website.html#logo)
