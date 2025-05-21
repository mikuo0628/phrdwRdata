# phrdwRdata 1.1.0

* Added `read_ssrs` function to support user's R pipeline retrieving data
  from SSRS reports.
* Added new datasets for various service lines.
* Minor bug fixes in SQL joins when there are duplicate columns from both sides.

# phrdwRdata 1.0.0

* Integrated `olapR`.
* Removed deprecated packages like `RODBCext`.
* Rebuilt handling SQL and MDX queries to supercede legacy functions.
  * Instead of one (or two) scripts per datamart, and one function per dataset,
  query information is tabulated and parsed by a single function (one each
  for RDB and OLAP).
  * This makes the control of information much easier to manage.
  and can be leveraged for helpful error messages.
* Improved call time for SQL calls with `dbplyr` and optimization.
* Included troubleshooting tools.
* Updated documentations.
* Deployed Github IO page.

### Future state

* Finish unit testing.
* Clean up `Legacy` vignette.
* Prepare for phasing out `PHRDW` nomenclature.
* Possiblity to include new tools/helper functions:
  * SSRS interface with `httr2` and `keyring`.
  * Centralize place for other functions?
* [Hex logo?](https://r-pkgs.org/website.html#logo)
