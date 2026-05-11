# Creates a connection object for PHRDW data marts.

Connect to PHRDW data marts. Depending on the mart, the appropriate
driver and connection parameters will be selected automatically.

For a detailed list of data marts and respective servers, please see
`phrdwRdata:::servers`.

## Usage

``` r
connect_to_phrdw(
  phrdw_datamart = NULL,
  mart = NULL,
  type = c("prod", "su", "sa")[1],
  .conn_str = NULL,
  .return_conn_str = F
)
```

## Arguments

- phrdw_datamart:

  **\[superseded\]**

  Legacy mart designations provided by previous package authors. This
  backward-compatibility is meant to minimize changes on the user end.
  The `stable` approach is to reference `mart` and `type`.

- mart:

  **\[stable\]**

  Provide an appropriate mart name (non-case specific). Must be one of
  "CDI", "CD", "Respiratory", "Enteric", "STIBBI", and "VPD". Non
  case-sensitive.

- type:

  **\[stable\]**

  Provide an appropriate mart type (non-case specific). Must be one of
  "prod" (default), "su", or "sa". Non case-sensitive. See `Details`.

- .conn_str:

  Defaults to `NULL`. For advance usage or testing purposes: if you are
  clear on the exact connection parameters, you can enter here as a
  named list, where name of element is `cube` or `sql`, which will
  determine the appropriate connection driver, and the element being the
  character string containing the specific parameters. See `Details`.

- .return_conn_str:

  If `TRUE`, will return `character` vector instead of connection
  objects. For troubleshooting purposes. Defaults to `FALSE`.

## Value

By default, an `odbc` or `OLAP_Conn` connection object that can be
executed with appropriate queries to retrieve views. If
`.return_conn_str` is `TRUE`, will return `character` vector of
connection parameters.

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

library(phrdwRdata)

# Legacy ------------------------------------------------------------------
phrdw_datamart <- 'CD Mart'
phrdw_datamart_connection <- connect_to_phrdw(phrdw_datamart)


phrdw_datamart <- 'STIBBI'
phrdw_datamart_connection <- connect_to_phrdw(phrdw_datamart)

# Stable ------------------------------------------------------------------

connect_to_phrdw(mart = 'stibbi')
connect_to_phrdw(mart = 'stibbi', type = 'su')
connect_to_phrdw(mart = 'stibbi', type = 'su', .return_conn_str = T)

# Connect to STIBBI cube with connection string
conn_str_cube <-
  list(
    cube =
      paste(
        "Data Source=SPRSASBI001.phsabc.ehcnet.ca\\PRISASBIM",
        "Initial catalog=PHRDW_STIBBI",
        "Provider=MSOLAP",
        "Packet Size=32767",
        sep = ';'
      )
  )
connect_to_phrdw(.conn_str = conn_str_cube)

# Connect to CD mart with connection string
connect_to_phrdw(
  .conn_str =
    list(
      sql =
        paste(
          "driver={SQL Server}",
          "server=SPRDBSBI003.phsabc.ehcnet.ca\\PRIDBSBIEDW",
          "database=SPEDW",
          sep = ';'
        )
    )
)

} # }
```
