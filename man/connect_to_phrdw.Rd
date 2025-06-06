% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/connect_to_phrdw.R
\name{connect_to_phrdw}
\alias{connect_to_phrdw}
\title{Creates a connection object for PHRDW data marts.}
\usage{
connect_to_phrdw(
  phrdw_datamart = NULL,
  mart = NULL,
  type = c("prod", "su", "sa")[1],
  .conn_str = NULL,
  .return_conn_str = F
)
}
\arguments{
\item{phrdw_datamart}{\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#superseded}{\figure{lifecycle-superseded.svg}{options: alt='[Superseded]'}}}{\strong{[Superseded]}}

Legacy mart designations provided by previous package authors.
This backward-compatibility is meant to minimize changes on the user end.
The \code{stable} approach is to reference \code{mart} and \code{type}.}

\item{mart}{\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#stable}{\figure{lifecycle-stable.svg}{options: alt='[Stable]'}}}{\strong{[Stable]}}

Provide an appropriate mart name (non-case specific).
Must be one of "CDI", "CD", "Respiratory", "Enteric", "STIBBI", and "VPD".
Non case-sensitive.}

\item{type}{\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#stable}{\figure{lifecycle-stable.svg}{options: alt='[Stable]'}}}{\strong{[Stable]}}

Provide an appropriate mart type (non-case specific).
Must be one of "prod" (default), "su", or "sa".
Non case-sensitive. See \code{Details}.}

\item{.conn_str}{Defaults to \code{NULL}. For advance usage or testing purposes:
if you are clear on the exact connection parameters,
you can enter here as a named list, where name of
element is \code{cube} or \code{sql}, which will determine the appropriate connection
driver, and the element being the character string containing the specific
parameters. See \code{Details}.}

\item{.return_conn_str}{If \code{TRUE}, will return \code{character} vector instead
of connection objects. For troubleshooting purposes. Defaults to \code{FALSE}.}
}
\value{
By default, an \code{odbc} or \code{OLAP_Conn} connection object that can be
executed with appropriate queries to retrieve views.
If \code{.return_conn_str} is \code{TRUE}, will return \code{character} vector of
connection parameters.
}
\description{
Connect to PHRDW data marts. Depending on the mart, the appropriate driver
and connection parameters will be selected automatically.

For a detailed list of data marts and respective servers, please see
\code{phrdwRdata:::servers}.
}
\details{
\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#superseded}{\figure{lifecycle-superseded.svg}{options: alt='[Superseded]'}}}{\strong{[Superseded]}}
List of values to supply \code{phrdw_datamart}. Case sensitive.
\itemize{
\item \code{CDI}: Chronic Disease & Injury; links data from Vital Statistics
death records and census-based socio-economic data.
\item \verb{CD Mart}: Communicable Diseases; contains communicable disease public
health investigation data from the Panorama public health system.
\item \code{Enteric}: Enteric; links data from the Panorama
public health system and the Sunquest laboratory information
system at PHSA.
\item \code{Respiratory}: Respiratory diseases; includes data from the Sunquest
laboratory information system at PHSA.
\item \code{STIBBI}: Sexually Transmitted Blood Borne Infections; links
data from the Panorama public health system, the Sunquest laboratory
information system at PHSA, STIIS, HAISYS, and legacy laboratory systems.
\item \code{VPD}: Vaccine Preventable Disease; links data from the
Panorama public health system and the Sunquest laboratory information
system at PHSA.
\item \code{TAT}: TBD.
\item \verb{Enteric SU}: UAT server of Enteric.
\item \verb{STIBBI SU}: UAT server of STIBBI.
\item \verb{STIBBI SA}: PROD copy/Staging server of STIBBI.
\item \verb{VPD SU}: UAT server of VPD.
}

\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#stable}{\figure{lifecycle-stable.svg}{options: alt='[Stable]'}}}{\strong{[Stable]}}
Using \code{mart} and \code{type} is preferred. They are not case-sensitive and
more readable.

For now, PHRDW data architecture is either data warehouse/relational
table or  data cubes, depending on which mart. Connection to data
warehouse returns an \code{\link[odbc:dbConnect-OdbcDriver-method]{odbc::dbConnect()}} connection object, whereas
connection to data cube returns an \code{OLAP_Conn} object, which is
just a character string under the hood that will be executed by
a back-end C routine.

This means that connection to data warehouse allows for memory-efficient
tools like \code{dbplyr} where data is read lazily rather than loaded
into memory.

Users can supply their own connection string using \code{.conn_str}. To
distinguish between the different architectures, this parameter needs to
be named list, as either \code{sql} or \code{cube}. See \code{Examples}.
}
\examples{
\dontrun{

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
        "Data Source=SPRSASBI001.phsabc.ehcnet.ca\\\\PRISASBIM",
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
          "server=SPRDBSBI003.phsabc.ehcnet.ca\\\\PRIDBSBIEDW",
          "database=SPEDW",
          sep = ';'
        )
    )
)

}


}
