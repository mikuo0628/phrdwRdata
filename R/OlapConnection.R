#' Constructs an "OlapConnection" object: string of connection parameters that
#' can be executed with MDX queries using C routines for efficiency.
#'
#' @description
#' Checks if connection string contains all the necessary components to connect
#' to OLAP.
#'
#' @param conn_str Connection string that requires `Initial Catalog`, which is
#' the name of the data mart, `Data Source`, `Provider=MSOLAP`, and
#' `Packet Size=32767` for PHRDW OLAP server.
#'
#' @return A character string with class `OLAP_Conn`.
#' @export
#'
OlapConnection <- function(
    conn_str = "Data Source=localhost; Provider=MSOLAP;"
) {

  if (!is.character(conn_str) || is.null(conn_str)) {

    stop('`conn_str` must be passed as a string')

  }

  if (!any(grep('Provider\\s*=\\s*MSOLAP;', conn_str))) {

    warning('`conn_str` must contain "Provider=MSOLAP;"')

  }

  if (!any(grep('Data Source\\s*=\\s*[^;]*;', conn_str))) {

    warning('`conn_str` must contain "Data Source=[a data source];"')

  }

  ocs <- list(cnn = conn_str)

  class(ocs) <- append(class(ocs), 'OLAP_Conn')

  return(ocs)

}


#' Simple helper function to print `OLAP_conn` object.
#'
#' @param mdx_qry
#'
print.OlapConnection <- function(mdx_qry) { print(mdx_qry$cnn) }

