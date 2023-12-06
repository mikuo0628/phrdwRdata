#' Connect to PHRDW data marts. Depending on the mart, the correct driver will
#' be selected and connection parameters will be populated automatically.
#'
#' For a detailed list of data marts and respective servers, please see
#' `phrdwRdata::servers`.
#'
#' For developers, connections can be expanded by expanding
#' `phrdwRdata::servers`.
#'
#' List of data marts that can be connected to using `phrdw_datamart` and their
#' full name:
#' \itemize{
#'    \item CD: Communicable Diseases
#'    \item CDI: Chronic Disease & Injury
#'    \item PHRDW_Enteric_Panorama:
#'    \item SU_PHRDW_Enteric_Panorama:
#'    \item PHRDW_STIBBI:
#'    \item SU_PHRDW_STIBBI:
#'    \item SA_PHRDW_STIBBI:
#'    \item PHRDW_VPD:
#'    \item SU_PHRDW_VPD:
#' }
#'
#' Alternatively, use a combination of the below:
#' List 0f marts:
#' \itemize{
#'    \item CD:
#'    \item CDI:
#'    \item Enteric:
#'    \item STIBBI:
#'    \item VPD:
#' }
#'
#' list of mart types:
#' \itemize{
#'    \item prod: production
#'    \item su: UAT
#'    \item sa: Staging
#' }
#'
#' @param phrdw_datamart `r lifecycle::badge('superceded)`
#' @param server_params
#'
#' @return An `RODBC` or `OLAP_Conn` connection object that can be
#' executed with appropriate queries to retrieve views.
#' @export
#'
#' @examples connect_to_phrdw(phrdw_datamart = 'CD', type = 'prod')
connect_to_phrdw <- function(
    phrdw_datamart = NULL,
    server_params  = list(mart = NULL, type = NULL)
    # connection     = NULL
) {

  select_phrdw_datamart <- force(phrdw_datamart)

  # prioritize `connection` if provided
  # if (is.null(select_phrdw_datamart) & !is.null(connection)) {
  #
  #   return(
  #     RODBC::odbcDriverConnect(connection = connection)
  #   )
  #
  # }

  if (!exists('server') || is.null(server)) server <- servers

  # handling CD ie SQL tables
  if (
    any(
      stringr::str_detect(
        c(select_phrdw_datamart, server_params$mart), '^CD($| Mart)'
      )
    )
  ) {

    conn <-
      server$sql %>%
      {

        if (!is.null(select_phrdw_datamart)) {

          dplyr::filter(., .data$phrdw_datamart == select_phrdw_datamart)

        } else {

          dplyr::filter(
            .,
            .data$mart == server_params$mart,
            .data$type == server_params$type,
          )

        }

      } %>%
      {

        server <- .

        conn <- -1
        i    <-  1

        while (conn == -1 & i <= nrow(server)) {

          conn_str <-
            paste(
              paste(
                c('driver', 'server', 'database'),
                c(
                  as.character(server[i, ]$driver),
                  server[i, ]$server,
                  server[i, ]$database
                ),
                sep = '=',
                collapse = ';'
              )
            )

          i <- i + 1

          conn <- RODBC::odbcDriverConnect(connection = conn_str)

          conn

        }

        conn

      }

    if (conn == -1) stop('---CD Mart connection failed---')

    return(conn)

  } else if ( # other data marts ie. non SQL
    any(!is.null(select_phrdw_datamart), !is.null(server_params$mart))
  ) {

    lookup_marts <- c(select_phrdw_datamart, server_params$mart)

    server <-
      server$olap %>%
      # use `filter_at` for backward compatibility with R 3.5
      dplyr::filter_at(
        dplyr::vars(tidyselect::matches('mart')),
        dplyr::any_vars(. %in% lookup_marts)
      ) %>%
      {

        if (nrow(.) == 0) stop('---Datamart or type not found---')
        if (nrow(.) == 1) {

          .

        } else if (nrow(.) > 1 & !is.null(server_params$type)) {

          dplyr::filter(., .data$type == server_params$type)

        } else if (nrow(.) > 1) {

          # choose prod by default
          dplyr::filter(., .data$type == 'prod')

        } else {

          stop('Please specified mart type in `server_params`.')

        }

      }

    conn <-
      server %>%
      dplyr::select(-matches('mart|type')) %>%
      purrr::pmap(
        function(initial_catalog, data_source, provider, packet_size) {

          conn_str <-
            paste(
              c('Data Source', 'Initial catalog', 'Provider', 'Packet Size'),
              c( data_source,   initial_catalog,   provider,   packet_size ),
              sep = '=',
              collapse = ';'
            )

          conn_obj <- phrdwRdata::OlapConnection(conn_str)

          return(conn_obj)

        }
      )

    if (length(conn) > 1) stop('---Please check OLAP connection params---')

    cat(
      paste(
        '---Connection to', server$initial_catalog, 'is ready---\n'
      )
    )

    return(conn[[1]])

  }

  # using mart and type should be encouraged
  if (all(is.null(select_mart), is.null(server_params$type))) {

    stop('---Please enter the approrpiate datamart name and type---')

  }

}

#' Closes ODBC connection to PHRDW data marts.
#'
#' @param phrdw_conn
#'
#' @return
#' @export
#'
#' @examples
close_connection_to_phrdw <- function(phrdw_conn) {

  if (inherits(phrdw_conn, 'RODBC')) RODBC::odbcClose(phrdw_conn)

}

