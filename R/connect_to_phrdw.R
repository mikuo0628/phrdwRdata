#' Creates a connection object for PHRDW datamart based on user input
#'
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
#'    \item Respiratory:
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
#' @param phrdw_datamart `r lifecycle::badge('superseded')` Original mart
#' designations provided by previous package authors. This backward-
#' compatibility is meant to minimize changes on the user end.
#' @param mart Provide an appropriate mart name (non-case specific).
#' See `Details`.
#' @param type Provide an appropriate mart type (non-case specific).
#' @param .conn_str Defaults to `NULL`. For advance usage or testing purposes:
#'  if you are clear on the exact connection parameters,
#'  you can enter here as a named list, where name of
#' element is `cube` or `sql`, which will determine the appropriate connection
#' driver, and the element being the character string containing the specific
#' parameters.
#' See `Details`.
#' @param .return_conn_str
#'
#' @return An `odbc` or `OLAP_Conn` connection object that can be
#' executed with appropriate queries to retrieve views.
#' @export
#'
#' @examples connect_to_phrdw('STIBBI')
#' connect_to_phrdw('Respiratory')
#' connect_to_phrdw(mart = 'STIBBI', type = 'prod')
connect_to_phrdw <- function(
    phrdw_datamart   = NULL,
    mart             = NULL,
    type             = NULL,
    .conn_str        = NULL,
    .return_conn_str = F
) {

  # prioritize `connection` if provided
  if (!is.null(.conn_str)) {

    if (!tolower(names(.conn_str)) %in% c('sql', 'cube')) {

      stop(
        paste(
          '\n',
          'Connection driver cannot be determined:\n',
          'Please ensure `connection` parameter is a correctly named `list()`.',
          collapse = '\n',
          sep = ''
        )
      )

    }

    if (tolower(names(.conn_str)) == 'cube') {

      conn <- phrdwRdata::OlapConnection(.conn_str$cube)

    } else {

      conn <-
        odbc::dbConnect(
          drv = odbc::odbc(),
          .connection_string = .conn_str$sql
        )

    }

    return(conn)

  }

  select_phrdw_datamart <- force(phrdw_datamart)

  server_params <- list(mart = mart, type = type)

  if (!exists('server') || is.null(server)) { server <- servers }

  # handling CD ie SQL tables
  if (
    any(
      stringr::str_detect(
        c(select_phrdw_datamart, server_params$mart),
        stringr::regex('^CD($| Mart)', ignore_case = T)
      )
    )
  ) {

    conn_objs <-
      server$sql %>%
      {

        if (!is.null(select_phrdw_datamart)) {

          dplyr::filter(., .data$phrdw_datamart == select_phrdw_datamart)

        } else {

          dplyr::filter(
            .,
            tolower(.data$mart) == tolower(server_params$mart),
            tolower(.data$type) == ifelse(is.null(server_params$type),
                                          'prod',
                                          tolower(server_params$type))
          )

        }

      } %>%
      {

        server <- .

        conn <- -1
        i    <-  1

        while (identical(conn, -1) & i <= nrow(server)) {

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

          # option 1: RODBC
          # conn <- RODBC::odbcDriverConnect(connection = conn_str)

          # option 2: odbc
          conn <-
            odbc::dbConnect(
              drv = odbc::odbc(),
              .connection_string = conn_str
            )

          # list(
          #   conn = conn,
          #   conn_str = conn_str
          # )

        }

        list(
          conn = conn,
          conn_str = conn_str
        )

      }

    if (.return_conn_str) return(conn_objs$conn_str)

    if (identical(conn_objs$conn, -1)) {

      stop('--- CD Mart connection failed ---\n')

    }

    return(conn_objs$conn)

  } else if ( # other data marts ie. non SQL
    any(!is.null(select_phrdw_datamart), !is.null(server_params$mart))
  ) {

    lookup_marts <- tolower(c(select_phrdw_datamart, server_params$mart))

    server <-
      server$olap %>%
      # use `filter_at` for backward compatibility with R 3.5
      dplyr::filter_at(
        dplyr::vars(dplyr::matches('mart')),
        dplyr::any_vars(tolower(.) %in% lookup_marts)
      ) %>%
      {

        if (nrow(.) == 0) stop('--- Datamart or type not found ---\n')

        if (nrow(.) == 1) {

          .

        } else if (nrow(.) > 1) {

          dplyr::filter(
            .,
            tolower(.data$type) == ifelse(is.null(server_params$type),
                                          'prod',
                                          tolower(server_params$type))
          )

        } else { stop('Please specified mart type in `server_params`.') }

      }

    conn_str <-
      server %>%
      dplyr::select(-dplyr::matches('mart|type')) %>%
      purrr::pmap_chr(
        function(initial_catalog, data_source, provider, packet_size) {

          conn_str <-
            paste(
              c('Data Source', 'Initial catalog', 'Provider', 'Packet Size'),
              c( data_source,   initial_catalog,   provider,   packet_size ),
              sep = '=',
              collapse = ';'
            )

          # conn_obj <- phrdwRdata::OlapConnection(conn_str)

          return(conn_str)

        }
      )

    if (.return_conn_str) return(conn_str)

    conn <- phrdwRdata::OlapConnection(conn_str)

    if (length(conn) > 1) stop('--- Please check OLAP connection params ---\n')

    cat(paste('--- Connection to', server$initial_catalog, '---\n'))

    return(conn)

  }

  # using mart and type should be encouraged
  if (all(is.null(server_params$mart), is.null(server_params$type))) {

    stop('--- Please enter the approrpiate datamart name (and type) ---\n')

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

