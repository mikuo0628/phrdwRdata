#' Creates a connection object for PHRDW data marts.
#'
#' @description
#' Connect to PHRDW data marts. Depending on the mart, the appropriate driver
#'   and connection parameters will be selected automatically.
#'
#' For a detailed list of data marts and respective servers, please see
#'   `phrdwRdata:::servers`.
#'
#' @details
#' `r lifecycle::badge('superseded')`
#' List of values to supply `phrdw_datamart`. Case sensitive.
#' * `CDI`: Chronic Disease & Injury; links data from Vital Statistics
#'   death records and census-based socio-economic data.
#' * `CD Mart`: Communicable Diseases; contains communicable disease public
#'   health investigation data from the Panorama public health system.
#' * `Enteric`: Enteric; links data from the Panorama
#'   public health system and the Sunquest laboratory information
#'   system at PHSA.
#' * `Respiratory`: Respiratory diseases; includes data from the Sunquest
#'   laboratory information system at PHSA.
#' * `STIBBI`: Sexually Transmitted Blood Borne Infections; links
#'   data from the Panorama public health system, the Sunquest laboratory
#'   information system at PHSA, STIIS, HAISYS, and legacy laboratory systems.
#' * `VPD`: Vaccine Preventable Disease; links data from the
#'   Panorama public health system and the Sunquest laboratory information
#'   system at PHSA.
#' * `TAT`: TBD.
#' * `Enteric SU`: UAT server of Enteric.
#' * `STIBBI SU`: UAT server of STIBBI.
#' * `STIBBI SA`: PROD copy/Staging server of STIBBI.
#' * `VPD SU`: UAT server of VPD.
#'
#' `r lifecycle::badge('stable')`
#' Using `mart` and `type` is preferred. They are not case-sensitive and
#'   more readable.
#'
#' For now, PHRDW data architecture is either data warehouse/relational
#' table or  data cubes, depending on which mart. Connection to data
#' warehouse returns an [odbc::dbConnect()] connection object, whereas
#' connection to data cube returns an `OLAP_Conn` object, which is
#' just a character string under the hood that will be executed by
#' a back-end C routine.
#'
#' This means that connection to data warehouse allows for memory-efficient
#' tools like `dbplyr` where data is read lazily rather than loaded
#' into memory.
#'
#' Users can supply their own connection string using `.conn_str`. To
#' distinguish between the different architectures, this parameter needs to
#' be named list, as either `sql` or `cube`. See `Examples`.
#'
#' @param phrdw_datamart `r lifecycle::badge('superseded')`
#'
#'   Legacy mart designations provided by previous package authors.
#'   This backward-compatibility is meant to minimize changes on the user end.
#'   The `stable` approach is to reference `mart` and `type`.
#'
#' @param mart `r lifecycle::badge('stable')`
#'
#'   Provide an appropriate mart name (non-case specific).
#'   Must be one of "CDI", "CD", "Respiratory", "Enteric", "STIBBI", and "VPD".
#'   Non case-sensitive.
#'
#' @param type `r lifecycle::badge('stable')`
#'
#'   Provide an appropriate mart type (non-case specific).
#'   Must be one of "prod" (default), "su", or "sa".
#'   Non case-sensitive. See `Details`.
#'
#' @param .conn_str Defaults to `NULL`. For advance usage or testing purposes:
#'   if you are clear on the exact connection parameters,
#'   you can enter here as a named list, where name of
#'   element is `cube` or `sql`, which will determine the appropriate connection
#'   driver, and the element being the character string containing the specific
#'   parameters. See `Details`.
#'
#' @param .return_conn_str If `TRUE`, will return `character` vector instead
#'   of connection objects. For troubleshooting purposes. Defaults to `FALSE`.
#'
#' @return By default, an `odbc` or `OLAP_Conn` connection object that can be
#'   executed with appropriate queries to retrieve views.
#'   If `.return_conn_str` is `TRUE`, will return `character` vector of
#'   connection parameters.
#'
#' @export
#'
#' @examples
#' \dontrun{
#'
#' library(phrdwRdata)
#'
#' # Legacy ------------------------------------------------------------------
#' phrdw_datamart <- 'CD Mart'
#' phrdw_datamart_connection <- connect_to_phrdw(phrdw_datamart)
#'
#'
#' phrdw_datamart <- 'STIBBI'
#' phrdw_datamart_connection <- connect_to_phrdw(phrdw_datamart)
#'
#' # Stable ------------------------------------------------------------------
#'
#' connect_to_phrdw(mart = 'stibbi')
#' connect_to_phrdw(mart = 'stibbi', type = 'su')
#' connect_to_phrdw(mart = 'stibbi', type = 'su', .return_conn_str = T)
#'
#' # Connect to STIBBI cube with connection string
#' conn_str_cube <-
#'   list(
#'     cube =
#'       paste(
#'         "Data Source=SPRSASBI001.phsabc.ehcnet.ca\\PRISASBIM",
#'         "Initial catalog=PHRDW_STIBBI",
#'         "Provider=MSOLAP",
#'         "Packet Size=32767",
#'         sep = ';'
#'       )
#'   )
#' connect_to_phrdw(.conn_str = conn_str_cube)
#'
#' # Connect to CD mart with connection string
#' connect_to_phrdw(
#'   .conn_str =
#'     list(
#'       sql =
#'         paste(
#'           "driver={SQL Server}",
#'           "server=SPRDBSBI003.phsabc.ehcnet.ca\\PRIDBSBIEDW",
#'           "database=SPEDW",
#'           sep = ';'
#'         )
#'     )
#' )
#'
#' }
#'
#'
connect_to_phrdw <- function(
    phrdw_datamart   = NULL,
    mart             = NULL,
    type             = c('prod', 'su', 'sa')[1],
    .conn_str        = NULL,
    .return_conn_str = F
) {

  # prioritize `.conn_str` if provided
  if (!is.null(.conn_str)) {

    if (!tolower(names(.conn_str)) %in% c('sql', 'cube')) {

      stop(
      paste(
        '\n',
        'Connection driver cannot be determined:\n',
        'Please ensure `.conn_str` parameter is a correctly named list.',
        collapse = '\n',
        sep = ''
      )
      )

    }

    if (tolower(names(.conn_str)) == 'cube') {

      conn <- try(OlapConnection(.conn_str$cube), silent = T)

    } else {

      conn <-
        try(
          odbc::dbConnect(
            drv = odbc::odbc(),
            .connection_string = .conn_str$sql
          ),
          silent = T
        )

    }

    if (inherits(conn, 'try-error')) {

      cat('--- CD Mart connection failed ---\n')
      cat('Check connection string', '\n')

      stop(conn, call. = F)

    }

    return(conn)

  }

  # legacy
  select_phrdw_datamart <- force(phrdw_datamart)

  # stable
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

          dplyr::filter(
            .,
            tolower(.data$phrdw_datamart) == tolower(select_phrdw_datamart)
          )

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

        conn <- NULL
        i    <- 1

        while (inherits(conn, c('try-error', 'NULL')) & i <= nrow(server)) {

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
            try(
              odbc::dbConnect(
                drv = odbc::odbc(),
                .connection_string = conn_str
              ),
              silent = T
            )

        }

        list(
          conn = conn,
          conn_str = conn_str
        )

      }

    if (inherits(conn_objs$conn, 'try-error')) {

      cat('--- CD Mart connection failed ---\n')
      cat('Check connection:\n')
      cat(conn_objs$conn_str, '\n')
      stop(conn_objs$conn, call. = F)

    }

    if (.return_conn_str) return(conn_objs$conn_str)

    cat(paste('--- Connection to CD ---\n'))
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

          # conn_obj <- OlapConnection(conn_str)

          return(conn_str)

        }
      )

    if (.return_conn_str) return(conn_str)

    conn <- OlapConnection(conn_str)

    if (length(conn) > 1) stop('--- Please check OLAP connection params ---\n')

    cat(paste('--- Connection to', server$initial_catalog, '---\n'))

    return(conn)

  }

  # using mart and type should be encouraged
  if (all(is.null(server_params$mart))) {

    stop('--- Please enter the approrpiate datamart name (and type) ---\n')

  }

}

