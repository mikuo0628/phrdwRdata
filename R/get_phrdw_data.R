#' Get PHRDW data.
#'
#' @description
#'
#' @inherit connect_to_phrdw Details
#'
#' @param phrdw_datamart_connection `r lifecycle::badge('superseded')` Legacy
#' function design: supply a connection object created by [connect_to_phrdw].
#' Recommend using `mart` and `type` instead for flexibility
#' (see [connect_to_phrdw]). The function takes care of connecting
#' to the appropriate PHRDW database and disconnect after performing the
#' requested data filtering and retrieving.
#' @param dataset_name The name of the pre-built dataset to retrieve.
#' @param query_start_date
#' @param query_end_date
#' @param include_patient_identifiers
#' @param include_indigenous_identifiers
#' @param retrieve_system_ids
#' @param disease
#' @param surveillance_condition
#' @param classification
#' @param surveillance_region_ha
#' @param infection_group
#' @param ordering_provider_ha
#' @param lis_status
#' @param episode_status
#' @param test_type
#' @param episode_testing_pattern
#' @param testing_region_ha
#' @param case_status
#' @param case_source
#' @param ucd_3_char_code
#' @param ccd_3_char_code
#' @param residential_location_ha
#' @param death_location_ha
#' @param .check_params
#' @param .return_query
#' @param .return_data
#' @param .clean_data
#' @param .query_info
#' @inheritParams connect_to_phrdw
#'
#' @return
#' @export
#'
#' @examples
get_phrdw_data <- function(
    phrdw_datamart_connection      = NULL,
    phrdw_datamart                 = NULL,
    dataset_name                   = NULL,

    query_start_date               = NULL,
    query_end_date                 = NULL,
    include_patient_identifiers    = F,
    include_indigenous_identifiers = F,
    retrieve_system_ids            = 'Yes',

    # Legacy: general
    disease                        = NULL,
    surveillance_condition         = NULL,
    classification                 = NULL,
    surveillance_region_ha         = NULL,
    infection_group                = NULL,
    ordering_provider_ha           = NULL,
    lis_status                     = NULL,
    episode_status                 = NULL,
    test_type                      = NULL,
    episode_testing_pattern        = NULL,
    testing_region_ha              = NULL,
    case_status                    = NULL,
    case_source                    = NULL,
    # Legacy: CDI specific
    ucd_3_char_code                = NULL,
    ccd_3_char_code                = NULL,
    residential_location_ha        = NULL,
    death_location_ha              = NULL,

    mart                           = NULL,
    type                           = c('prod', 'su', 'sa')[1],
    # user options
    .check_params                  = F,
    .return_query                  = F,
    .return_data                   = !.return_query,
    .clean_data                    = F,
    .query_info                    = NULL,
    ...
) {

  # Basic checks
  if (is.null(dataset_name)) stop('Please supply `dataset_name`.')
  if (is.null(c(phrdw_datamart, mart))) {

    stop('Please supply either `phrdw_datamart` or `mart`.')

  }

  default_params <- as.list(environment())
  user_params    <- rlang::list2(...)

  # Data source
  data_source <-
    {

      if (!is.null(phrdw_datamart_connection)) {

        stringr::str_extract(class(phrdw_datamart_connection), 'SQL|OLAP') %>%
          purrr::discard(is.na)

      } else if (!is.null(mart)) {

        mart_index <-
          which(tolower(names(available_prebuilt_datasets)) == tolower(mart))

        purrr::map(servers, dplyr::pull, mart) %>%
          purrr::map(unique) %>%
          purrr::map_lgl(~ tolower(mart) %in% tolower(.x)) %>%
          purrr::discard(isFALSE) %>%
          names()

      }

    } %>%
    tolower()

  .query_info <- if (is.null(.query_info)) list_query_info[[data_source]]

  # Optional checks: check user inputs
  if (isTRUE(.check_params)) {

    if (!is.null(phrdw_datamart)) {

      phrdw_datamarts <-
        unique(unlist(purrr::map(servers, pluck, 'phrdw_datamart')))

      mart <-
        servers %>%
        purrr::map(filter, .data$phrdw_datamart == .env$phrdw_datamart) %>%
        purrr::discard(~ nrow(.x) == 0) %>%
        purrr::pluck(1, 'mart')

      if (!phrdw_datamart %in% phrdw_datamarts) {

        stop(
          paste0(
            'Please check `phrdw_datamart` spelling.\n',
            'It should be one of the following ',
            '(case-sensitive):\n\n',
            paste('  -', phrdw_datamarts, collapse = '\n')
          ),
          call. = F
        )

      }

    } else if (!is.null(mart)) {

      if (identical(mart_index, integer(0))) {

        stop(
          paste0(
            'Please check `mart` spelling.\n',
            'It should be one of the following ',
            '(non-case-sensitive):\n\n',
            paste('  -', names(available_prebuilt_datasets), collapse = '\n')
          ),
          call. = F
        )

      }

    }

    if (!is.null(dataset_name)) {

      prebuilt_datasets <- available_prebuilt_datasets[[mart_index]]

      if (!tolower(dataset_name) %in% tolower(prebuilt_datasets)) {

        stop(
          paste0(
            'Please check `dataset_name` spelling.\n',
            'It should be one of the following ',
            '(case-sensitive if legacy):\n\n',
            paste('  -', prebuilt_datasets, collapse = '\n')
          ),
          call. = F
        )

      }

    }

  }

  # Legacy method: requires phrdw_datamart_connection
  if (!is.null(phrdw_datamart_connection)) {

    if (is.null(phrdw_datamart)) stop('Please supply `phrdw_datamart`.')

    if (stringr::str_detect(tolower(phrdw_datamart), 'cdi')) {

      return(
        do.call(
          what = get_phrdw_cdi_data,
          args =
            default_params %>%
            purrr::keep(
              .p = names(.) %in% formalArgs(get_phrdw_cdi_data)
            ) %>%
            purrr::modify_if(.p = is.null, .f = ~ '')
        )
      )

    } else {

      return(
        do.call(
          what = get_phrdw_data_legacy,
          args =
            default_params %>%
            purrr::keep(
              .p = names(.) %in% formalArgs(get_phrdw_data_legacy)
            ) %>%
            purrr::modify_if(.p = is.null, .f = ~ '') #%>%
            # purrr::modify_if(.p = names(.) == 'retrieve_system_ids', .f = ~ 'Yes')
        )
      )

    }

  }

  # New method: use `mart` and `type` instead
  # no need phrdw_datamart_connection
  if (is.null(mart)) stop('Please supply `mart`.')

  if (data_source == 'sql') {

    # execute pre-build dataset_name specific queries
    if (tolower(dataset_name) == 'investigation') {

      # DO THIS
      # cd_investigation_query(mart, type, param_list)

    }

  } else if (data_source == 'olap') {

    if (is.character(.check_params)) {

      cubes <- explore(connect_to_phrdw(mart = mart, type = type))

      dim <-
        dplyr::filter(.query_info, .data$attr_hier == .check_params)$dim

      levels <-
        purrr::imap(
          rlang::set_names(cubes),
          ~ explore(
            connect_to_phrdw(mart = mart, type = type),
            .x,
            dim,
            .check_params,
            .check_params
          )
        )

      return(
        purrr::iwalk(
          levels,
          ~ cat(
            '\n',
            stringr::str_pad('Cube:',      13, 'right', ' '), .y, '\n',
            stringr::str_pad('Dimension:', 13, 'right', ' '), dim, '\n',
            stringr::str_pad('Hierarchy:', 13, 'right', ' '), .check_params, '\n',
            stringr::str_pad('Levels:',    13, 'right', ' '), '\n',
            paste('-', .x, collapse = '\n'), '\n\n',
            sep = ''
          )
        )
      )

    }

    # cube operations
    if (
      !tolower(dataset_name) %in%
      tolower(available_prebuilt_datasets[[mart_index]])
    ) {

      stop(
        paste0(
          'Please check `dataset_name` spelling.\n',
          'It should be one of the following ',
          '(non-case-sensitive):\n\n',
          paste(
            '  -',
            available_prebuilt_datasets[[mart_index]],
            collapse = '\n'
          )
        ),
        call. = F
      )

    }
  } else if (data_source == 'olap') {

    ## Create query
    environment(olap_handler) <- environment()

    query_output <- olap_handler()

    query_output <- rename_cols(query_output)

    # TODO: process data type?

    # Post data-retrieval processing, if needed.
    # Can handle both OLAP and SQL output data.
    if (tolower(dataset_name) == 'vital stats ccd dashboard') {

      query_output <-
        query_output %>%
        dplyr::group_by(
          !!!rlang::syms(stringr::str_subset(names(.), 'ccd', negate = T))
        ) %>%
        tidyr::nest(
          .key = stringr::str_subset(names(.), 'ccd')
        ) %>%
        dplyr::ungroup() %>%
        dplyr::mutate(
          dplyr::across(
            dplyr::where(is.list),
            ~ purrr::map(.x, unlist) %>%
              purrr::map_chr(paste, collapse = '|')
          )
        )

    }

  }

  if (isTRUE(.clean_data)) query_output <- datatype_cols(query_output)

  if (isTRUE(.return_data)) return(query_output)

}


