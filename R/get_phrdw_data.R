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

    # collect_data                   = T,
    mart                           = NULL,
    type                           = c('prod', 'su', 'sa')[1],
    .check_params                  = F,
    .return_query                  = F,
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
            as.list(environment()) %>%
            purrr::discard(
              .p = names(.) %in% c('mart', 'type', 'collect_data')
            ) %>%
            purrr::keep(
              .p = names(.) %in%
                c(
                  'phrdw_datamart_connection',
                  'phrdw_datamart',
                  'dataset_name',
                  'query_start_date',
                  'query_end_date',
                  'ucd_3char_code',
                  'ccd_3char_code',
                  'residential_location_ha',
                  'death_location_ha'
                )
            ) %>%
            purrr::modify_if(.p = is.null, .f = ~ '')
        )
      )

    } else {

      return(
        do.call(
          what = get_phrdw_data_legacy,
          args =
            as.list(environment()) %>%
            purrr::discard(
              .p = names(.) %in% c('mart', 'type', 'collect_data')
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
        dplyr::filter(mdx_query_info, .data$field_name == .check_params)$dim

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
            # purrr::keep(
            #   available_prebuilt_datasets,
            #   stringr::str_detect(
            #     names(available_prebuilt_datasets),
            #     regex(tolower('CDI'), ignore_case = T)
            #   )
            # ),
            available_prebuilt_datasets[[mart_index]],
            collapse = '\n'
          )
        ),
        call. = F
      )

    }

    # Create query
    mdx_query <-
      mdx_query_info %>%
      dplyr::filter(
        tolower(.data$cube)         == tolower(.env$mart),
        tolower(.data$dataset_name) == tolower(.env$dataset_name)
      ) %>% {

        columns <-
          dplyr::filter(., .data$field_type == 'columns')$field_name

        rows    <-
          dplyr::filter(., .data$field_type == 'rows') %>%
          dplyr::reframe(
            .by = dim,
            rows = list(field_name)
          ) %>%
          dplyr::mutate(
            dplyr::across(
              dplyr::where(is.list),
              ~ rlang::set_names(.x, dim)
            )
          ) %>%
          dplyr::pull(rows)

        if (isTRUE(.check_params)) {

          cat(
            paste(
            'The following are (hierarchy) fields',
            'included in this dataset:\n\n'
            )
          )

          purrr::iwalk(
            rows,
            ~ {

              cat(
                paste(stringr::str_pad('* Dimension:',  15, 'right', ' '),
                      .y, '\n'),
                paste(stringr::str_pad('** Hierarchy:', 15, 'right', ' '),
                      .x, '\n'),
                '\n',
                sep = ''
              )
              cat(paste(rep('-', 50), collapse = ''), '\n\n')

            }
          )

          filters <-
            dplyr::filter(
              .,
              stringr::str_detect(.data$field_type, 'filter')
            ) %>%
            dplyr::select(dim, field_name) %>%
            dplyr::group_by(dim) %>%
            dplyr::reframe(hier = list(field_name)) %>%
            purrr::pmap(\(dim, hier) rlang::set_names(hier, dim)) %>%
            unlist

          paste(
            'The following fields (hierarchies)',
            'that can take filters:\n\n'
          )

          purrr::iwalk(
            filters,
            ~ {

              cat(
                paste(stringr::str_pad('* Dimension:',  15, 'right', ' '),
                      .y, '\n'),
                paste(stringr::str_pad('** Hierarchy:', 15, 'right', ' '),
                      .x, '\n'),
                '\n',
                sep = ''
              )
              cat(paste(rep('-', 50), collapse = ''), '\n\n')

            }
          )

        }

        # discrete filters
        filters_discrete <-
          dplyr::filter(
            .,
            .data$field_type == 'filter_d',
            any(
              .data$field_name %in% names(user_params),
              .data$param_name %in%
                names(purrr::discard(default_params, is.null))
            )
          ) %>%
          {

            dplyr::bind_rows(
              # get from ...
              dplyr::full_join(
                by = 'field_name',
                .,
                tibble::enframe(
                  user_params, name = 'field_name', value = 'memb'
                ) %>%
                  dplyr::filter(
                    purrr::map_lgl(memb, is.character),
                    !stringr::str_detect(
                      .data$field_name,
                      stringr::regex('date\\b', ignore_case = T)
                    )
                  ) %>%
                  tidyr::unnest(dplyr::where(is.list))
              ),
              # get from default params
              dplyr::full_join(
                by = 'param_name',
                .,
                tibble::enframe(
                  default_params, name = 'param_name', value = 'memb'
                ) %>%
                  dplyr::filter(
                    purrr::map_lgl(memb, is.character),
                    !stringr::str_detect(
                      .data$param_name,
                      stringr::regex('date\\b', ignore_case = T)
                    )
                  ) %>%
                  tidyr::unnest(dplyr::where(is.list))
              )
            )

          } %>%
          dplyr::select(dim, attr = field_name, memb) %>%
          tidyr::drop_na() %>%
          { if (nrow(.) == 0) NULL else . }

        # date filter
        filter_date <-
          dplyr::filter(
            .,
            .data$field_type == 'filter_r',
            stringr::str_detect(
              .data$field_name,
              stringr::regex('date\\b', ignore_case = T)
            ) |
              .data$param_name %in% c('query_date')
          ) %>%
          dplyr::bind_cols(
            tibble::tibble(
              memb =
                list(
                  default_params$query_start_date,
                  default_params$query_end_date
                ) %>%
                purrr::modify(\(x) if (is.null(x)) 'null' else x) %>%
                unlist
            )
          ) %>%
          dplyr::select(dim, attr = field_name, memb) %>%
          { if (nrow(.) == 0) NULL else . }

        # TODO: other range filters?

        mdx_build(
          cube_name = unique(.$cube),
          columns   = columns,
          rows      = rows,
          discrete  = filters_discrete,
          range     = filter_date
        )

      }

    if (.return_query) return(mdx_query)

    df <-
      execute2D(
        connect_to_phrdw(mart = mart, type = type),
        mdx_query
      ) %>%
      tibble::as_tibble()

  }


}


