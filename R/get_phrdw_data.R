#' Retrieve pre-built datasets from specific datamarts.
#'
#' @description
#' Helps users retrieve pre-built datasets from specific datamarts, with the
#' capacity to supply values to some default filters, such as dates and
#' diseases.
#' In addition to retrieving data, user can perform preliminary
#' self-diagnostics to troubleshoot issues, return query instead of data, and
#' even supply filters outside the predefined default filters.
#'
#' This function retains the legacy form, which can be invoked by providing
#' arguments with `phrdw_` prefix, such as `phrdw_datamart_connection` and
#' `phrdw_datamart`.
#'
#' The modern form, on the other hand, is invoked by a simple `mart` and/or
#' `type`, which streamlines the process, allows additional features, and
#' avoids redundant inputs such as `phrdw_datamart` in both
#' [connect_to_phrdw()] and [get_phrdw_data()].
#'
#' @inherit connect_to_phrdw details
#'
#' @param phrdw_datamart_connection `r lifecycle::badge('superseded')`
#'
#'   Legacy function design: supply a connection object created by
#'   [phrdwRdata::connect_to_phrdw()]. Recommend using `mart` and `type`
#'   instead for flexibility (see [phrdwRdata::connect_to_phrdw()]).
#'   The function takes care of connecting
#'   to the appropriate PHRDW database and disconnect after performing the
#'   requested data filtering and retrieving.
#'
#' @param dataset_name The name of the pre-built dataset to retrieve.
#'
#' @param query_start_date Start date of your dataset. Can accept character date.
#'   Defaults to `NULL`, which indicates no lower bound.
#'
#' @param query_end_date End date of your dataset. Can accept character date.
#'   Defaults to `NULL`, which indicates no upper bound.
#'
#' @param include_patient_identifiers Whether to include patient identifier
#'   information. Accepts Boolean values. Defaults to `FALSE`. Note: user needs
#'   to have access, otherwise data restriction may return unintended results.
#'
#' @param include_indigenous_identifiers Whether to include indigenous
#'   identifier information. Accepts Boolean values. Defaults to `FALSE`. Note:
#'   user needs to have access, otherwise data restriction may return
#'   unintended results.
#'
#' @param retrieve_system_ids Whether to include systems IDs in the dataset.
#'  Currently only applies to `Enteric` datamart. Defaults to legacy value "Yes",
#'  but can accept Boolean, and no longer case-specific.
#'
#' @param disease Optional. Character vector of diseases. Only applicable to
#'   some datasets.
#'
#' @param surveillance_condition Optional. Character vector of surveillance
#'   conditions. Only for applicable datasets.
#'
#' @param classification Optional. Character vector of classifications.
#'   Only for applicable datasets.
#'
#' @param surveillance_region_ha Optional. Character vector of Health Region
#'   Authorities where the Patient lives/lived. Only for applicable datasets.
#'
#' @param infection_group Optional. Character vector of infection groups.
#'   Only for applicable datasets.
#'
#' @param ordering_provider_ha Optional. Character vector of Health Region
#'   Authorities where the Ordering Provide resides.
#'   Only for applicable datasets.
#'
#' @param lis_status Optional. Character vector of case level statuses about
#'   the LIS data in a Case. Only for applicable datasets.
#'
#' @param episode_status Optional. Character vector of episode statuses from the
#'   LIS result processing and rule engine. Only for applicable datasets.
#'
#' @param test_type Optional. Character vector of the types of tests. Only for
#'   applicable datasets.
#'
#' @param episode_testing_pattern Optional. Character vector of testing patterns.
#'   Only for applicable datasets.
#'
#' @param testing_region_ha Optional. Character vector of testing Health Region
#'   Authorities. Only for applicable datasets.
#'
#' @param case_status Optional. Character vector of case status. Only for
#'   applicable datasets.
#'
#' @param case_source Optional. Character vector of case status. Only for
#'   applicable datasets.
#'
#' @param ucd_3_char_code Optional. Character vector of UCD 3-character codes.
#'   Only for applicable CDI datasets.
#'
#' @param ccd_3_char_code Optional. Character vector of CCD 3-character codes.
#'   Only for applicable CDI datasets.
#'
#' @param residential_location_ha Optional. Character vector of BC Health
#'   Authorities associated with decedant's usual residence.
#'   Only for applicable CDI datasets.
#'
#' @param death_location_ha Optional. Character vector of BC Health
#'   Authorities associated with decedant's location of death.
#'   Only for applicable CDI datasets.
#'
#' @param .head `r lifecycle::badge('experimental')`
#'
#'  Optional. Single integer vector to indicate how many rows from the top
#'  to return. Note: `tail` is not supported on database backends.
#'
#' @param .check_params `r lifecycle::badge('stable')`
#'
#'   Can accept Boolean or character values.
#'
#'   Boolean `TRUE` will return general info of the dataset:
#'   For MDX queries: dimensions, hierarchies, and levels if possible, and
#'   hierarchies for default filters.
#'   For SQL queries: column names as they are in the source tables, and what
#'   they are renamed to, and columns for default filters.
#'
#'   On the other hand, user can supply hierarchy or column names retrieved from
#'   the above as character vector, and a list of the cardinal levels will
#'   return. Useful to check for typos, or available names to filter for.
#'
#' @param .return_query Boolean value. Whether to return query or not.
#'
#' @param .return_data Boolean value. Whether to return data or not.
#'
#' @param .clean_data Boolean value. Whether to attempt cleaning the dates in
#'   data or not.
#'
#' @param .query_info `r lifecycle::badge('experimental')`
#'
#'   TODO
#'   A user supplied `data.frame` object similar to
#'   'phrdwRdata:::list_query_info', on which the appropriate operation will
#'   take place and retrieve specified data.
#'
#' @param .query_str `r lifecycle::badge('experimental')`
#'
#'  TODO
#'  Single character vector of query. User is responsible for syntax validity
#'  and compatibility (ie. OLAP or SQL, appropriate access, etc) of the query.
#'
#' @param .cte `r lifecycle::badge('experimental')`
#'
#'   Experimental support for common table expressions (CTEs). Defaults to
#'   `FALSE`.
#'   This is the equivalent of `pipe`, which in essence allows writing
#'   subqueries in the order in which they are evaluated. Supported in
#'   [dplyr::show_query()], [dplyr::compute()], and [dplyr::collect()]. This
#'   is one of the ways to optimize SQL execution: using CTEs `WITH` instead of
#'   subqueries.
#'
#' @param ... User can supply named vector: names being the column or
#' hierarchy name, and elements of the vector being the value to filter for.
#' See `Details`.
#'
#' @inheritParams connect_to_phrdw
#'
#' @return Depending on user input, a `data.frame` or `tibble` or character
#'   string.
#'
#' @example examples/ex-get_phrdw_data.R
#'
#' @export
#'
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
    .head                          = NULL,
    .check_params                  = F,
    .return_query                  = F,
    .return_data                   = !(.return_query || isTRUE(.check_params) || is.character(.check_params)),
    .clean_data                    = F,
    .query_df                    = NULL,
    .query_str                     = NULL,
    .cte                           = F,
    ...
) {

  # Basic checks and deriving some useful vars ----
  if (is.null(c(phrdw_datamart, mart))) {

    stop(
      paste(
        'Missing essential parameter(s):',
        'Must enter valid value for either `phrdw_datamart` or `mart`.',
        sep = '\n'
      ),
      call. = F
    )

  }

  default_params <- as.list(environment())
  user_params    <- rlang::list2(...)

  # Check if input datamart exists
  if (!is.null(phrdw_datamart)) {

    phrdw_datamarts <-
      unique(unlist(purrr::map(servers, purrr::pluck, 'phrdw_datamart')))

    mart <-
      servers %>%
      purrr::map(dplyr::filter, .data$phrdw_datamart == .env$phrdw_datamart) %>%
      purrr::discard(~ nrow(.x) == 0) %>%
      purrr::pluck(1, 'mart') %>%
      unique #%>%
      # rlang::set_names()

    if (!phrdw_datamart %in% phrdw_datamarts) {

        stop(
          paste0(
            'Please check argument `phrdw_datamart` spelling.\n',
            'It should be one of the following ',
            '(case-sensitive):\n\n',
            paste('  -', phrdw_datamarts, collapse = '\n')
          ),
          call. = F
        )

    }

  }

  marts <- unique(unlist(purrr::map(servers, purrr::pluck, 'mart')))

  # Check if input mart exists
  if (!is.null(default_params$mart)) {

    if (!tolower(default_params$mart) %in% tolower(marts)) {

      stop(
        paste0(
          'Please check argument `mart` spelling.\n',
          'It should be one of the following ',
          '(non case-sensitive):\n\n',
          paste('  -', marts, collapse = '\n')
        ),
        call. = F
      )

    }

  }

  # name of mart: proper, to reference; element of mart: user input
  mart <-
    rlang::set_names(
      mart,
      marts[which(tolower(marts) == tolower(mart))]
    )

  # Data source
  data_source <-
    {

      if (!is.null(phrdw_datamart_connection)) {

        stringr::str_extract(class(phrdw_datamart_connection), 'SQL|OLAP') %>%
          purrr::discard(is.na)

      } else if (!is.null(mart)) {

        purrr::map(servers, dplyr::pull, mart) %>%
          purrr::map(unique) %>%
          purrr::map_lgl(~ tolower(mart) %in% tolower(.x)) %>%
          purrr::discard(isFALSE) %>%
          names()

      } else {

        list(.query_str, .query_df) %>%
          map(names) %>%
          discard(is.null) %>%
          unlist

      }

    } %>%
    tolower()

  # user supplied query str
  if (!is.null(.query_str)) {

    if (data_source == 'sql') {

      query_output <-
        odbc::dbGetQuery(
          connect_to_phrdw(mart = mart, type = type),
          .query_str
        )

    } else if (data_source == 'olap') {

      query_output <-
        execute2D(
          connect_to_phrdw(mart = mart, type = type),
          .query_str
        )

    }

    return(query_output)

  }

  # If input dataset_name, check spelling against existing dataset_names,
  if (!is.null(dataset_name)) {

    prebuilt_datasets <-
      list_query_info[[data_source]] %>%
      dplyr::filter(tolower(.data$mart) == tolower(.env$mart)) %>%
      dplyr::pull(dataset_name) %>%
      unique

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

  # Legacy method: requires phrdw_datamart_connection ----
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

  # New method: use `mart` and `type` instead ----
  # no need phrdw_datamart_connection
  if (!is.null(dataset_name)) {

    .query_info <-
      list_query_info[[data_source]] %>%
      dplyr::filter(
        tolower(.data$mart)         == tolower(.env$mart),
        tolower(.data$dataset_name) == tolower(.env$dataset_name),
      )

  } else {

    .query_info <- .query_df[[data_source]]

  }
  # if (is.null(.query_info)) {
  #
  #   if (is.null(dataset_name)) stop('Please supply `dataset_name`.')
  #
  #   .query_info <- list_query_info[[data_source]]
  #
  #   available_datasets <-
  #     .query_info %>%
  #     dplyr::select(mart, dataset_name) %>%
  #     dplyr::distinct() %>%
  #     dplyr::group_by(mart) %>%
  #     dplyr::summarise(dataset_name = list(dataset_name)) %>%
  #     { rlang::set_names(.$dataset_name, .$mart) }
  #
  #   if (
  #     !tolower(dataset_name) %in%
  #     tolower(available_datasets[[names(mart)]])
  #   ) {
  #
  #     stop(
  #       paste0(
  #         'Please check `dataset_name` spelling.\n',
  #         'It should be one of the following ',
  #         '(non case-sensitive):\n\n',
  #         paste(
  #           '  -',
  #           available_datasets[[names(mart)]],
  #           collapse = '\n'
  #         )
  #       ),
  #       call. = F
  #     )
  #
  #   }
  #
  # }

  if (data_source == 'sql') {

    environment(sql_handler) <- environment()

    query_output <- sql_handler()

  } else if (data_source == 'olap') {

    environment(olap_handler) <- environment()

    query_output <- olap_handler()

  }

  if (isTRUE(.return_query)) return(query_output)
  if (isFALSE(.return_data)) return()

  # Post data-retrieval processing, if needed.
  if (data_source == 'sql') {

    if (tolower(dataset_name) == 'sti') {

      age_breaks <-
        sort(unique(c(1, seq.int(0, 30, 5), seq.int(30, 60, 10), Inf))) %>%
        rlang::set_names(
          paste(., dplyr::lead(.) - 1, sep = '-') %>%
            dplyr::case_match(
              .x = .,
              .default = .,
              '0-0'    ~ '<1',
              '60-Inf' ~ '60+'
            ) %>%
            paste('Years')
        )

      query_output <-
        query_output %>%
        dplyr::mutate(
          birth_year_phs =
            as.integer(
              lubridate::year(lubridate::ymd(birth_year_phs))
            ),
        ) %>%
        dplyr::mutate(
          # .keep = 'used',
          .before = 'age_atoc_phs',
          age_grp_10_atoc_phs =
            cut(
              age_atoc_phs,
              breaks = age_breaks,
              labels = names(age_breaks)[-length(age_breaks)],
              right  = F,
              ordered_result = T
            )
        )

    }

  }
  if (data_source == 'olap') {

    query_output <- rename_cols(query_output)

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
            tidyselect::where(is.list),
            ~ purrr::map(.x, unlist) %>%
              purrr::map_chr(paste, collapse = '|')
          )
        )

    }

  }

  if (isTRUE(.clean_data)) query_output <- datatype_cols(query_output)

  return(query_output)

}


