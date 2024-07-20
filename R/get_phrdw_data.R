#' Title
#'
#' List of marts:
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
#' @param phrdw_datamart_connection `r lifecycle::badge('superseded')` Original
#' function design: supply a connection object created by [connect_to_phrdw].
#' Recommend using `mart` and `type` instead for flexibility
#' (see [connect_to_phrdw]). The function takes care of connecting
#' to the appropriate PHRDW database and disconnect after performing the
#' requested data filtering and retrieving.
#' @param phrdw_datamart `r lifecycle::badge('superseded')` Original function
#' design: supply the *exact* name of the datamart. Need to exercise case
#' sensitivity. Recommend using `mart` and `type` instead for flexibility
#' (see [connect_to_phrdw]).
#' @param mart Provide an appropriate mart name (non-case specific).
#' See `Details`.
#' @param type Provide an appropriate mart type (non-case specific).
#' See `Details`.
#' @param dataset_name
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
#' @param ...
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
    collect_data                   = T,
    mart                           = NULL,
    type                           = NULL,
    ...
) {

  if (!is.null(phrdw_datamart_connection)) {

    if (is.null(phrdw_datamart)) stop('Please supply `phrdw_datamart`.')
    if (is.null(dataset_name))   stop('Please supply `dataset_name`.')

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

      # Legacy method: requires phrdw_datamart_connection
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

  } else {

    if (stringr::str_detect(tolower(mart), '^cd')) {

      param_list <-
        append(
          # purrr::discard_at(as.list(environment()), c(1:2)),
          as.list(environment()),
          list(...)
        )

      # execute pre-build dataset_name specific queries
      if (tolower(param_list$dataset_name) == 'investigation') {

        # DO THIS
        cd_investigation_query(mart, type, param_list)

      }

    }

  }

}


