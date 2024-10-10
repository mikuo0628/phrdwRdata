
#' Get PHRDW CDI Data
#'
#' @description
#' `r lifecycle::badge('superseded')`
#'
#' Function for querying and returning PHRDW CDI data
#'
#' @param phrdw_datamart_connection A PHRDW connection object.
#'
#' @param phrdw_datamart The data mart to query.
#'
#' @param dataset_name The dataset to retrieve.
#'
#' @param query_start_date The start date of the data to retrieve.
#'
#' @param query_end_date The end date of the data to retrieve.
#'
#' @param ucd_3char_code A character vector of UCD Codes to retrieve.
#'   Optional parameter.
#'
#' @param ccd_3char_code A character vector of CCD Codes to retrieve.
#'   Optional parameter.
#'
#' @param residential_location_ha A character vector of BC Health Authorities
#'   associated with decedent's usual residence. Optional parameter.
#'
#' @param death_location_ha A character vector of BC Health Authorities
#'   associated with the location of death. Optional parameter.
#'
#' @return A data frame with the dataset retrieved from the specified PHRDW data mart.
#'
#' @noRd
#'
get_phrdw_cdi_data <- function(
    phrdw_datamart_connection,
    phrdw_datamart,
    dataset_name,
    query_start_date, query_end_date,
    ucd_3char_code = "",
    ccd_3char_code = "",
    residential_location_ha = "",
    death_location_ha = ""
) {

  #
  # Assign the function parameters to a list
  #
  # parameter_list = list(ucd_3char_code = ucd_3char_code,
  #                       ccd_3char_code = ccd_3char_code,
  #                       residential_location_ha = residential_location_ha,
  #                       death_location_ha = death_location_ha)

  parameter_list <-
    as.list(environment()) %>%
    purrr::discard(
      .p =
        names(.) %in% c('mart', 'type', 'collect_data') |
        stringr::str_detect(names(.), 'date$')
    ) %>%
    purrr::modify_if(.p = is.null, .f = ~ '')

  #
  # Check which datamart and dataset to query
  #
  query <- ""

  # CDI dataset library calls
  if(phrdw_datamart == "CDI"){

    if(dataset_name == "Vital Stats"){
      query <- cdi_vital_stats_query(query_start_date,query_end_date, parameter_list)

    }else if(dataset_name == "Vital Stats CCD"){
      query <- cdi_vital_stats_ccd_query(query_start_date, query_end_date, parameter_list)

    }

  }else{
    query =""
  }


  if(!is.na(query)){

    # Execute an MDX query against the PHRDW cubes

    # Use a try catch to handle queries that result in 0 rows of data
    phrdw_dataset = tryCatch({
    execute2D(phrdw_datamart_connection, query)

    }, error = function(e){
    phrdw_dataset = "No data was returned by this mdx query"

    }
    )

  }else{

    phrdw_dataset = "Unknown dataset or datamart was requested"
  }

  #
  # If a data.frame has been returned, then rename and assign columns
  #
  if(class(phrdw_dataset) == "data.frame" && phrdw_datamart != "CD Mart"){

    phrdw_dataset <- rename_phrdw_columns(phrdw_dataset)
    assign_phrdw_data_type(phrdw_dataset, phrdw_datamart = phrdw_datamart)

  }

  if(class(phrdw_dataset) == "data.frame"){
    phrdw_dataset <- phrdw_dataset %>%
      dplyr::mutate_if(is.character, list(~dplyr::na_if(., "")))
  }

  return(phrdw_dataset)



}
