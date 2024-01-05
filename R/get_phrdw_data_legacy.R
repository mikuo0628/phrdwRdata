
#' Get PHRDW Data
#'
#' Function for querying and returning PHRDW data
#'
#' @param phrdw_datamart_connection A PHRDW connection object.
#' @param phrdw_datamart The data mart to query.
#' @param dataset_name The dataset to retrieve.
#' @param query_start_date The start date of the data to retrieve.
#' @param query_end_date The end date of the data to retrieve.
#' @param include_patient_identifiers Sets whether to retrieve patient identifiers in a query. Only applicable to CD Data Mart. Default is FALSE.
#' @param include_indigenous_identifiers Sets whether to retrieve patient identifiers in a query. Only applicable to CD Data Mart. Default is FALSE.
#' @param retrieve_system_ids Sets whether to retrieve system ids in a query. Currenly Only applicable to Enteric Data Mart. Default is "Yes".
#' @param disease A character vector of diseases to retrieve. Optional parameter.
#' @param surveillance_condition A character vector of surveillance conditions to retrieve. Optional parameter.
#' @param classification A character vector of classifications to retrieve. Optional parameter.
#' @param surveillance_region_ha A character vector of Health Region Authorities to retrieve. Use this variable to pull data by where the Patient lived. Optional parameter.
#' @param infection_group A character vector of infection groups to retrieve. Optional parameter.
#' @param ordering_provider_ha A character vector of Health Region Authorities to retrieve. Use this variable to pull data by Ordering Provider Health Authorities. Optional parameter.
#' @param lis_status A character vector of case level statuses about the LIS data in a Case. Optional parameter.
#' @param episode_status A character vector of episode statuses from the LIS result processing and rule engine. Optional parameter.
#' @param test_type A character vector of the types of tests to retrieve. Optional parameter.
#' @param episode_testing_pattern A character vector of testing patterns to retrieve. Optional parameter.
#' @param testing_region_ha A character vector of the testing regions to retrieve. Optional parameter.
#' @param case_status A character vector of the case statuses to retrieve. Optional parameter.
#' @param case_source A character vector of the case sources to retrieve. Optional parameter.
#'
#' @return A data frame with the dataset retrieved from the specified PHRDW data mart.
get_phrdw_data_legacy <- function(phrdw_datamart_connection, phrdw_datamart, dataset_name, query_start_date, query_end_date,
                                  include_patient_identifiers = FALSE, include_indigenous_identifiers = FALSE, retrieve_system_ids = "Yes",
                                  disease="", surveillance_condition="", classification="", surveillance_region_ha="", infection_group="",
                                  ordering_provider_ha="", lis_status="", episode_status="", test_type="", episode_testing_pattern="",
                                  testing_region_ha="", case_status="", case_source="") {

  #
  # Assign the function parameters to a list
  #
  parameter_list = list(include_patient_identifiers = include_patient_identifiers,
                        include_indigenous_identifiers = include_indigenous_identifiers,
                        retrieve_system_ids = retrieve_system_ids,
                        disease = disease,
                        surveillance_condition = surveillance_condition,
                        classification = classification,
                        surveillance_region_ha = surveillance_region_ha,
                        infection_group = infection_group,
                        ordering_provider_ha = ordering_provider_ha,
                        lis_status = lis_status,
                        episode_status = episode_status,
                        test_type = test_type,
                        episode_testing_pattern = episode_testing_pattern,
                        testing_region_ha = testing_region_ha,
                        case_status = case_status,
                        case_source = case_source)


  #
  # Check which datamart and dataset to query
  #
  query <- ""


  # Respiratory dataset library calls
  if(phrdw_datamart == "Respiratory"){

    if(dataset_name == "LIS Tests"){
      query <- respiratory_lis_test_volume_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "LIS Episodes"){
      query <- respiratory_lis_episode_query(query_start_date, query_end_date, parameter_list)
    }


  # STIBBI dataset library calls
  }else if(phrdw_datamart == "STIBBI" | phrdw_datamart == "STIBBI SU" | phrdw_datamart == "STIBBI SA"){

    if(dataset_name == "Case"){
      query <- stibbi_case_query(query_start_date,query_end_date, parameter_list)

    }else if(dataset_name == "Investigation"){
      query <- stibbi_investigation_query(query_start_date,query_end_date, parameter_list)

    }else if(dataset_name == "Client"){
      query <- stibbi_client_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "PHS Body Site"){
      query <- stibbi_body_site_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "PHS Client Risk Factor"){
      query <- stibbi_client_risk_factor_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "PHS Investigation Risk Factor"){
      query <- stibbi_investigation_risk_factor_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "PHS UDF Long"){
      query <- stibbi_udf_long_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "LIS Tests"){
      query <- stibbi_lis_test_volume_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "LIS Test Providers"){
      query <- stibbi_lis_test_provider_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "LIS Episodes"){
      query <- stibbi_lis_episode_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "LIS POC"){
      query <- stibbi_lis_poc_query(query_start_date, query_end_date, parameter_list)
    }


    # STIBBI QA dataset library calls
  }else if(phrdw_datamart == "STIBBI QA"){

    if(dataset_name == "Case QA"){
      query <- stibbi_qa_case_query(query_start_date,query_end_date, parameter_list)

    }else if(dataset_name == "Investigation QA"){
      query <- stibbi_qa_investigation_query(query_start_date,query_end_date, parameter_list)

    }else if(dataset_name == "Client QA"){
      query <- stibbi_qa_client_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "PHS Body Site QA"){
      query <- stibbi_qa_body_site_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "PHS Client Risk Factor QA"){
      query <- stibbi_qa_client_risk_factor_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "PHS Investigation Risk Factor QA"){
      query <- stibbi_qa_investigation_risk_factor_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "PHS UDF Long QA"){
      query <- stibbi_qa_udf_long_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "LIS Tests QA"){
      query <- stibbi_qa_lis_test_volume_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "LIS Test Providers QA"){
      query <- stibbi_qa_lis_test_provider_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "LIS Episodes QA"){
      query <- stibbi_qa_lis_episode_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "LIS POC QA"){
      query <- stibbi_qa_lis_poc_query(query_start_date, query_end_date, parameter_list)
    }

  # Enteric dataset library calls
  }else if(phrdw_datamart == "Enteric" | phrdw_datamart == "Enteric SU"){

    if(dataset_name == "Case Investigation"){
      query <- enteric_case_investigation_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "Risk Factor"){
      query <- enteric_risk_factor_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "Symptom"){
      query <- enteric_symptom_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "UDF"){
      query <- enteric_udf_query(query_start_date, query_end_date, parameter_list)

    }else if(dataset_name == "LIS Data"){
      query <- enteric_lis_data_query(query_start_date, query_end_date, parameter_list)
    }

  # VDP dataset library calls
  }else if(phrdw_datamart == "VPD" | phrdw_datamart == "VPD SU"){

    disease_check_list <- vpd_disease_check_list(parameter_list)

    if(dataset_name == "Case Investigation"){
      query <- vpd_case_investigation_query(query_start_date,query_end_date,parameter_list, disease_check_list)

    }else if(dataset_name == "Symptoms"){
      query <- vpd_symptoms_query(query_start_date,query_end_date,parameter_list, disease_check_list)

    }else if(dataset_name == "Symptoms Long"){
      query <- vpd_symptoms_long_query(query_start_date,query_end_date,parameter_list, disease_check_list)

    }else if(dataset_name == "Risk Factors"){
      query <- vpd_risk_factor_query(query_start_date,query_end_date,parameter_list, disease_check_list)

    }else if(dataset_name == "UDF"){
      query <- vpd_udf_query(query_start_date,query_end_date,parameter_list, disease_check_list)

    }else if(dataset_name == "UDF Long"){
      query <- vpd_udf_long_query(query_start_date,query_end_date,parameter_list, disease_check_list)

    }else if(dataset_name == "Immunizations"){
      query <- vpd_immunizations_query(query_start_date,query_end_date,parameter_list, disease_check_list)

    }else if(dataset_name == "Special Considerations"){
      query <- vpd_special_considerations_query(query_start_date,query_end_date,parameter_list, disease_check_list)

    }else if(dataset_name == "LIS Tests"){
      query <- vpd_lis_tests_query(query_start_date,query_end_date,parameter_list, disease_check_list)
    }

  #
  # CD Mart dataset library calls
  #
  }else if(phrdw_datamart == "CD Mart" | phrdw_datamart == "CD Mart SU"){

    if(dataset_name == "Investigation"){
      query <- cd_mart_investigation_query(parameter_list)

    }else if(dataset_name == "Client"){
      query <- cd_mart_client_query(parameter_list)

    }else if(dataset_name == "Risk Factor"){
      query <- cd_mart_risk_factor_query(parameter_list)

    }else if(dataset_name == "Symptom"){
      query <- cd_mart_symptoms_query(parameter_list)

    }else if(dataset_name == "Observation"){
      query <- cd_mart_observations_query(parameter_list)

    }else if(dataset_name == "UDF"){
      query <- cd_mart_udf_query(parameter_list)

    }else if(dataset_name == "Lab"){
      query <- cd_mart_lab_query(parameter_list)

    }else if(dataset_name == "Transmission Events"){
      query <- cd_mart_transmission_event_query(parameter_list)

    }else if(dataset_name == "Contacts"){
      query <- cd_mart_contact_query(parameter_list)

    }else if(dataset_name == "Outbreak"){
      query <- cd_mart_outbreak_query(parameter_list)

    }else if(dataset_name == "TB Contacts"){
      query <- cd_mart_tb_contact_query(parameter_list)

    }else if(dataset_name == "TB Investigation"){
      query <- cd_mart_tb_investigation_query(parameter_list)

    }else if(dataset_name == "TB Transmission Events"){
      query <- cd_mart_tb_transmission_event_query(parameter_list)

    }else if(dataset_name == "TB Client"){
      query <- cd_mart_tb_client_query(parameter_list)

    }else if(dataset_name == "TB TST Investigation"){
      query <- cd_mart_tb_tst_investigation_query(parameter_list)

    }else if(dataset_name == "TB TST Client"){
      query <- cd_mart_tb_tst_client_query(parameter_list)

    }else if(dataset_name == "TB Lab"){
      query <- cd_mart_tb_lab_query(parameter_list)

    }else if(dataset_name == "Complication"){
      query <- cd_mart_complication_query(parameter_list)
    }


  # PHRDW Training dataset library calls
  }else if(phrdw_datamart == "PHRDW Training"){

    if(dataset_name == "Investigation"){
      phrdw_training_dataset <- phrdw_training_investigations(query_start_date, query_end_date, parameter_list)
      query <- "Training Query"

    }else if(dataset_name == "LIS Tests"){
      phrdw_training_dataset <- phrdw_training_lis(query_start_date, query_end_date, parameter_list)
      query <- "Training Query"

    }else if(dataset_name == "UDF"){
      phrdw_training_dataset <- phrdw_training_udf(query_start_date, query_end_date, parameter_list)
      query <- "Training Query"

    }else if(dataset_name == "Symptoms"){
      phrdw_training_dataset <- phrdw_training_symptoms(query_start_date, query_end_date, parameter_list)
      query <- "Training Query"
   }

  }else{
    query <- ""
  }

  if(!query == ""){

    if(phrdw_datamart == "CD Mart" | phrdw_datamart == "CD Mart SU"){

      # Build the parameters data frame
      #
      # TB Contacts uses a different WHERE clause and needs additional date parameters
      #
      if(dataset_name %in% c("TB Contacts","TB TST Client", "Contacts")){
        parameters <- data.frame(param1 = as.character(query_start_date),
                                 param2 = as.character(query_end_date),
                                 param3 = as.character(query_start_date),
                                 param4 = as.character(query_end_date))
      }else{
        parameters <- data.frame(param1 = as.character(query_start_date),
                                 param2 = as.character(query_end_date))
      }
      # Execute the sql query
      phrdw_dataset = tryCatch({
        # RODBCext::sqlExecute(phrdw_datamart_connection,
        #                          query,
        #                          parameters,
        #                          fetch = TRUE,
        #                          stringsAsFactors=FALSE)
        odbc::dbGetQuery(phrdw_datamart_connection,
                         query,
                         params = parameters)
      }, error = function(e){
        phrdw_dataset = "No data was returned by this CD Mart query"

      }
      )

    }else if(phrdw_datamart == "PHRDW Training"){

      #
      # Return the PHRDW Training dataset
      #
      phrdw_dataset <- phrdw_training_dataset


    }else{

      # Execute an MDX query against the PHRDW cubes

      # Use a try catch to handle queries that result in 0 rows of data
      phrdw_dataset = tryCatch({
        execute2D(phrdw_datamart_connection, query)

      }, error = function(e){
        phrdw_dataset = "No data was returned by this mdx query"

      }
      )
    }

  }else{

    phrdw_dataset = "No data was returned or an unknown dataset was requested"
  }

  #
  # If a data.frame has been returned, then rename and assign columns
  #
  if(class(phrdw_dataset) == "data.frame" && phrdw_datamart != "CD Mart" && phrdw_datamart != "CD Mart SU"  && phrdw_datamart != "PHRDW Training"){
    phrdw_dataset <- rename_phrdw_columns(phrdw_dataset)
    assign_phrdw_data_type(phrdw_dataset)

    #
    # Remove the Test ID from the LIS Episode dataset
    #
    if(dataset_name == "LIS Episodes" || dataset_name == "LIS Episodes QA"){
      drop_test_id <- c("test_id")

      phrdw_dataset <- phrdw_dataset[ ,!(names(phrdw_dataset) %in% drop_test_id)] %>%
        distinct()
    }

  }


  if(class(phrdw_dataset) == "data.frame"){
    phrdw_dataset <- phrdw_dataset %>%
      dplyr::mutate_if(is.character, list(~dplyr::na_if(., "")))
  }


  return(phrdw_dataset)


}
