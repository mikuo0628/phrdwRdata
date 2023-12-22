####################################################################
# fn_phrdw_training_queries - Queries for using the PHRDW Training
# datasets
#
# Each function contains the "Select" part of the query. The filter
# parameters are built separately
#
#
# Author: Darren Frizzell
# Created: 2019-01-31
#
# Modification: See Git Comments
#
####################################################################



#
# Function returns the filter condition
#
phrdw_training_filter_clause <- function(query_start_date, query_end_date, parameter_list)
{

  #
  # Get the optional parameters to the filter the investigations_data
  #
  parm_disease <- parameter_list$disease
  parm_classification <- parameter_list$classification
  parm_surveillance_region_ha <- parameter_list$surveillance_region_ha

  #
  # Ensure no empty strings are in the variables
  #
  parm_disease <- unique(parm_disease[parm_disease != ""])
  parm_classification <- unique(parm_classification[parm_classification != ""])
  parm_surveillance_region_ha <- unique(parm_surveillance_region_ha[parm_surveillance_region_ha != ""])




  phrdw_training_filter_dataset <- phrdwRdata:::investigations_data

  if(length(parm_disease) >0){
    phrdw_training_filter_dataset <- phrdw_training_filter_dataset %>%
      filter(disease %in% parm_disease)
  }

  if(length(parm_classification) >0){
    phrdw_training_filter_dataset <- phrdw_training_filter_dataset %>%
      filter(classification %in% parm_classification)
  }


  if(length(parm_surveillance_region_ha) >0){
    phrdw_training_filter_dataset <- phrdw_training_filter_dataset %>%
      filter(surveillance_region_ha %in% parm_surveillance_region_ha)
  }


  #
  # Filter the dataset to matched the passed in dates.
  #

  case_ids <- phrdw_training_filter_dataset %>%
    filter(case_date >= query_start_date & case_date <= query_end_date) %>%
    select(case_id)

  return(case_ids)

}


#
# Function returns the phrdw training investigations dataset
#

phrdw_training_investigations <- function(query_start_date, query_end_date, parameter_list)
{

  #
  # Determine which case_ids match the passed in parameters
  #
  case_ids <- phrdw_training_filter_clause(query_start_date, query_end_date, parameter_list)

  #
  # Determine the investigations to return based on a list of case_ids
  #
  phrdw_training_dataset <- phrdwRdata:::investigations_data %>%
    filter(case_id %in% case_ids$case_id)


return (phrdw_training_dataset)
}



#
# Function returns the phrdw training udf dataset
#

phrdw_training_udf <- function(query_start_date, query_end_date, parameter_list)
{

  #
  # Determine which case_ids match the passed in parameters
  #
  case_ids <- phrdw_training_filter_clause(query_start_date, query_end_date, parameter_list)


  #
  # Determine the UDF data to return based on a list of case_ids
  #
  phrdw_training_dataset <- phrdwRdata:::udf_data %>%
    filter(case_id %in% case_ids$case_id)


  return (phrdw_training_dataset)
}


#
# Function returns the phrdw training Symptom dataset
#

phrdw_training_symptoms <- function(query_start_date, query_end_date, parameter_list)
{

  #
  # Determine which case_ids match the passed in parameters
  #
  case_ids <- phrdw_training_filter_clause(query_start_date, query_end_date, parameter_list)


  #
  # Determine the Symptom data to return based on a list of case_ids
  #
  phrdw_training_dataset <- phrdwRdata:::symptom_data %>%
    filter(case_id %in% case_ids$case_id)


  return (phrdw_training_dataset)
}



#
# Function returns the phrdw training lis dataset
#

phrdw_training_lis <- function(query_start_date, query_end_date, parameter_list)
{

  #
  # Determine which case_ids match the passed in parameters
  #
  case_ids <- phrdw_training_filter_clause(query_start_date, query_end_date, parameter_list)


  #
  # Determine the LIS Tests to return based on a list of case_ids
  #
  phrdw_training_dataset <- phrdwRdata:::lis_data %>%
    filter(case_id %in% case_ids$case_id)


  return (phrdw_training_dataset)
}




