#####################################################################
# Assign data type to phrdw data`
#
# Purpose: Assign a data type to the columns from a phrdw dataset
#
# Author: Darren Frizzell
# Created: 2018-03-02
#
#####################################################################
#' @title Assigns numeric datatype to datasets.
#' @description
#' `r lifecycle::badge('superseded')`
#'
#' Assigns a data type to specific columns.
#' This function will only assign a numeric datatype for fields ending
#' with `_id` or `_key`
#'
#' @param phrdw_dataset A data frame with columns from the PHRDW.
#' @return A data frame with numeric fields assigned.
#' @export
assign_phrdw_data_type <- function(phrdw_dataset, phrdw_datamart)
{

  column_names = names(phrdw_dataset)

  col_num = 1

  # For each of the columns, check for patterns to assign a data type
  for(col_name in column_names){

      # If the column name ends with _id or _key, then change it to an integer
      if(stringr::str_detect(col_name,"_id$|_key$") && col_name != "container_id" && col_name != "parent_container_id"
         && col_name != "emr_form_id" && col_name != "haisys_sti_id" && col_name != "stiis_client_number" &&
         phrdw_datamart != "Respiratory"){

          # print(col_name)
          phrdw_dataset[,col_num] = as.numeric(phrdw_dataset[,col_num])

        } else {

          if(stringr::str_detect(col_name,"_id$|_key$") && col_name != "episode_id" && col_name != "container_id" ){

            # print(col_name)
            phrdw_dataset[,col_num] = as.numeric(phrdw_dataset[,col_num])

            }

          }


    # # Original: If the column name ends with _id or _key, then change it to an integer
    # if(stringr::str_detect(col_name,"_id$|_key$") && col_name != "container_id" && col_name != "parent_container_id"
    #    && col_name != "emr_form_id" && col_name != "haisys_sti_id" && col_name != "stiis_client_number"){
    #
    #   # print(col_name)
    #   phrdw_dataset[,col_num] = as.numeric(phrdw_dataset[,col_num])
    #
    #
    # }

    col_num = col_num + 1
  }

  return(phrdw_dataset)
}
