#######################################################################
# fn_query_filter_builder - builds filter conditions for cubes and SQL
#
# The function takes in 3 paramters and uses them to an mdx
# string that can
#
#
# Author: Darren Frizzell
# Created: 2018-05-29
#
# Modification: See Git Comments
#
########################################################################




#
# Function returns the filter/where condition for an mdx query based on the filter_rules dataframe
#
filter_clause_olap <- function(query_start_date, query_end_date, parameter_list)
{

  #
  # Get the filter rules for this query
  #
  phrdw_filter_rules <- phrdwRdata:::filter_rules


  phrdw_filter_rules <- phrdw_filter_rules %>%
    dplyr::filter(
      datamart == parameter_list$phrdw_datamart,
      dataset == parameter_list$dataset_name
    )


  character_filter_rules <- phrdw_filter_rules %>%
    dplyr::filter(filter_type == "character")


  filter_clause <- "FROM ( SELECT ("


  for(c in seq_along(character_filter_rules)){

    # print(character_filter_rules$dimension_name[c])
    parm_temp <- character_filter_rules$parameter[c]
    parm_temp <- parameter_list[[parm_temp]]

    mdx_filter <- mdx_dimension_filter(dimension_name = character_filter_rules$dimension_name[c],
                                       dimension_name_level_2 = character_filter_rules$dimension_name_level_2[c],
                                       dimension_attributes = parm_temp)

    filter_clause <- paste0(filter_clause, mdx_filter)

  }


  #
  # Build the date filter
  #
  date_mdx <- ""

  start_date_filter_rules <- phrdw_filter_rules %>%
    dplyr::filter(parameter == "query_start_date")

  end_date_filter_rules <- phrdw_filter_rules %>%
    dplyr::filter(parameter == "query_end_date")

  date_mdx <- paste0("{[",start_date_filter_rules$dimension_name,"].[Date].&[",query_start_date,"T00:00:00]",
                     ":[",end_date_filter_rules$dimension_name,"].[Date].&[",query_end_date,"T00:00:00]}")


  #
  # Paste all the mdx together.
  #

  if(parameter_list$phrdw_datamart == "STIBBI" |
     parameter_list$phrdw_datamart == "STIBBI QA" |
     parameter_list$phrdw_datamart == "STIBBI SU" |
     parameter_list$phrdw_datamart == "STIBBI SA"){

    filter_clause <- paste0(filter_clause, date_mdx,") ON COLUMNS FROM [StibbiDM])")

  } else

    if (parameter_list$phrdw_datamart == "Respiratory"){

      filter_clause <- paste0(filter_clause, date_mdx,") ON COLUMNS FROM [RespiratoryDM])")

    }

  return(filter_clause)
}




#
# Function returns the filter conditions for an mdx query
#
mdx_dimension_filter <- function(dimension_name, dimension_name_level_2, dimension_attributes)
{
  #
  # Ensure no empty strings are passed into the filter.
  #
  dimension_attributes = unique(dimension_attributes[dimension_attributes != ""])

  #
  # Allow for a vector of dimension attributes to be passed in to the filter
  #
  dim_len = length(dimension_attributes)

  # Test to ensure at least 1 disease was passed in
  if(dim_len > 0){

    dim_start = 1
    dimension_filter_mdx = "{"

    for(dim in dimension_attributes){

      dimension_filter_mdx = paste0(dimension_filter_mdx,"[",dimension_name,"].[",dimension_name_level_2,"].&[",dim,"]")

      # Check if there are more dimension attributes to be added
      if(dim_start < dim_len){
        dimension_filter_mdx <- paste0(dimension_filter_mdx,",")
      }
      dim_start = dim_start + 1
    }

    dimension_filter_mdx = paste0(dimension_filter_mdx,"},")


  }else{
    dimension_filter_mdx = ""
  }

  # Return the mdx for uses in an mdx query
  return(dimension_filter_mdx)

}




#
# Function returns the filter conditions for an SQL query
#
sql_where_clause_filter <- function(field_name, filter_values)
{
  #
  # Ensure no empty strings are passed into the filter.
  #
  filter_values = unique(filter_values[filter_values != ""])


  #
  # Allow for a vector of diseases to be passed in to the filter
  #
  values_len = length(filter_values)

  # Test to ensure at least 1 filter value was passed in
  if(values_len > 0){

    value_start = 1
    filter_sql = paste0("AND ", field_name, " IN (")

    for(value in filter_values){

      filter_sql = paste0(filter_sql,"'",value,"'")

      # Check if there are more filter values to be added to the sql
      if(value_start < values_len){
        filter_sql <- paste0(filter_sql,",")
      }
      value_start = value_start + 1
    }

    filter_sql = paste0(filter_sql,")")


  }else{
    filter_sql = ""
  }

  # Return the sql for uses in an sql query
  return(filter_sql)

}

