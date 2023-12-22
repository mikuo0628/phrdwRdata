####################################################################
# fn_cdi_mdx_queries - mdx queries for the CDI cube
#
# Each function contains the "Select" part of the query. The filter
# parameters are built separately
#
#
# Author: Darren Frizzell
# Created: 2018-09-16
#
# Modification: See Git Comments
#
####################################################################



#
# Function returns the filter/where condition for an mdx query
#
cdi_filter_clause <- function(query_start_date, query_end_date, parameter_list)
{



  #
  # Get the mdx statement for a vector of UCD Codes
  #
  ucd_3char_code = parameter_list$ucd_3char_code

  ucd_3char_code_mdx <- mdx_dimension_filter(dimension_name = "VS - Cause of Death - Underlying",
                                             dimension_name_level_2 = "UCD 3Char Code",
                                             dimension_attributes = ucd_3char_code)


  #
  # Get the mdx statement for a vector of CCD Codes
  #
  ccd_3char_code = parameter_list$ccd_3char_code

  ccd_3char_code_mdx <- mdx_dimension_filter(dimension_name = "VS - Cause of Death - Contributing",
                                             dimension_name_level_2 = "CCD 3Char Code",
                                             dimension_attributes = ccd_3char_code)


  #
  # Get the mdx statement for a vector of Residential Location HA's
  #
  residential_location_ha = parameter_list$residential_location_ha

  residential_location_ha_mdx <- mdx_dimension_filter(dimension_name = "VS - Geo - Residential Location Region",
                                                dimension_name_level_2 = "VS Residential Location HA",
                                                dimension_attributes = residential_location_ha)




  #
  # Get the mdx statement for a vector of Death Location HA's
  #
  death_location_ha = parameter_list$death_location_ha

  death_location_ha_mdx <- mdx_dimension_filter(dimension_name = "VS - Geo - Death Location Region",
                                             dimension_name_level_2 = "VS Death Location HA",
                                             dimension_attributes = death_location_ha)


  #
  # Build the query filter
  #
  filter_clause =paste("FROM (SELECT (",ucd_3char_code_mdx, ccd_3char_code_mdx, residential_location_ha_mdx, death_location_ha_mdx,
"{[VS - Date - Death].[Date].&[",
                       query_start_date,"T00:00:00]:[VS - Date - Death].[Date].&[",
                       query_end_date,"T00:00:00]}) ON COLUMNS FROM [CDI])",
                       sep="")

  return(filter_clause)

}


#
# Function returns an mdx query for pulling core panorama data from the CDI cube
#

cdi_vital_stats_query <- function(query_start_date, query_end_date, parameter_list)
{


#
# Build an mdx query
#
mdx_query = "
SELECT
[Measures].[VS Death Count]
ON COLUMNS,
NON EMPTY {
[VS - ID].[Unique ID].[Unique ID]
* [VS - Cause of Death - Underlying].[UCD Code Hierarchy].[UCD 5Char Code].ALLMEMBERS
* [VS - Date - Death].[Date].[Date]
* [CDI - Gender].[Sex].[Sex]
* [VS - Age at Death].[Age Year].[Age Year]
* [VS - Age at Death].[Age Group 07].[Age Group 07]
* [VS - Age at Death].[Age Group Population].[Age Group Population]
* [VS - Place of Injury Type].[Place of Injury Type].[Place of Injury Type]
* [VS - Geo - Residential Location Region].[VS Residential Location Health Authorities].[VS Residential Location LHA].ALLMEMBERS
* [VS - Geo - Death Location Region].[VS Death Location Health Authorities].[VS Death Location LHA].ALLMEMBERS
* [VS - Geo - Residential Location Census - 2011].[VS Residential Location Census - 2011].[VS Residential Dissemination Area].ALLMEMBERS
* [VS - Geo - Death Location Census - 2011].[VS Death Location Census - 2011].[VS Death Location Dissemination Area].ALLMEMBERS
}

ON ROWS"

#
# Get the filter clause for this MDX
#
mdx_filter_clause = cdi_filter_clause(query_start_date, query_end_date, parameter_list)

mdx_query = paste(mdx_query,mdx_filter_clause)

return (mdx_query)

}


cdi_vital_stats_ccd_query <- function(query_start_date, query_end_date, parameter_list)
{


  #
  # Build an mdx query
  #
  mdx_query = "
SELECT
[Measures].[VS Death Count]
ON COLUMNS,
NON EMPTY {
  [VS - ID].[Unique ID].[Unique ID]
* [VS - Cause of Death - Contributing].[CCD Code Hierarchy].[CCD 5Char Code].ALLMEMBERS
}

ON ROWS"

#
# Get the filter clause for this MDX
#
mdx_filter_clause = cdi_filter_clause(query_start_date, query_end_date, parameter_list)

mdx_query = paste(mdx_query,mdx_filter_clause)

return (mdx_query)

}






