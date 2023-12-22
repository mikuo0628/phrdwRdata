####################################################################
# fn_respiratory_mdx_queries - mdx queries for pullling respiratory data
#
# Each function contains the "Select" part of the query. The filter
# parameters are built separately
#
#
# Author: Sovit Chalise
# Created: 2020-10-15
#
####################################################################



#
# Function returns an mdx query for pulling LIS - Test data from the Flu cube.
#

respiratory_lis_test_volume_query <- function(query_start_date, query_end_date, parameter_list)
{
  test_volume_mdx = "
  SELECT
NON EMPTY {
  [Measures].[LIS Test Count]
} ON COLUMNS,

NON EMPTY { (
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  * [LIS - IDs].[Test ID].[Test ID]
  * [LIS - IDs].[Accession Number].[Accession Number]
  * [LIS - IDs].[Container ID].[Container ID]
  * [LIS - Episode IDs].[Episode ID].[Episode ID]
  * [LIS - Patient].[Gender].[Gender]
  * [LIS - Patient].[Species].[Species]
  * [LIS - Infection Group].[Infection Group].[Infection Group]
  * [LIS - Date - Collection].[Date].[Date]
  * [LIS - Date - Receive].[Date].[Date]
  * [LIS - Date - Result].[Date].[Date]
  * [LIS - Age at Collection].[Age Years].[Age Years]
  * [LIS - Test].[Tests].[Test Code].ALLMEMBERS
  * [LIS - Order Item].[Orders].[Order Code].ALLMEMBERS
  * [LIS - Test].[Source System ID].[Source System ID]
  * [LIS - Result - Test Outcome].[Test Outcome].[Test Outcome]
  * [LIS - Result - Organism].[Organisms].[Organism Level 4].ALLMEMBERS
  * [LIS - Geo - Patient Region].[LIS Patient Health Authorities].[LIS Patient LHA].ALLMEMBERS
  * [LIS - Geo - Patient Address].[LIS Patient Addresses].[LIS Patient Postal Code].ALLMEMBERS
  * [LIS - Geo - Ordering Provider Region ATOT].[LIS Ordering Provider Health Authorities].[LIS Ordering Provider LHA].ALLMEMBERS
  * [LIS - Geo - Ordering Provider Address ATOT].[LIS Ordering Provider Addresses].[LIS Ordering Provider Postal Code].ALLMEMBERS
  * [LIS - Lab Location - Result].[Lab Name].[Lab Name]
  * [LIS - Flag - Proficiency Test].[Proficiency Test].[Proficiency Test]
  * [LIS - Flag - Test Performed].[Test Performed].[Test Performed]
  * [LIS - Flag - Test Valid Status].[Valid Test].[Valid Test]
  * [LIS - Result Attributes].[Result Full Description].[Result Full Description]
  ) }
ON ROWS "

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  test_volume_mdx = paste(test_volume_mdx,mdx_filter_clause)

  return (test_volume_mdx)
}



#
# Function returns an mdx query for pulling LIS Episode data from the STIBBI cube.
#
respiratory_lis_episode_query <- function(query_start_date, query_end_date, parameter_list)
{
  mdx_query =
  "SELECT
NON EMPTY {
  [Measures].[LIS Episode Count]
} ON COLUMNS,

NON EMPTY { (
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  * [LIS - Episode IDs].[Episode ID].[Episode ID]
  * [LIS - Infection Group].[Infection Group].[Infection Group]
  * [LIS - Date - Episode Start].[Flu Season].[Date].ALLMEMBERS
  * [LIS - Episode Attributes].[Episode Testing Pattern].[Episode Testing Pattern]
  * [LIS - Rule Engine - Episode Status].[Rule Engine - Episode Status].[Rule Detail Description].ALLMEMBERS

) }
ON ROWS "

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}

