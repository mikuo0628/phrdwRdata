####################################################################
# fn_enteric_mdx_queries - mdx queries for building the enteric
# mdx queries
#
# Each function contains the "Select" part of the query. The filter
# parameters are built separately
#
#
# Author: Darren Frizzell
# Created: 2018-04-16
#
# Modification: See Git Comments
#
####################################################################



#
# Function returns the filter/where condition for an mdx query
#
enteric_filter_clause <- function(query_start_date, query_end_date, parameter_list)
{


  #
  # Get the mdx statement for a vector of diseases
  #
  disease = parameter_list$disease

  disease_mdx <- mdx_dimension_filter(dimension_name = "Case - Disease",
                                      dimension_name_level_2 = "Case Disease",
                                      dimension_attributes = disease)


  #
  # Get the mdx statement for a vector of surveillance conditions
  #
  surveillance_condition = parameter_list$surveillance_condition

  surveillance_condition_mdx <- mdx_dimension_filter(dimension_name = "Case - Surveillance Condition",
                                      dimension_name_level_2 = "Surveillance Condition",
                                      dimension_attributes = surveillance_condition)



  #
  # Get the mdx statement for a vector of classifications
  #
  classification = parameter_list$classification

  classification_mdx <- mdx_dimension_filter(dimension_name = "Pan - Investigation Classification",
                                                     dimension_name_level_2 = "Classification",
                                                     dimension_attributes = classification)


  #
  # Get the mdx statement for a vector of classifications
  #
  surveillance_region_ha = parameter_list$surveillance_region_ha

  surveillance_region_ha_mdx <- mdx_dimension_filter(dimension_name = "Pan - Surveillance Region",
                                             dimension_name_level_2 = "Pan Surveillance Region HA",
                                             dimension_attributes = surveillance_region_ha)

  #
  # Build the query filter
  #

  filter_clause =paste("FROM (SELECT (",disease_mdx, surveillance_condition_mdx, classification_mdx, surveillance_region_ha_mdx,
"{
[Pan - Investigation Classification].[Classification Group].&[**No Investigation],
  [Pan - Investigation Classification].[Classification Group].&[Case]
},
{
[LIS - Patient].[Species Category].&[Human],
[LIS - Patient].[Species Category].&[**No Test]
}
,[Patient - Match Level].[Match Level Description].&[3]
,{[Case - Date - Earliest].[Date].&[",
                       query_start_date,"T00:00:00]:[Case - Date - Earliest].[Date].&[",
                       query_end_date,"T00:00:00]}) ON COLUMNS FROM [EntericDM])",
                       sep="")

  return(filter_clause)

}


#
# Function returns an mdx query for pulling core panorama data from the Enteric cube
#

enteric_case_investigation_query <- function(query_start_date, query_end_date, parameter_list)
{

#
# Get the system id variable
#
retrieve_system_ids = parameter_list$retrieve_system_ids

#
# Build an mdx query
#

system_ids = ""
if(retrieve_system_ids == "Yes"){

  system_ids <- ",[Case - IDs].[Case Combined ID].[Investigation ID]
  ,[Case - IDs].[Case Combined ID].[Disease Event ID]
  ,[Case - IDs].[Case Combined ID].[Pan Client ID]
  ,[Case - IDs].[Case Combined ID].[Paris Assessment Number]"


mdx_query = "
SELECT
	[Measures].[Case Count]
ON COLUMNS,

NON EMPTY {
[Case - IDs].[Case Combined ID].[Case Combined ID]
* [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
* [Case - Date - Earliest].[Epi-Year-Week].[Date]
* [Case - Date Earliest Based On].[Earliest Date Based On].[Earliest Date Based On]
* [Patient - Patient Master].[Gender].[Gender]
* [Case - Match Status].[Case Match Description].[Case Match Description]
* [Case - Disease].[Case Disease].[Case Disease]
* [Case - Surveillance Condition].[Surveillance Condition].[Surveillance Condition]
* [Pan - Date - Surveillance Reported].[Epi-Year-Week].[Date]
* [Pan - Date - Onset Symptom].[Date].[Date]
* [Pan - Investigation Classification].[Classification].[Classification]
* [Pan - Investigation Status].[Investigation Status].[Investigation Status]
* [Pan - Investigation Outcome].[Last Outcome].[Last Outcome]
* [Pan - Surveillance Region].[Pan Surveillance Region HA].[Pan Surveillance Region HA]
* [Pan - Surveillance Region].[Pan Surveillance Region HSDA].[Pan Surveillance Region HSDA]
* [Pan - Surveillance Region].[Pan Surveillance Region LHA].[Pan Surveillance Region LHA]
* [Pan - Address at Time of Case].[Address Location].[Address At Time Of Case Postal Code].ALLMEMBERS
* [Pan - Age At Surveillance Reported Date].[Age Years].[Age Years]
* [Pan - Age At Surveillance Reported Date].[Age Group 5].[Age Group 5]
* [Pan - Age At Surveillance Reported Date].[Age Group 10].[Age Group 10]
* [Pan - Etiologic Agent].[Etiologic Agent Level].[Etiologic Agent Level 2].ALLMEMBERS
* [Pan - Etiologic Agent].[Etiologic Agent Further Differentiation].[Etiologic Agent Further Differentiation]

* [Case - IDs].[FHA PARIS Client Id].[FHA PARIS Client Id]
* [Case - IDs].[VCH PARIS Client Id].[VCH PARIS Client Id]

}

DIMENSION PROPERTIES MEMBER_CAPTION,
[Case - IDs].[Case Combined ID].[Case ID]"


}

mdx_query = paste(mdx_query, system_ids, "ON ROWS")

#
# Get the filter clause for this MDX
#
mdx_filter_clause = enteric_filter_clause(query_start_date, query_end_date, parameter_list)

mdx_query = paste(mdx_query,mdx_filter_clause)

return (mdx_query)
}



#
# Function returns an mdx query for pulling the symptoms data from the Enteric cube
#
enteric_symptom_query <- function(query_start_date, query_end_date, parameter_list)
{
  mdx_query = "
SELECT
[Measures].[Case Count]
ON COLUMNS,

NON EMPTY {
[Case - IDs].[Case Combined ID].[Case Combined ID]
* [Pan - Symptom].[Symptom].[Symptom]
* [Pan - Symptom].[Symptom Response].[Symptom Response]
}

DIMENSION PROPERTIES MEMBER_CAPTION,
[Case - IDs].[Case Combined ID].[Case ID]

  ON ROWS"

#
# Get the filter clause for this MDX
#
mdx_filter_clause = enteric_filter_clause(query_start_date, query_end_date, parameter_list)

mdx_query = paste(mdx_query,mdx_filter_clause)

return (mdx_query)
}


#
# Function returns an mdx query for pulling the Risk Factor data from the Enteric cube
#
enteric_risk_factor_query <- function(query_start_date, query_end_date, parameter_list)
{
mdx_query = "
SELECT
[Measures].[Case Count]
ON COLUMNS,

NON EMPTY {
[Case - IDs].[Case Combined ID].[Case Combined ID]
* [Pan - Risk Factor - Investigation].[Risk Factor].[Risk Factor]
* [Pan - Risk Factor - Investigation].[Risk Factor Response].[Risk Factor Response]
}

DIMENSION PROPERTIES MEMBER_CAPTION,
[Case - IDs].[Case Combined ID].[Case ID]
  ON ROWS"

#
# Get the filter clause for this MDX
#
mdx_filter_clause = enteric_filter_clause(query_start_date, query_end_date, parameter_list)

mdx_query = paste(mdx_query,mdx_filter_clause)

return (mdx_query)
}



#
# Function returns an mdx query for pulling the UDF ALL data from the Enteric cube
#
enteric_udf_query <- function(query_start_date, query_end_date, parameter_list)
{
  mdx_query = "
SELECT
[Measures].[Case Count]
  ON COLUMNS,

  NON EMPTY {
  [Case - IDs].[Case Combined ID].[Case Combined ID]
  * [Pan - UDF - All].[UDF Question].[Question Keyword Common].ALLMEMBERS
  * [Pan - UDF - All].[Answer Value].[Answer Value]
  * [Pan - UDF - All].[Answer Row ID].[Answer Row ID]
  * [Pan - UDF - All].[Question Sort ID].[Question Sort ID]
  * [Pan - UDF - All].[UDF Template Version].[UDF Template Version]
  * [Pan - UDF - All].[UDF Name].[UDF Name]
  }
  DIMENSION PROPERTIES MEMBER_CAPTION,
  [Case - IDs].[Case Combined ID].[Case ID],
[Case - IDs].[Case Combined ID].[Disease Event ID]

  ON ROWS"

#
# Get the filter clause for this MDX
#
mdx_filter_clause = enteric_filter_clause(query_start_date, query_end_date, parameter_list)

mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}



#
# Funcation returns an mdx query for pulling the LIS data from the Enteric cube
#
enteric_lis_data_query <- function(query_start_date, query_end_date, parameter_list)
{

#
# Get the system id variable
#
retrieve_system_ids = parameter_list$retrieve_system_ids

#
# Build an mdx query
#
mdx_query = "
SELECT
[Measures].[Case Count]
  ON COLUMNS,

  NON EMPTY {
[Case - IDs].[Case Combined ID].[Case Combined ID]
* [LIS - Date - Collection Date].[Date].[Date]
* [LIS - Date - Order Entry].[Date].[Date]
* [LIS - Date - Receive Date].[Date].[Date]
* [LIS - Date - Result Date].[Date].[Date]
* [LIS - Age At Collection].[Age Years].[Age Years]
* [LIS - Flag - Proficiency Test].[Proficiency Test].[Proficiency Test]
* [LIS - Flag - Test Performed].[Test Performed].[Test Performed]
* [LIS - Specimen].[Specimen Description].[Specimen Description]
* [LIS - Microorganism].[Organism].[Serotype].ALLMEMBERS
* [LIS - Patient City].[Patient City Name].[Patient City Name]
* [LIS - Patient Health Authority].[Patient Health Authorities].[Patient Local Health Area].ALLMEMBERS
* [LIS - Result Attributes].[Organism Isolated].[Organism Isolated]
* [LIS - Result Attributes].[Result Full Description].[Result Full Description]
* [LIS - Result Attributes - Direct Exam].[Direct Exam Result Full Description].[Direct Exam Result Full Description]
* [LIS - Result Attributes - Direct Exam].[Shiga Toxin Result].[Shiga Toxin Result]
* [LIS - Test].[Order Code].[Order Code]
* [LIS - Test].[Test Code].[Test Code]
* [LIS - Copy To Provider 1].[Provider Types].[Provider Name].ALLMEMBERS
* [LIS - Order Entry Lab Location].[Lab Location].[Lab Location Code].ALLMEMBERS
* [LIS - Ordering Provider].[Provider Types].[Provider Name].ALLMEMBERS
* [LIS - Ordering Provider City].[Ordering Provider City Name].[Ordering Provider City Name]
* [LIS - Ordering Provider Health Authority].[Ordering Provider Health Authorities].[Ordering Provider Local Health Area].ALLMEMBERS
* [LIS - Bionumerics Result WGS].[WGS Cluster Code].[WGS Cluster Code]
* [LIS - Bionumerics Result].[Pfge Apai Pattern].[Pfge Apai Pattern]
* [LIS - Bionumerics Result].[Pfge Asci Pattern].[Pfge Asci Pattern]
* [LIS - Bionumerics Result].[Pfge Blnl Pattern].[Pfge Blnl Pattern]
* [LIS - Bionumerics Result].[Pfge Notl Pattern].[Pfge Notl Pattern]
* [LIS - Bionumerics Result].[Pfge Xbal Pattern].[Pfge Xbal Pattern]
* [LIS - Bionumerics Result].[Phage Type].[Phage Type]
}

DIMENSION PROPERTIES MEMBER_CAPTION,
[Case - IDs].[Case Combined ID].[Case ID]"

system_ids = ""
if(retrieve_system_ids == "Yes"){

  system_ids <- ",[Case - IDs].[Case Combined ID].[Container ID]
,[Case - IDs].[Case Combined ID].[Accession Number]
,[Case - IDs].[Case Combined ID].[Parent Container ID]"

}

mdx_query = paste(mdx_query, system_ids, "ON ROWS")

#
# Get the filter clause for this MDX
#
mdx_filter_clause = enteric_filter_clause(query_start_date, query_end_date, parameter_list)

mdx_query = paste(mdx_query,mdx_filter_clause)

return (mdx_query)
}



