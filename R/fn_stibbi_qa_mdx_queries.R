####################################################################
# fn_stibbi_qa_mdx_queries - mdx queries for pullling stibbi data - QA
#
# Each function contains the "Select" part of the query. The filter
# parameters are built separately
#
#
# Author: Darren Frizzell
# Created: 2019-10-09
#
# Modification: See Git Comments
#
####################################################################




#
# Function returns an mdx query for pulling case data from the STIBBI cube
#

stibbi_qa_case_query <- function(query_start_date, query_end_date, parameter_list)
{

  #
  # Get the mdx statement for a vector of PHS diseases
  #
  disease = parameter_list$disease

  #
  # Build disease specific checks. Used for including disease specific dimensions
  #
  hiv_check <- disease %in% "Human immunodeficiency virus (HIV) infection"
  aids_check <- disease %in% "Acquired immunodeficiency syndrome (AIDS)"
  syphilis_check <- any(disease %in% c("Syphilis","Syphilis (congenital)"))
  hcv_check <- disease %in% "Hepatitis C"
  chlamydia_check <- disease %in% "Chlamydia"
  gonorrhea_check <- disease %in% "Gonorrhea"


  hiv_aids_mdx <- ""
  syphilis_mdx <- ""
  hcv_mdx <- ""
  stiis_mdx <- ""

  #
  # Build the mdx for this dataset
  #
  mdx_query = "SELECT
	[Measures].[Case Count]
  ON COLUMNS,

  NON EMPTY {
  [Case - IDs].[Case Combined ID].[Case Combined ID]
  * [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  * [Patient - Patient Master].[Gender].[Gender]
  * [Patient - Patient Master].[Birth Year].[Birth Year]
  * [Case - Disease].[Case Disease].[Case Disease]
  * [Case - Date - Earliest].[Date].[Date]
  * [Case - Date - Earliest From].[Case Earliest Date From].[Case Earliest Date From]
  * [Case - Age at Earliest Date].[Age Years].[Age Years]
  * [Case - Age at Earliest Date].[Age Group 10].[Age Group 10]
  * [Case - Geo - Surveillance Region].[Case Surveillance Region Health Authorities].[Case Surveillance Region LHA].ALLMEMBERS
* [Case - Geo - Surveillance Region Based On].[Case Surveillance Region Based On].[Case Surveillance Region Based On]
  * [Case - Geo - Testing Region].[Case Testing Region Health Authorities].[Case Testing Region LHA]
  * [Case - Geo - Testing Region Based On].[Case Testing Region Based On].[Case Testing Region Based On]
  * [Case - Sources].[Case Source].[Case Source]
  * [Case - Status].[Case Status].[Case Status]
  * [Case - Status - LIS].[Case Status LIS].[Case Status LIS]
  * [Case - Status - PHS].[Case Status PHS].[Case Status PHS]
  * [Case - Status Confirmed By].[Case Status Confirmed By].[Case Status Confirmed By]
  * [Case - Status Determined By].[Case Status Determined By].[Case Status Determined By]"

  if(any(hcv_check)){
    hcv_mdx <- "* [Case - HCV - Flag Acute].[Yes No].[Yes No]
* [Case - HCV - Flag Acute From].[HCV Acute Flag From].[HCV Acute Flag From]
* [Case - HCV - Flag Reinfection].[Yes No].[Yes No]
* [Case - HCV - Interval - Seroconversion Days].[HCV Days Since Prior Negative].[HCV Days Since Prior Negative]"
  }



  #
  # Paste together the mdx variables to create the final query without the filter.
  #
  mdx_query <- paste(mdx_query, hcv_mdx, syphilis_mdx, stiis_mdx, hiv_aids_mdx,
                     " } DIMENSION PROPERTIES MEMBER_CAPTION ON ROWS")

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}





#
# Function returns an mdx query for pulling core panorama data from the STIBBI cube
#

stibbi_qa_investigation_query <- function(query_start_date, query_end_date, parameter_list)
{

  #
  # Get the mdx statement for a vector of PHS diseases
  #
  disease = parameter_list$disease

  #
  # Get the include indigenous identifiers variable
  #
  include_indigenous_identifiers = parameter_list$include_indigenous_identifiers

  #
  # Build disease specific checks. Used for including disease specific dimensions
  #
  hiv_check <- disease %in% "Human immunodeficiency virus (HIV) infection"
  aids_check <- disease %in% "Acquired immunodeficiency syndrome (AIDS)"
  syphilis_check <- any(disease %in% c("Syphilis","Syphilis (congenital)"))
  hcv_check <- disease %in% "Hepatitis C"
  chlamydia_check <- disease %in% "Chlamydia"
  gonorrhea_check <- disease %in% "Gonorrhea"
  lymphogranuloma_check <- disease %in% "Lymphogranuloma venereum"

  hiv_aids_mdx <- ""
  syphilis_mdx <- ""
  hcv_mdx <- ""
  stiis_mdx <- ""
  indigenous_ident_mdx <- ""

  #
  # Build the mdx for this dataset
  #
  mdx_query = "SELECT
	[Measures].[PHS Disease Event Count]
  ON COLUMNS,

  NON EMPTY {
  [PHS - IDs].[Disease Event ID].[Disease Event ID]
* [Case - IDs].[Case Combined ID].[Case Combined ID]
* [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
* [PHS - Client].[Client ID].[Client ID]
* [PHS - Classification].[PHS Classification].[PHS Classification]
* [PHS - Date - Surveillance].[Date].[Date]
* [PHS - Date - Surveillance Reported].[Date].[Date]
* [PHS - Disease].[PHS Disease].[PHS Disease]
* [PHS - Surveillance Condition].[Surveillance Condition].[Surveillance Condition]
* [PHS - Etiologic Agent].[Etiologic Agent Level 1].[Etiologic Agent Level 1]
* [PHS - Etiologic Agent].[Etiologic Agent Level 2].[Etiologic Agent Level 2]
* [PHS - Geo - Surveillance Region].[PHS Surveillance Region Health Authorities].[PHS Surveillance Region LHA].ALLMEMBERS
* [PHS - Geo - Surveillance Region Based On].[Surveillance Region Based On].[Surveillance Region Based On]
* [PHS - Geo - Client Address ATOC Region].[PHS Client Address ATOC Health Authorities].[PHS Client Address ATOC LHA].ALLMEMBERS
* [PHS - Geo - Client Address ATOC].[PHS Client Addresses ATOC].[PHS Client Address ATOC Postal Code].ALLMEMBERS
* [PHS - Geo - Earliest Positive Ordering Provider Region].[PHS Earliest Positive Ordering Provider Health Authorities].[PHS Earliest Positive Ordering Provider LHA].ALLMEMBERS
* [PHS - Geo - Earliest Positive Ordering Provider Address].[PHS Earliest Positive Ordering Provider Address].[PHS Earliest Positive Ordering Provider Postal Code].ALLMEMBERS
* [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Key]
* [PHS - Source System].[Source System].[Source System]
* [PHS - Gender Of Partners].[Gender Of Partners].[Gender Of Partners]
* [PHS - Sexual Orientation].[Sexual Orientation].[Sexual Orientation]
* [PHS - Age at Surveillance Date].[Age Group 10].[Age Group 10]
* [PHS - Age at Surveillance Date].[Age Years].[Age Years]
* [PHS - Stage of Infection].[Stage of Infection].[Stage of Infection]"

  if(include_indigenous_identifiers){
    indigenous_ident_mdx <- "* [PHS - Indigenous First Nations Status].[First Nations Status].[First Nations Status]
* [PHS - Indigenous Identity].[Indigenous Identity].[Indigenous Identity]
* [PHS - Indigenous On Reserve Administered By].[On Reserve Administered By].[On Reserve Administered By]
* [PHS - Indigenous Organization].[Indigenous Organization].[Indigenous Organization]
* [PHS - Indigenous Self Identify].[Indigenous Self Identify].[Indigenous Self Identify]
* [PHS - HAISYS On Reserve].[HAISYS On Reserve].[HAISYS On Reserve]"
  }

  if(any(hcv_check)){
    hcv_mdx <- "* [PHS - Stage of Infection - Earliest].[Earliest Stage of Infection].[Earliest Stage of Infection]"
  }

  if(any(syphilis_check)){
    syphilis_mdx <- "* [PHS - STI Stage of Infection Category].[Stage of Infection Category].[Stage of Infection Category]"
  }

  if(any(syphilis_check) | any(chlamydia_check) | any(gonorrhea_check) | any(lymphogranuloma_check)){

    stiis_mdx <- "* [PHS - STIIS Risk Category].[STIIS Risk Category].[STIIS Risk Category]
* [PHS - STI Body Site Category].[STI Body Site Category].[STI Body Site Category]
* [PHS - Flag - Pregnant at Time of Case].[Pregnant at Time of Case Flag].[Pregnant at Time of Case Flag]"
  }


  if(any(hiv_check) | any(aids_check)){
    hiv_aids_mdx <- "* [PHS - HIV - Client Stage of Infection].[HIV Client Stage of Infection].[HIV Client Stage of Infection]
* [PHS - HIV - Client Stage of Infection].[HIV Stage of Infection Based On].[HIV Stage of Infection Based On]
    * [PHS - HIV - Diagnosis Test Type].[HIV Diagnosis Test Type].[HIV Diagnosis Test Type]
    * [PHS - HIV - Exposure Category].[HAISYS Exposure Category].[HAISYS Exposure Category]
    * [PHS - HIV - Flag - Non Nominal].[HIV Non Nominal Flag].[HIV Non Nominal Flag]
    * [PHS - HIV - First AIDS Defining Illness].[First AIDS Defining Illnesses].[First AIDS Defining Illness].ALLMEMBERS
    * [PHS - HIV - Reporting Category].[HIV Reporting Category].[HIV Reporting Category]
    * [PHS - HIV - Source of Diagnosis].[HIV Source of Diagnosis].[HIV Source of Diagnosis]
    * [PHS - HIV - When AIDS Diagnosed].[When AIDS Diagnosed].[When AIDS Diagnosed]
    * [PHS - HIV - Flag - Prenatal Screen].[HIV Prenatal Screen Flag].[HIV Prenatal Screen Flag]"
  }

  #
  # Paste together the mdx variables to create the final query without the filter.
  #
  mdx_query <- paste(mdx_query, indigenous_ident_mdx, hcv_mdx, syphilis_mdx, stiis_mdx, hiv_aids_mdx,
                     " } DIMENSION PROPERTIES MEMBER_CAPTION,
                     [PHS - IDs].[Disease Event ID].[Investigation ID],")

  if(any(syphilis_check) | any(chlamydia_check) | any(gonorrhea_check) | any(lymphogranuloma_check)){
    mdx_query <- paste(mdx_query, "[PHS - IDs].[Disease Event ID].[EMR Form ID],")
  }

  mdx_query <- paste(mdx_query, "[PHS - IDs].[Disease Event ID].[Date - Investigation Created],
                     [PHS - IDs].[Disease Event ID].[Date - Investigation Updated],
                     [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Code],
                     [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Clinic Name],
                     [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Clinic Code],
                     [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Address Flag],
                     [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Name]")
  if(any(syphilis_check) | any(chlamydia_check) | any(gonorrhea_check) | any(lymphogranuloma_check)){
    mdx_query <- paste(mdx_query, ",[PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Emr Form Tab]")
  }
  mdx_query <- paste(mdx_query, "ON ROWS")

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}






#
# Function returns an mdx query for pulling panorama client data from the STIBBI cube.
# Function pulls clients for the specified disease
#

stibbi_qa_client_query <- function(query_start_date, query_end_date, parameter_list)
{

  #
  # Get the mdx statement for a vector of PHS diseases
  #
  disease = parameter_list$disease

  #
  # Build disease specific checks. Used for including disease specific dimensions
  #
  hiv_check <- disease %in% "Human immunodeficiency virus (HIV) infection"
  aids_check <- disease %in% "Acquired immunodeficiency syndrome (AIDS)"
  syphilis_check <- any(disease %in% c("Syphilis","Syphilis (congenital)"))
  hcv_check <- disease %in% "Hepatitis C"
  chlamydia_check <- disease %in% "Chlamydia"
  gonorrhea_check <- disease %in% "Gonorrhea"
  lymphogranuloma_check <- disease %in% "Lymphogranuloma venereum"


  hiv_aids_mdx <- ""
  syphilis_mdx <- ""
  hcv_mdx <- ""
  stiis_mdx <- ""




  mdx_query = "
SELECT
	[Measures].[PHS Client Count]
  ON COLUMNS,

  NON EMPTY {
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  * [Patient - Patient Master].[Birth Year].[Birth Year]
  * [PHS - Client].[Client ID].[Client ID]
  * [PHS - Client].[Ethnicity].[Ethnicity]
  * [PHS - Client].[Gender].[Gender]
  * [PHS - Client].[Gender Identity].[Gender Identity]
  * [PHS - Client].[Other Ethnicity].[Other Ethnicity]"

  if(any(hiv_check) | any(aids_check)){
    hiv_aids_mdx <- "
    * [PHS - Client].[HAISYS Chart Number].[HAISYS Chart Number]
    * [PHS - Client].[Haisys STI ID].[Haisys STI ID]"
  }

  if(any(syphilis_check) | any(chlamydia_check) | any(gonorrhea_check) | any(lymphogranuloma_check)){

    stiis_mdx <- "* [PHS - Client].[STIIS Client Number].[STIIS Client Number]
* [PHS - Client].[EMR Client ID].[EMR Client ID]"
  }

  mdx_query <- paste(mdx_query, hcv_mdx, syphilis_mdx, hiv_aids_mdx, stiis_mdx,
                     "}  ON ROWS")

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}


#
# Function returns an mdx query for pulling PHS Body Site data from the STIBBI cube.
#

stibbi_qa_body_site_query <- function(query_start_date, query_end_date, parameter_list)
{
  mdx_query = "SELECT
	[Measures].[PHS Disease Event Count]
  ON COLUMNS,

  NON EMPTY {
  [PHS - IDs].[Disease Event ID].[Disease Event ID]
  * [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  * [PHS - Client].[Client ID].[Client ID]
  * [PHS - Body Site and Drug Resistance].[Body Site Affected].[Body Site Affected]
  * [PHS - Body Site and Drug Resistance].[Sensitivity Interpretation].[Sensitivity Interpretation]
  * [PHS - Body Site and Drug Resistance].[Antimicrobial Drug].[Antimicrobial Drug]
  }

  DIMENSION PROPERTIES MEMBER_CAPTION,
  [PHS - IDs].[Disease Event ID].[Investigation ID]

  ON ROWS"

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}


#
# Function returns an mdx query for pulling PHS Cilent Risk Factor data from the STIBBI cube.
#

stibbi_qa_client_risk_factor_query <- function(query_start_date, query_end_date, parameter_list)
{
  mdx_query = "SELECT
	[Measures].[PHS Disease Event Count]
  ON COLUMNS,

  NON EMPTY {
  [PHS - IDs].[Disease Event ID].[Disease Event ID]
* [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
* [PHS - Client].[Client ID].[Client ID]
* [PHS - Risk Factor - Client].[Client Risk Factor].[Client Risk Factor]
* [PHS - Risk Factor - Client].[Client Risk Factor Response].[Client Risk Factor Response]
* [PHS - Risk Factor - Client].[Client Risk Factor Specify]
* [PHS - Risk Factor - Client].[Client Risk Factor End Reason].[Client Risk Factor End Reason]
* [PHS - Risk Factor - Client].[Date - Client Risk Factor Start].[Date - Client Risk Factor Start]
* [PHS - Risk Factor - Client].[Date - Client Risk Factor End].[Date - Client Risk Factor End]
* [PHS - Risk Factor - Client].[Date - Client Risk Factor Reported].[Date - Client Risk Factor Reported]
  }

  DIMENSION PROPERTIES MEMBER_CAPTION,
  [PHS - IDs].[Disease Event ID].[Investigation ID]

  ON ROWS"

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}


#
# Function returns an mdx query for pulling PHS Investigation Risk Factor data from the STIBBI cube.
#

stibbi_qa_investigation_risk_factor_query <- function(query_start_date, query_end_date, parameter_list)
{
  mdx_query = "SELECT
	[Measures].[PHS Disease Event Count]
  ON COLUMNS,

  NON EMPTY {
  [PHS - IDs].[Disease Event ID].[Disease Event ID]
* [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  * [PHS - Client].[Client ID].[Client ID]
  * [PHS - Risk Factor - Investigation].[Investigation Risk Factor].[Investigation Risk Factor]
  * [PHS - Risk Factor - Investigation].[Investigation Risk Factor Response].[Investigation Risk Factor Response]
  * [PHS - Risk Factor - Investigation].[Investigation Risk Factor Specify].[Investigation Risk Factor Specify]
  * [PHS - Risk Factor - Investigation].[Investigation Risk Factor End Reason].[Investigation Risk Factor End Reason]
  * [PHS - Risk Factor - Investigation].[Date - Investigation Risk Factor Start].[Date - Investigation Risk Factor Start]
  * [PHS - Risk Factor - Investigation].[Date - Investigation Risk Factor End].[Date - Investigation Risk Factor End]
  * [PHS - Risk Factor - Investigation].[Date - Investigation Risk Factor Reported].[Date - Investigation Risk Factor Reported]
  }

  DIMENSION PROPERTIES MEMBER_CAPTION,
  [PHS - IDs].[Disease Event ID].[Investigation ID]

  ON ROWS"

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}


#
# Function returns an mdx query for pulling PHS UDF All data from the STIBBI cube.
#

stibbi_qa_udf_long_query <- function(query_start_date, query_end_date, parameter_list)
{
  mdx_query = "SELECT
	[Measures].[PHS Disease Event Count]
ON COLUMNS,
NON EMPTY {

	[PHS - IDs].[Disease Event ID].[Disease Event ID]
	* [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
	* [PHS - Client].[Client ID].[Client ID]
	* [PHS - UDF - All].[UDF All Key].[UDF All Key]
 }
DIMENSION PROPERTIES MEMBER_CAPTION,
[PHS - IDs].[Disease Event ID].[Investigation ID],
[PHS - UDF - All].[UDF All Key].[Section Name],
[PHS - UDF - All].[UDF All Key].[Question Keyword Common],
[PHS - UDF - All].[UDF All Key].[Answer Value],
[PHS - UDF - All].[UDF All Key].[Answer Row ID],
[PHS - UDF - All].[UDF All Key].[Section Sort ID],
[PHS - UDF - All].[UDF All Key].[Question Sort ID],
[PHS - UDF - All].[UDF All Key].[UDF Name],
[PHS - UDF - All].[UDF All Key].[UDF Template Version]

ON ROWS"

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}



#
# Function returns an mdx query for pulling LIS data from the STIBBI cube.
#

stibbi_qa_lis_test_volume_query <- function(query_start_date, query_end_date, parameter_list)
{
  test_volume_mdx = "
  SELECT
NON EMPTY {
  [Measures].[LIS Test Count]
} ON COLUMNS,

  NON EMPTY { (
  [LIS - IDs].[Test ID].[Test ID]
  * [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  * [LIS - Patient].[Patient Key].[Patient Key]
  * [LIS - Infection Group].[Infection Group].[Infection Group]
  * [LIS - Date - Collection].[Date].[Date]
  * [LIS - Date - Receive].[Date].[Date]
  * [LIS - Date - Result].[Date].[Date]
  * [LIS - Age at Collection].[Age Years].[Age Years]
  * [LIS - Test].[Tests].[Test Name].ALLMEMBERS
  * [LIS - Test].[Order Code].[Order Code]
  * [LIS - Test].[Test Code].[Test Code]
  * [LIS - Test].[Source System ID].[Source System ID]
  * [LIS - Result Attributes].[Test Outcome].[Test Outcome]
  * [LIS - Result - Organism].[Organisms].[Organism Level 3].ALLMEMBERS
  * [LIS - Geo - Patient Region ATOT].[LIS Patient Health Authorities ATOT].[LIS Patient LHA ATOT].ALLMEMBERS
  * [LIS - Geo - Patient Region Proxy].[LIS Patient Health Authorities Proxy].[LIS Patient LHA Proxy].ALLMEMBERS
  * [LIS - Geo - Patient Address ATOT].[LIS Patient Addresses ATOT].[LIS Patient Postal Code ATOT].ALLMEMBERS
  * [LIS - Patient Location at Order].[Hospital Code].[Hospital Code]
  * [LIS - Patient Location at Order].[Location Type].[Location Type]
  * [LIS - Patient Location at Order].[Patient Location Code].[Patient Location Code]
  * [LIS - Patient Location at Order].[Patient Location Name].[Patient Location Name]
  * [LIS - Lab Location - Result].[Lab Locations].[Hospital Code]
  * [LIS - Lab Location - Result].[Lab Code].[Lab Code]
  * [LIS - Lab Location - Result].[Lab Name].[Lab Name]
  * [LIS - Lab Location - Result].[Lab Location Code].[Lab Location Code]
  * [LIS - Lab Location - Result].[Lab Location Name].[Lab Location Name]
  * [LIS - Lab Location - Result].[Lab Location Description].[Lab Location Description]
  * [LIS - Lab Location - Order Entry].[Lab Locations].[Hospital Code]
  * [LIS - Lab Location - Order Entry].[Lab Code].[Lab Code]
  * [LIS - Lab Location - Order Entry].[Lab Name].[Lab Name]
  * [LIS - Lab Location - Order Entry].[Lab Location Code].[Lab Location Code]
  * [LIS - Lab Location - Order Entry].[Lab Location Name].[Lab Location Name]
  * [LIS - Lab Location - Order Entry].[Lab Location Description].[Lab Location Description]
  * [LIS - Flag - Prenatal Test].[Prenatal Flag].[Prenatal Flag]
  * [LIS - Flag - Proficiency Test].[Yes No].[Yes No]
  * [LIS - Flag - Test Performed].[Test Performed].[Test Performed]
  * [LIS - Result Attributes].[Result Full Description].[Result Full Description]
  ) }
  DIMENSION PROPERTIES MEMBER_CAPTION,
  [LIS - IDs].[Test ID].[Episode ID]
  ,[LIS - IDs].[Test ID].[Accession Number]
  ,[LIS - IDs].[Test ID].[Container ID]
  ,[LIS - Patient].[Patient Key].[PHN]
  ,[LIS - Patient].[Patient Key].[Name First]
  ,[LIS - Patient].[Patient Key].[Name Last]
  ,[LIS - Patient].[Patient Key].[Birth Year]
  ,[LIS - Patient].[Patient Key].[Gender]

  ON ROWS"

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  test_volume_mdx = paste(test_volume_mdx,mdx_filter_clause)

  return (test_volume_mdx)
}


#
# Function returns an mdx query for pulling LIS Provider data from the STIBBI cube.
#
stibbi_qa_lis_test_provider_query <- function(query_start_date, query_end_date, parameter_list)
{
  mdx_query = "
  SELECT
NON EMPTY {
  [Measures].[LIS Test Count]
} ON COLUMNS,

  NON EMPTY { (
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  * [LIS - Patient].[Patient Key].[Patient Key]
  * [LIS - IDs].[Test ID].[Test ID]
  * [LIS - Date - Collection].[Date].[Date]
  * [LIS - Date - Receive].[Date].[Date]
  * [LIS - Date - Episode Start].[Date].[Date]
  * [LIS - Infection Group].[Infection Group].[Infection Group]
  * [LIS - Provider - Ordering].[Provider Key].[Provider Key]
  * [LIS - Geo - Ordering Provider Region ATOT].[LIS Ordering Provider Health Authorities ATOT].[LIS Ordering Provider LHA ATOT].ALLMEMBERS
  * [LIS - Geo - Ordering Provider Address ATOT].[LIS Ordering Provider Addresses ATOT].[LIS Ordering Provider Postal Code ATOT].ALLMEMBERS
  * [LIS - Provider - Copy To 1].[Provider Key].[Provider Key]
) }
DIMENSION PROPERTIES MEMBER_CAPTION,
[LIS - IDs].[Test ID].[Episode ID]
  ,[LIS - IDs].[Test ID].[Accession Number]
  ,[LIS - IDs].[Test ID].[Container ID]
  ,[Patient - Patient Master].[Patient Master Key].[Birth Year]
  ,[LIS - Provider - Ordering].[Provider Key].[Provider Code]
  ,[LIS - Provider - Ordering].[Provider Key].[Provider Type]
  ,[LIS - Provider - Ordering].[Provider Key].[Provider Group]
  ,[LIS - Provider - Ordering].[Provider Key].[Provider Name]
  ,[LIS - Provider - Ordering].[Provider Key].[Full Address]
  ,[LIS - Provider - Copy To 1].[Provider Key].[Provider Code]
  ,[LIS - Provider - Copy To 1].[Provider Key].[Provider Type]
  ,[LIS - Provider - Copy To 1].[Provider Key].[Provider Group]
  ,[LIS - Provider - Copy To 1].[Provider Key].[Provider Name]
  ,[LIS - Provider - Copy To 1].[Provider Key].[Full Address]
  ,[LIS - Provider - Copy To 2].[Provider Key].[Provider Code]
  ,[LIS - Provider - Copy To 2].[Provider Key].[Provider Type]
  ,[LIS - Provider - Copy To 2].[Provider Key].[Provider Group]
  ,[LIS - Provider - Copy To 2].[Provider Key].[Provider Name]
  ,[LIS - Provider - Copy To 2].[Provider Key].[Full Address]
  ,[LIS - Provider - Copy To 3].[Provider Key].[Provider Code]
  ,[LIS - Provider - Copy To 3].[Provider Key].[Provider Type]
  ,[LIS - Provider - Copy To 3].[Provider Key].[Provider Group]
  ,[LIS - Provider - Copy To 3].[Provider Key].[Provider Name]
  ,[LIS - Provider - Copy To 3].[Provider Key].[Full Address]

ON ROWS"

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}


#
# Function returns an mdx query for pulling LIS Episode data from the STIBBI cube.
#
stibbi_qa_lis_episode_query <- function(query_start_date, query_end_date, parameter_list)
{
  mdx_query = "SELECT
NON EMPTY {
  [Measures].[LIS Episode Count]
} ON COLUMNS,

NON EMPTY { (
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  * [Patient - Patient Master].[Birth Year].[Birth Year]
  * [LIS - IDs].[Test ID].[Test ID]
  * [Case - IDs].[Case Combined ID].[Case Combined ID]
  * [LIS - Infection Group].[Infection Group].[Infection Group]
  * [LIS - Date - Episode Start].[Date].[Date]
  * [LIS - Date - Episode End].[Date].[Date]
  * [LIS - Gender at Episode Start].[Gender].[Gender]
  * [LIS - Gender at Episode Start].[Gender Code].[Gender Code]
  * [LIS - Age at Episode Start].[Age Years].[Age Years]
  * [LIS - Age at Episode Start].[Age Group 10].[Age Group 10]
  * [LIS - Geo - Patient Region].[LIS Patient Health Authorities].[LIS Patient LHA].ALLMEMBERS
  * [LIS - Geo - Patient Region Proxy].[LIS Patient Health Authorities Proxy].[LIS Patient LHA Proxy].ALLMEMBERS
  * [LIS - Geo - Patient Address].[LIS Patient Addresses].[LIS Patient Postal Code].ALLMEMBERS
  * [LIS - Geo - Ordering Provider Region].[LIS Ordering Provider Health Authorities].[LIS Ordering Provider LHA].ALLMEMBERS
  * [LIS - Geo - Ordering Provider Region Proxy].[LIS Ordering Provider Health Authorities Proxy].[LIS Ordering Provider LHA Proxy].ALLMEMBERS
  * [LIS - Geo - Ordering Provider Address].[LIS Ordering Provider Addresses].[LIS Ordering Provider Postal Code].ALLMEMBERS
  * [LIS - Episode Testing Pattern].[Episode Testing Pattern].[Episode Testing Pattern]
  * [LIS - Flag - Prenatal Episode].[Prenatal Flag].[Prenatal Flag]
  * [LIS - Flag - Reinfection].[Yes No].[Yes No]
  * [LIS - Flag - Repeat Tester].[Yes No].[Yes No]
  * [LIS - Interval - Days Since First Episode].[Interval in Days].[Interval in Days]
  * [LIS - Interval - Days Since First Episode].[Interval Group 1].[Interval Group 1]
  * [LIS - Interval - Days Since Prior Episode].[Interval in Days].[Interval in Days]
  * [LIS - Interval - Days Since Prior Episode].[Interval Group 1].[Interval Group 1]
  * [LIS - Interval - Days Since Prior Pos or Neg Episode].[Interval in Days].[Interval in Days]
  * [LIS - Interval - Days Since Prior Pos or Neg Episode].[Interval Group 1].[Interval Group 1]
  * [LIS - Rule Engine - Episode Status].[Rule Engine - Episode Status].[Rule Detail Description].ALLMEMBERS
) }
DIMENSION PROPERTIES MEMBER_CAPTION,
  [LIS - IDs].[Test ID].[Episode ID]
  ,[LIS - IDs].[Test ID].[Episode Seq]

ON ROWS "

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}


#
# Function returns an mdx query for pulling LIS POC data from the STIBBI cube.
#

stibbi_qa_lis_poc_query <- function(query_start_date, query_end_date, parameter_list)
{
  #
  # Build the mdx query for the case investigaiton dataset
  #

  mdx_query = "SELECT
NON EMPTY {
  [Measures].[POC Test Count],
  [Measures].[Non Reactive POC Test Count],
  [Measures].[Indeterminate POC Test Count],
  [Measures].[Reactive POC Test Count]
} ON COLUMNS,

NON EMPTY { (
	[LIS - Infection Group].[Infection Group].[Infection Group]
	* [LIS - POC - Test Location].[Health Authority].[Health Authority]
	* [LIS - POC - Test Location].[HSDA].[HSDA]
	* [LIS - POC - Test Location].[City].[City]
	* [LIS - POC - Test Location].[Site Name].[Site Name]
	* [LIS - Date - POC Reporting].[Date].[Date]
) }

ON ROWS"

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = filter_clause_olap(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  return (mdx_query)
}






