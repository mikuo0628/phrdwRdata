####################################################################
# fn_vpd_mdx_queries - mdx queries for pull vpd cube data
#
# Each funcion contains the "Select" part of the query. The filter
# parameters are built separately
#
#
# Author: Darren Frizzell
# Created: 2018-04-04
#
####################################################################



#
# Function returns the filter/where condition for an mdx query
#
vpd_filter_clause <- function(query_start_date, query_end_date, parameter_list)
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
  classification = parameter_list$classification

  classification_mdx <- mdx_dimension_filter(dimension_name = "Case - Classification - PHS",
                                             dimension_name_level_2 = "Case Classification",
                                             dimension_attributes = classification)


  #
  # Get the mdx statement for a vector of Patient / Surveillance HA's
  #
  health_region_ha = parameter_list$surveillance_region_ha

  health_region_ha_mdx <- mdx_dimension_filter(dimension_name = "Case - Geo - Surveillance Region",
                                               dimension_name_level_2 = "Case Surveillance Region HA",
                                               dimension_attributes = health_region_ha)


  #
  # Get the mdx statement for a vector of Patient LIS Status
  #
  lis_status = parameter_list$lis_status

  lis_status_mdx <- mdx_dimension_filter(dimension_name = "Case - Status - LIS",
                                               dimension_name_level_2 = "Case Status LIS",
                                               dimension_attributes = lis_status)



  filter_clause =paste("FROM ( SELECT (",disease_mdx, classification_mdx, health_region_ha_mdx, lis_status_mdx,
"{
	[Case - Classification - PHS].[Case Classification Group].&[Case],
[Case - Classification - PHS].[Case Classification Group].&[No PHS Data]
},
[Patient - Match Level].[Match Level Description].&[3],
{
[Case - Date].[Date].&[",
  query_start_date,"T00:00:00]:[Case - Date].[Date].&[",
  query_end_date,"T00:00:00]}) ON COLUMNS FROM [VPD])",
  sep="")

  return(filter_clause)
}


#
# Function returns an mdx query for pulling Case and Core panorama data from the VPD cube
#

vpd_case_investigation_query <- function(query_start_date, query_end_date, parameter_list, disease_check_list)
{
#
# Build the mdx query for the case investigaiton dataset
#
  mdx_query = "SELECT
[Measures].[Case Count]
ON COLUMNS,
NON EMPTY {

[Patient - Patient Master].[Patient Master Key].[Patient Master Key]
* [Case - IDs].[Case ID].[Case ID]
* [Case - IDs].[Investigation ID].[Investigation ID]
* [Case - IDs].[Disease Event ID].[Disease Event ID]
* [Case - IDs].[PHS Client ID].[PHS Client ID]
* [Case - IDs].[VCH PARIS Client Id].[VCH PARIS Client Id]
* [Case - IDs].[FHA PARIS Client Id].[FHA PARIS Client Id]
* [Case - IDs].[PARIS Assessment Number].[PARIS Assessment Number]
* [Case - Date].[Date].[Date]
* [Case - Date].[Epi-Year].[Epi-Year]
* [Case - Date].[Epi-Week].[Epi-Week]
* [Case - Date - Based On].[Case Date Based On].[Case Date Based On]
* [Case - Disease].[Case Disease].[Case Disease]"

if(disease_check_list$group_b_strep_check |
   disease_check_list$igas_check |
   disease_check_list$ipd_check |
   disease_check_list$bordetella_check |
   disease_check_list$meningo_check |
   disease_check_list$reportable_vpd_check){
  mdx_query <- paste(mdx_query, "* [Case - Serotype].[Case Serotype].[Case Serotype]")
}

mdx_query <- paste(mdx_query, "* [Patient - Patient Master].[Gender].[Gender]
* [Patient - Patient Master].[Birth Date].[Birth Date]
* [Case - Age at Case Date].[Age Group 10].[Age Group 10]
* [Case - Age at Case Date].[Age Year].[Age Year]
* [Case - Age at Case Date].[Age Month].[Age Month]")


if(disease_check_list$group_b_strep_check |
   disease_check_list$other_disease_check){
  mdx_query <- paste(mdx_query, "* [Case - Age at Case Date].[Age Day].[Age Day]
  * [Case - Age at Case Date].[NGBS].[Age Group Ngbs 2].ALLMEMBERS")
}

if(disease_check_list$ipd_check){
  mdx_query <- paste(mdx_query, "* [Case - Age at Case Date].[IPD].[Age Group Ipd 2].ALLMEMBERS")
}


if(disease_check_list$mumps_check){
  mdx_query <- paste(mdx_query, "* [Case - Age at Case Date].[Age Group Mumps].[Age Group Mumps]")
}


if(disease_check_list$bordetella_check){
  mdx_query <- paste(mdx_query, "* [Case - Age at Case Date].[Age Group Pertussis].[Age Group Pertussis]
  * [PHS - Date - Paroxysmal Onset].[Date].[Date]")
}


mdx_query <- paste(mdx_query, "* [Case - Sources].[Case Source].[Case Source]")

if(disease_check_list$group_b_strep_check |
   disease_check_list$igas_check |
   disease_check_list$ipd_check |
   disease_check_list$measles_check |
   disease_check_list$mumps_check |
   disease_check_list$rubella_check |
   disease_check_list$bordetella_check |
   disease_check_list$meningo_check |
   disease_check_list$reportable_vpd_check){
  mdx_query <- paste(mdx_query, "* [Case - Status - LIS].[Case Status LIS].[Case Status LIS]")
}

mdx_query <- paste(mdx_query, "* [Case - Classification - PHS].[Case Classifications].[Case Classification].ALLMEMBERS
* [Case - Status].[Case Status].[Case Status]
* [Case - Geo - Surveillance Region].[Case Surveillance Region Health Authorities].[Case Surveillance Region LHA].ALLMEMBERS
* [Case - Geo - Surveillance Region Based On].[Case Surveillance Region Based On].[Case Surveillance Region Based On]
* [PHS - Geo - Client Address ATOC].[PHS Client Addresses ATOC].[PHS Client Address ATOC Postal Code].ALLMEMBERS")

if(disease_check_list$group_b_strep_check |
   disease_check_list$igas_check |
   disease_check_list$ipd_check |
   disease_check_list$measles_check |
   disease_check_list$mumps_check |
   disease_check_list$rubella_check |
   disease_check_list$bordetella_check |
   disease_check_list$meningo_check |
   disease_check_list$reportable_vpd_check){
  mdx_query <- paste(mdx_query, "* [Case - Discordant Status].[Discordant Classification].[Discordant Classification]
* [Case - Discordant Status].[Discordant Etiologic Agent 2].[Discordant Etiologic Agent 2]")
}

if(disease_check_list$bordetella_check){
  mdx_query <- paste(mdx_query, "* [Case - Discordant Status].[Discordant Infection Group].[Discordant Infection Group]")
}

mdx_query <- paste(mdx_query, "* [PHS - Etiologic Agent].[Etiologic Agent Levels].[Etiologic Agent Level 2].ALLMEMBERS
* [PHS - Etiologic Agent].[Etiologic Agent Further Differentiation].[Etiologic Agent Further Differentiation]
* [PHS - Date - Onset Symptom].[Date].[Date]
* [PHS - Date - Surveillance Reported].[Date].[Date]
* [PHS - Symptom - All].[Onset Symptom].[Onset Symptom]")

if(disease_check_list$group_b_strep_check |
   disease_check_list$igas_check |
   disease_check_list$ipd_check |
   disease_check_list$measles_check |
   disease_check_list$mumps_check |
   disease_check_list$rubella_check |
   disease_check_list$bordetella_check |
   disease_check_list$meningo_check |
   disease_check_list$reportable_vpd_check){
  mdx_query <- paste(mdx_query, "* [PHS - Investigation Last Outcome].[Last Outcome].[Last Outcome]")
}

if(disease_check_list$ipd_check |
   disease_check_list$measles_check |
   disease_check_list$mumps_check |
   disease_check_list$bordetella_check |
   disease_check_list$rubella_check |
   disease_check_list$meningo_check |
   disease_check_list$reportable_vpd_check){
  mdx_query <- paste(mdx_query, "* [PHS - Date - Last Outcome].[Date].[Date]")
}

if(disease_check_list$igas_check |
   disease_check_list$ipd_check |
   disease_check_list$measles_check |
   disease_check_list$mumps_check |
   disease_check_list$bordetella_check |
   disease_check_list$rubella_check |
   disease_check_list$meningo_check |
   disease_check_list$reportable_vpd_check){
  mdx_query <- paste(mdx_query, "* [PHS - Investigation Last Outcome].[Cause of Death].[Cause of Death]")
}


if(disease_check_list$igas_check |
   disease_check_list$reportable_vpd_check){
  mdx_query <- paste(mdx_query, "* [PHS - Stage of Infection].[Stage of Infection].[Stage of Infection]")
}

if(disease_check_list$measles_check |
   disease_check_list$mumps_check |
   disease_check_list$rubella_check){
  mdx_query <- paste(mdx_query, "* [PHS - Complication].[Encephalitis].[Encephalitis]
* [PHS - Complication].[Meningitis].[Meningitis]")
}

if(disease_check_list$mumps_check){
  mdx_query <- paste(mdx_query, "* [PHS - Complication].[Permanent Hearing Loss].[Permanent Hearing Loss]")
}


if(disease_check_list$measles_check){
  mdx_query <- paste(mdx_query, "* [PHS - Complication].[Pneumonia].[Pneumonia]")
}

mdx_query <- paste(mdx_query, "} ON ROWS")



#
# Get the filter clause for this MDX
#
mdx_filter_clause = vpd_filter_clause(query_start_date, query_end_date, parameter_list)

mdx_query = paste(mdx_query,mdx_filter_clause)

return (mdx_query)

}







#
# Function returns an mdx query for pulling Panorama Symptoms data from the VPD cube
#

vpd_symptoms_query <- function(query_start_date, query_end_date, parameter_list, disease_check_list)
{


  #
  # Build the mdx query for the Symptoms dataset
  #

  mdx_query <- ""

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$rubella_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check){

mdx_query <- "SELECT	[Measures].[Case Count]  ON COLUMNS,
NON EMPTY ([Case - IDs].[Case ID].[Case ID]
* [Case - IDs].[Investigation ID].[Investigation ID]
* [Case - Disease].[Case Disease].[Case Disease]"

  }

  if(disease_check_list$measles_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Arthralgia].[Arthralgia]")
  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Arthritis].[Arthritis]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$meningo_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Bacteremia].[Bacteremia]")
  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Cellulitis].[Cellulitis]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Conjunctivitis].[Conjunctivitis]")
  }

  if(disease_check_list$measles_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Coryza].[Coryza]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$bordetella_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Cough].[Cough]")
  }

  if(disease_check_list$bordetella_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Cough Associated With Apnea].[Cough Associated With Apnea]
  * [PHS - Symptom].[Cough Ending In Inspriatory Whoop].[Cough Ending In Inspriatory Whoop]
  * [PHS - Symptom].[Cough Ending In Vomiting Or Gagging].[Cough Ending In Vomiting Or Gagging]
  * [PHS - Symptom].[Cough Lasting More Than 2 Weeks].[Cough Lasting More Than 2 Weeks]
  * [PHS - Symptom].[Cough Paroxysmal].[Cough Paroxysmal]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$rubella_check |
     disease_check_list$bordetella_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Fever].[Fever]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Lymphadenopathy].[Lymphadenopathy]")
  }

  if(disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Lymphadenopathy Occipital].[Lymphadenopathy Occipital]
  * [PHS - Symptom].[Lymphadenopathy Post - Auricular].[Lymphadenopathy Post - Auricular]
  * [PHS - Symptom].[Lymphadenopathy Posterior Cervical].[Lymphadenopathy Posterior Cervical]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$meningo_check |
     disease_check_list$ipd_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Meningitis].[Meningitis]")
  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Necrotizing Fasciiti - Myositis - Gangrene].[Necrotizing Fasciiti - Myositis - Gangrene]")
  }

  if(disease_check_list$mumps_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Orchitis].[Orchitis]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Other].[Other]")
  }

  if(disease_check_list$mumps_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Parotitis Bilateral].[Parotitis Bilateral]
  * [PHS - Symptom].[Parotitis Unilateral].[Parotitis Unilateral]")
  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Peri - Partum Fever].[Peri - Partum Fever]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Pneumonia].[Pneumonia]")
  }


  if(disease_check_list$meningo_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Purpura Fulminans - Meningococcemia].[Purpura Fulminans - Meningococcemia]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Rash Maculopapular].[Rash Maculopapular]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$meningo_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Rash Petechial].[Rash Petechial]")
  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - Symptom].[Toxic Shock Syndrome].[Toxic Shock Syndrome]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, ") ON ROWS")


    #
    # Get the filter clause for this MDX
    #

    mdx_filter_clause = vpd_filter_clause(query_start_date, query_end_date, parameter_list)

    mdx_query = paste(mdx_query,mdx_filter_clause)

  }



  return (mdx_query)
}



#
# Function returns an mdx query for pulling Panorama Symptoms data from the VPD cube
#

vpd_symptoms_long_query <- function(query_start_date, query_end_date, parameter_list, disease_check_list)
{


  #
  # Build the mdx query for the Symptoms dataset
  #

  mdx_query <- ""

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$rubella_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check){

    mdx_query <- "SELECT	[Measures].[Case Count]  ON COLUMNS,
    NON EMPTY ([Case - IDs].[Case ID].[Case ID]
    * [Case - IDs].[Investigation ID].[Investigation ID]
    * [Case - Disease].[Case Disease].[Case Disease]
    * [PHS - Symptom - All].[Symptom].[Symptom]
	* [PHS - Symptom - All].[Symptom Response].[Symptom Response]
	* [PHS - Symptom - All].[Symptom Date].[Symptom Date]) ON ROWS"


    #
    # Get the filter clause for this MDX
    #

    mdx_filter_clause = vpd_filter_clause(query_start_date, query_end_date, parameter_list)

    mdx_query = paste(mdx_query,mdx_filter_clause)

  }



  return (mdx_query)
}



#
# Function returns an mdx query for pulling PHS Risk Factor data from the VPD cube
#

vpd_risk_factor_query <- function(query_start_date, query_end_date, parameter_list, disease_check_list)
{

  #
  # Build the mdx query for the Risk Factor dataset
  #

  mdx_query <- ""


  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check){

    mdx_query <- "SELECT	[Measures].[Case Count]  ON COLUMNS,
    NON EMPTY ([Case - IDs].[Case ID].[Case ID]
    * [Case - IDs].[Investigation ID].[Investigation ID]
    * [Case - Disease].[Case Disease].[Case Disease]"

  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Alcohol Use].[Alcohol Use]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Chronic Cardiac Condition].[Chronic Cardiac Condition]")
  }


  if(disease_check_list$ipd_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Chronic CSF Leak].[Chronic CSF Leak]
  * [PHS - Risk Factor].[Chronic Liver Disease].[Chronic Liver Disease]
  * [PHS - Risk Factor].[Chronic Liver Disease Specify].[Chronic Liver Disease Specify]
  * [PHS - Risk Factor].[Chronic Renal Disease].[Chronic Renal Disease]
  * [PHS - Risk Factor].[Cystic Fibrosis].[Cystic Fibrosis]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Chronic Respiratory Pulmonary Condition].[Chronic Respiratory Pulmonary Condition]
  * [PHS - Risk Factor].[Diabetes Mellitus].[Diabetes Mellitus]")
  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Homelessness Underhoused].[Homelessness Underhoused]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$meningo_check |
     disease_check_list$ipd_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Immunocompromised Other].[Immunocompromised Other]
  * [PHS - Risk Factor].[Immunocompromised Other Specify].[Immunocompromised Other Specify]
  * [PHS - Risk Factor].[Immunocompromised Transplant].[Immunocompromised Transplant]
  * [PHS - Risk Factor].[Immunocompromised Transplant Specify].[Immunocompromised Transplant Specify]
  * [PHS - Risk Factor].[Immunocompromised Treatment].[Immunocompromised Treatment]
  * [PHS - Risk Factor].[Immunocompromised Treatment Specify].[Immunocompromised Treatment Specify]
  * [PHS - Risk Factor].[Immunocompromising Condition].[Immunocompromising Condition]
  * [PHS - Risk Factor].[Immunocompromising Condition Specify].[Immunocompromising Condition Specify]")
  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Injection Drug Use].[Injection Drug Use]")
  }

  if(disease_check_list$ipd_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Malignancies Cancer].[Malignancies Cancer]")
  }


  if(disease_check_list$meningo_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[MSM].[MSM]")
  }


  if(disease_check_list$igas_check |
     disease_check_list$meningo_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$ipd_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Other].[Other]
  * [PHS - Risk Factor].[Other Specify].[Other Specify]")
  }

  if(disease_check_list$bordetella_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Pregnancy].[Pregnancy]")
  }


  if(disease_check_list$ipd_check){
    mdx_query <- paste(mdx_query, "* [PHS - Risk Factor].[Sickle Cell Disease].[Sickle Cell Disease]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, ") ON ROWS")


    #
    # Get the filter clause for this MDX
    #

    mdx_filter_clause = vpd_filter_clause(query_start_date, query_end_date, parameter_list)

    mdx_query = paste(mdx_query,mdx_filter_clause)

  }

  return (mdx_query)
}




#
# Function returns an mdx query for pulling PHS UDF data from the VPD cube
#

vpd_udf_query <- function(query_start_date, query_end_date, parameter_list, disease_check_list)
{

  #
  # Build the mdx query for the UDF dataset
  #

  mdx_query <- ""


  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){

    mdx_query <- "SELECT	[Measures].[Case Count]  ON COLUMNS,
    NON EMPTY ([Case - IDs].[Case ID].[Case ID]
    * [Case - IDs].[Investigation ID].[Investigation ID]
    * [Case - Disease].[Case Disease].[Case Disease]"

  }

  if(disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Immunization].[Immunization Status Prior To Onset].[Immunization Status Prior To Onset]")
  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Pregnancy].[Pregnancy Outcome].[Pregnancy Outcome]
  * [PHS - UDF - Pregnancy].[Pregnancy Was Infant Affected].[Pregnancy Was Infant Affected]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$bordetella_check |
     disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Hospitalization].[ER Visit].[ER Visit]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$bordetella_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Hospitalization].[ER Visit Hospital Name].[ER Visit Hospital Name]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Hospitalization].[Admitted To Hospital].[Admitted To Hospital]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$bordetella_check |
     disease_check_list$mumps_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Hospitalization].[Name Of Hospital For Admission].[Name Of Hospital For Admission]")
  }

  if(disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Hospitalization].[Admission Date].[Admission Date]
  * [PHS - UDF - Hospitalization].[Admitted To ICU].[Admitted To ICU]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$bordetella_check |
     disease_check_list$mumps_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Hospitalization].[ICU Hospital Name].[ICU Hospital Name]")
  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Hospitalization].[Surgery].[Surgery]
  * [PHS - UDF - Predisposing Conditions].[Predisposing Condition Chickenpox].[Predisposing Condition Chickenpox]
  * [PHS - UDF - Predisposing Conditions].[Predisposing Condition Skin Infection].[Predisposing Condition Skin Infection]
  * [PHS - UDF - Predisposing Conditions].[Predisposing Condition Wound].[Predisposing Condition Wound]
  * [PHS - UDF - Predisposing Conditions].[Wound Type].[Wound Type]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Exposures].[Contact With Known Case].[Contact With Known Case]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$mumps_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Exposures].[Did Case Travel During Incubation Period].[Did Case Travel During Incubation Period]
  * [PHS - UDF - Exposures].[Where Case Travelled During Incubation Period].[Where Case Travelled During Incubation Period]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$rubella_check |
     disease_check_list$mumps_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Exposures].[Travel Locations During Incubation Period].[Travel Locations During Incubation Period]
  * [PHS - UDF - Exposures].[Source Of Infection].[Source Of Infection]")
  }

  if(disease_check_list$igas_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Exposures].[Hospital Associated Infection].[Hospital Associated Infection]
  * [PHS - UDF - Exposures].[Hospital Associated Infection Specify].[Hospital Associated Infection Specify]")
  }


  if(disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Exposures].[Previous Pregnancies].[Previous Pregnancies]")
  }


  if(disease_check_list$measles_check |
     disease_check_list$rubella_check |
     disease_check_list$mumps_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Settings].[Healthcare Worker].[Healthcare Worker]
  * [PHS - UDF - Settings].[Setting Name Type Location].[Setting Name Type Location]")
  }

  if(disease_check_list$measles_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$igas_check |
     disease_check_list$mumps_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Settings].[Child Care School University].[Child Care School University]
  * [PHS - UDF - Settings].[Lives In Communal Setting].[Lives In Communal Setting]")
  }


  if(disease_check_list$bordetella_check){
    mdx_query <- paste(mdx_query, "  * [PHS - UDF - Contacts].[Infants Under One Year].[Infants Under One Year]
  * [PHS - UDF - Contacts].[Pregnant Women Third Trimester].[Pregnant Women Third Trimester]
  * [PHS - UDF - Contacts].[Household Contact].[Household Contact]
  * [PHS - UDF - Contacts].[Family Daycare Contacts].[Family Daycare Contacts]")
  }


  if(disease_check_list$measles_check |
     disease_check_list$rubella_check |
     disease_check_list$mumps_check){
    mdx_query <- paste(mdx_query, "  * [PHS - UDF - Travel Communicability Period].[Did Case Travel During Communicability].[Did Case Travel During Communicability]
  * [PHS - UDF - Travel Communicability Period].[Where Case Travelled During Communicability].[Where Case Travelled During Communicability]")
  }


  if(disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check){
    mdx_query <- paste(mdx_query, "* [PHS - UDF - Notes].[General Comments].[General Comments]")
  }

  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){
    mdx_query <- paste(mdx_query, ") ON ROWS")


    #
    # Get the filter clause for this MDX
    #

    mdx_filter_clause = vpd_filter_clause(query_start_date, query_end_date, parameter_list)

    mdx_query = paste(mdx_query,mdx_filter_clause)

  }

  return (mdx_query)
}


#
# Function returns an mdx query for pulling Panorama UDF data from the VPD cube
#

vpd_udf_long_query <- function(query_start_date, query_end_date, parameter_list, disease_check_list)
{

  #
  # Build the mdx query for the UDF dataset
  #
  mdx_query <- ""


  if(disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){

    mdx_query <- "SELECT	[Measures].[Case Count]  ON COLUMNS,
NON EMPTY {
[Case - IDs].[Case ID].[Case ID]
* [Case - IDs].[Investigation ID].[Investigation ID]
* [Case - Disease].[Case Disease].[Case Disease]
* [PHS - UDF - All].[UDF All Key].[UDF All Key]
}
DIMENSION PROPERTIES MEMBER_CAPTION,
[PHS - UDF - All].[UDF All Key].[Answer Row ID],
[PHS - UDF - All].[UDF All Key].[Answer Value],
[PHS - UDF - All].[UDF All Key].[Question Keyword Common],
[PHS - UDF - All].[UDF All Key].[Question Sort ID],
[PHS - UDF - All].[UDF All Key].[Section Name],
[PHS - UDF - All].[UDF All Key].[Section Sort ID],
[PHS - UDF - All].[UDF All Key].[Form Name],
[PHS - UDF - All].[UDF All Key].[Form Template Version]
ON ROWS"


    #
    # Get the filter clause for this MDX
    #

    mdx_filter_clause = vpd_filter_clause(query_start_date, query_end_date, parameter_list)

    mdx_query = paste(mdx_query,mdx_filter_clause)

  }

  return (mdx_query)
}





#
# Function returns an mdx query for pulling Immunization data from the VPD cube
#

vpd_immunizations_query <- function(query_start_date, query_end_date, parameter_list, disease_check_list)
{

  #
  # Build the mdx query for the Immunizations dataset
  #

  mdx_query <- ""


  if(disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){

    mdx_query <- "SELECT
	[Measures].[Case Count]
  ON COLUMNS,
  NON EMPTY (
  [Case - IDs].[Case ID].[Case ID]
  * [Case - IDs].[Investigation ID].[Investigation ID]
  * [Case - Disease].[Case Disease].[Case Disease]
  * [PHS - Imms - Investigation].[Immunization Id].[Immunization Id]
  * [PHS - Imms - Investigation].[Administered Date].[Administered Date]
  * [PHS - Imms - Investigation].[Administered Date Estimated Flag].[Administered Date Estimated Flag]
  * [PHS - Imms - Age at Time of Dose].[Age Year].[Age Year]
  * [PHS - Imms - Age at Time of Dose].[Age Month].[Age Month]
  * [PHS - Imms - Age at Time of Dose].[Age Day].[Age Day]
  * [PHS - Imms - Investigation].[Agent].[Agent]
  * [PHS - Imms - Investigation].[Antigen].[Antigen]
  * [PHS - Imms - Investigation].[Antigen Status].[Antigen Status]
  * [PHS - Imms - Investigation].[Agent Dose Number].[Agent Dose Number]
  * [PHS - Imms - Investigation].[Dose Status].[Dose Status]
  * [PHS - Imms - Investigation].[Historical Flag].[Historical Flag]
  * [PHS - Imms - Investigation].[Immunization Reason].[Immunization Reason]
  * [PHS - Imms - Investigation].[Override Flag].[Override Flag]
  * [PHS - Imms - Investigation].[Agent Revised Dose Number].[Agent Revised Dose Number]
  * [PHS - Imms - Investigation].[Agent Dose Number Derived].[Agent Dose Number Derived]
  * [PHS - Imms - Investigation].[Revised Dose Reason].[Revised Dose Reason]"
  }

  if(disease_check_list$ipd_check){
    mdx_query <- paste(mdx_query, "* [PHS - Imms - Investigation].[Trade Name].[Trade Name]")
  }

  if(disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){

    mdx_query <- paste(mdx_query, ") ON ROWS")


    #
    # Get the filter clause for this MDX
    #

    mdx_filter_clause = vpd_filter_clause(query_start_date, query_end_date, parameter_list)

    mdx_query = paste(mdx_query,mdx_filter_clause)

  }

  return (mdx_query)
}



#
# Function returns an mdx query for pulling Immunization Special Considerations data from the VPD cube
#

vpd_special_considerations_query <- function(query_start_date, query_end_date, parameter_list, disease_check_list)
{
  #
  # Build the mdx query for the Special Considerations Immunizations dataset
  #

  mdx_query <- ""


  if(disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){

  mdx_query = "SELECT
	[Measures].[Case Count]
ON COLUMNS,
NON EMPTY (
[Case - IDs].[Case ID].[Case ID]
* [Case - IDs].[Investigation ID].[Investigation ID]
* [Case - Disease].[Case Disease].[Case Disease]
* [PHS - Imms - Special Consideration].[Antigen].[Antigen]
* [PHS - Imms - Special Consideration].[Created Date].[Created Date]
* [PHS - Imms - Special Consideration].[Special Consideration Type].[Special Consideration Type]
* [PHS - Imms - Special Consideration].[Special Consideration Reason].[Special Consideration Reason]
* [PHS - Imms - Special Consideration].[Special Consideration Reason Comment].[Special Consideration Reason Comment]
* [PHS - Imms - Special Consideration].[Effective From Date].[Effective From Date]
* [PHS - Imms - Special Consideration].[Effective To Date].[Effective To Date]
* [PHS - Imms - Special Consideration].[Source Evidence].[Source Evidence]
* [PHS - Imms - Special Consideration].[Disease Date].[Disease Date]
* [PHS - Imms - Special Consideration].[Accurate Disease Date Indicator].[Accurate Disease Date Indicator]
)
ON ROWS"

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = vpd_filter_clause(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)
  }


  return (mdx_query)
}



#
# Function returns an mdx query for pulling LIS Testing data from the VPD cube
#

vpd_lis_tests_query <- function(query_start_date, query_end_date, parameter_list, disease_check_list)
{

  #
  # Build the mdx query for the LIS Testing dataset
  #

  mdx_query <- ""


  if(disease_check_list$group_b_strep_check |
     disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){

  mdx_query = "SELECT
	[Measures].[Case Count]
  ON COLUMNS,
  NON EMPTY (
  [Case - IDs].[Case ID].[Case ID]
  * [Case - IDs].[Container ID].[Container ID]
  * [Case - IDs].[Investigation ID].[Investigation ID]
  * [LIS - IDs].[Episode ID].[Episode ID]
  * [LIS - IDs].[Accession Number].[Accession Number]
  * [LIS - Date - Collection].[Date].[Date]
  * [LIS - Date - Result].[Date].[Date]
  * [LIS - Rule Engine - Episode Status].[Episode Status].[Episode Status]
  * [LIS - Infection Group].[Infection Group].[Infection Group]
  * [LIS - Specimen].[Specimen Description].[Specimen Description]"
  }

  if(disease_check_list$group_b_strep_check |
     disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){

    mdx_query <- paste(mdx_query, "* [LIS - Specimen].[Sterile Status].[Sterile Status]")
  }

  if(disease_check_list$group_b_strep_check |
     disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){

    mdx_query <- paste(mdx_query, "* [LIS - Result - Organism].[Organisms].[Level 3].ALLMEMBERS
* [LIS - Test].[Order Code].[Order Code]
* [LIS - Test].[Test Code].[Test Code]
* [LIS - Special Request].[SREQ Description].[SREQ Description]")
  }

  if(disease_check_list$igas_check){

    mdx_query <- paste(mdx_query, "* [LIS - Bacterial Typing].[AOF].[AOF]
* [LIS - Bacterial Typing].[SOF].[SOF]
* [LIS - Bacterial Typing].[T Type].[T Type]")
  }

  if(disease_check_list$reportable_vpd_check){

    mdx_query <- paste(mdx_query, "* [LIS - Bacterial Typing].[Biotype].[Biotype]")
  }

  if(disease_check_list$reportable_vpd_check |
     disease_check_list$meningo_check){

    mdx_query <- paste(mdx_query, "* [LIS - Bacterial Typing].[ET Type].[ET Type]
  * [LIS - Bacterial Typing].[Sero-subtype].[Sero-subtype]")
  }

  if(disease_check_list$group_b_strep_check |
     disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){

    mdx_query <- paste(mdx_query, "* [LIS - Result Attributes - Test Outcome].[Test Outcome].[Test Outcome]
  * [LIS - Flag - Test Valid Status].[Test Valid Status].[Test Valid Status]
* [LIS - Rule Engine - Episode Status].[Rule Description].[Rule Description]
* [LIS - Flag - Proficiency Test].[Proficiency Test].[Proficiency Test]
* [LIS - Flag - Test Performed].[Test Performed].[Test Performed]
* [LIS - Result Attributes].[Organism Identified].[Organism Identified]
* [LIS - Result Attributes].[Result Full Description].[Result Full Description]")
  }

  if(disease_check_list$rubella_check){

    mdx_query <- paste(mdx_query, "* [LIS - Result Attributes].[Diphtheria Toxin Result].[Diphtheria Toxin Result]")
  }


  if(disease_check_list$group_b_strep_check |
     disease_check_list$igas_check |
     disease_check_list$ipd_check |
     disease_check_list$measles_check |
     disease_check_list$mumps_check |
     disease_check_list$bordetella_check |
     disease_check_list$meningo_check |
     disease_check_list$rubella_check |
     disease_check_list$reportable_vpd_check){

    mdx_query <- paste(mdx_query, ") ON ROWS")

  #
  # Get the filter clause for this MDX
  #
  mdx_filter_clause = vpd_filter_clause(query_start_date, query_end_date, parameter_list)

  mdx_query = paste(mdx_query,mdx_filter_clause)

  }

  return (mdx_query)
}


