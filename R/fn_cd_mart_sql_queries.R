####################################################################
# fn_cd_mart_sql_queries - SQL queries for use agains the CD Mart
#
# Each function contains the "Select" part of the query. The filter
# parameters are built separately
#
#
# Author: Darren Frizzell
# Created: 2018-04-17
#
# Modification: See Git Comments
#
####################################################################



#
# Function returns the filter/where condition for a sql query
#
cd_mart_filter_clause <- function(parameter_list)
{

  #
  # Add the optional parameters to the WHERE clause
  #
  disease = parameter_list$disease
  disease_sql <- sql_where_clause_filter(field_name = "inv.disease",
                                         filter_values = disease)

  surveillance_condition = parameter_list$surveillance_condition
  surveillance_condition_sql <- sql_where_clause_filter(field_name = "sc.surveillance_condition",
                                                        filter_values = surveillance_condition)

  classification = parameter_list$classification
  classification_sql <- sql_where_clause_filter(field_name = "cla.classification",
                                                filter_values = classification)

  surveillance_region_ha = parameter_list$surveillance_region_ha
  surveillance_region_ha_sql <- sql_where_clause_filter(field_name = "sr.health_authority",
                                                filter_values = surveillance_region_ha)


  filter_clause =paste0("WHERE
cla.classification_authority = 'Provincial'
AND inv.surveillance_date BETWEEN ? AND ? ", disease_sql, surveillance_condition_sql, classification_sql, surveillance_region_ha_sql,
"ORDER BY
	inv.surveillance_date")

  return(filter_clause)

}


#
# Function returns a sql query for pulling core panorama data from the CD Mart
#
cd_mart_investigation_query <- function(parameter_list)
{
  #
  # Get the include patient identifiers variable
  #
  include_patient_identifiers = parameter_list$include_patient_identifiers

  # Variable for holding identifiable sql
  pt_ident_sql <- ""


select_sql = "SELECT DISTINCT
	inv.investigation_id,
	inv.disease_event_id,
	inv.earliest_stage_of_infection,
inv.client_id,
cli.vch_paris_client_id,
cli.fha_paris_client_id,
inv.paris_assessment_number,"

if(include_patient_identifiers){
  pt_ident_sql <- "cli.first_name,
  cli.middle_name,
  cli.last_name,
  cli.phn,
  cli.birth_date,"
}

from_sql <- "gen.gender,
ob.outbreak_id,
inv.surveillance_date,
dd.cdc_epidemiology_year AS surveillance_date_epi_year,
dd.cdc_epidemiology_week AS surveillance_date_epi_week,
inv.surveillance_reported_date,
CONVERT(date,inv.investigation_record_created_dt_tm) AS date_investigation_created,
CONVERT(date,investigation_record_modified_dt_tm) AS date_investigation_updated,
inv.disease,
sc.surveillance_condition,
cla.classification_group,
cla.classification,
inv.classification_date,
sr.health_authority AS surveillance_region_ha,
sr.health_services_delivery_area AS surveillance_region_hsda,
inv.surveillance_region_based_on,
CONVERT(date,inv.investigation_symptom_onset_dt_tm) AS symptom_onset_date,
inv.symptom_set_as_onset,
inv.age_at_time_of_case_years AS age_at_surveillance_date_years,
age.age_group_10 AS age_at_surveillance_date_age_group_10,
ea.etiologic_agent_level_1,
ea.etiologic_agent_level_2,
ea.etiologic_agent_level_3,
inv.etiologic_agent_further_differentiation,
inv.stage_of_infection,
inv.address_at_time_of_case_country,
inv.address_at_time_of_case_province,
inv.address_at_time_of_case_city,
inv.address_at_time_of_case_postal_code,
inv.address_at_time_of_case_street_number,
inv.address_at_time_of_case_street_name,
inv.address_at_time_of_case_street_direction,
lha.health_authority AS address_at_time_of_case_ha,
lha.health_services_delivery_area AS address_at_time_of_case_hsda,
lha.local_health_area AS address_at_time_of_case_lha,
inv.outcome,
inv.outcome_date AS last_outcome_date,
inv.cause_of_death,
sta.investigation_status,
inv.status_date AS investigation_status_date,
inv.disposition,
inv.source_system
FROM phs_cd.vw_pan_investigation inv
INNER JOIN phs_cd.vw_pan_client cli ON cli.client_id = inv.client_id
INNER JOIN phs_cd.vw_pan_gender gen ON cli.gender_key = gen.gender_key
INNER JOIN phs_cd.vw_pan_age age ON inv.age_at_surveillance_reported_date_key = age.age_key
INNER JOIN phs_cd.vw_pan_investigation_status sta ON inv.investigation_status_key = sta.investigation_status_key
INNER JOIN phs_cd.vw_pan_etiologic_agent ea ON inv.etiologic_agent_key = ea.etiologic_agent_key
INNER JOIN phs_cd.vw_pan_lha lha ON inv.address_at_time_of_case_lha_key = lha.lha_key
INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
INNER JOIN phs_cd.vw_pan_date dd ON inv.surveillance_date_key = dd.date_key
INNER JOIN phs_cd.vw_pan_encounter_group eg ON inv.encounter_group_key = eg.encounter_group_key
LEFT OUTER JOIN phs_cd.vw_pan_investigation_outbreak ob ON inv.investigation_id = ob.outbreak_id"


sql_query <- paste(select_sql, pt_ident_sql, from_sql)

#
# Get the filter clause for this SQL
#
sql_filter_clause = cd_mart_filter_clause(parameter_list)

sql_query = paste(sql_query,sql_filter_clause)

return (sql_query)
}



#
# Function returns a sql query for pulling Clients from CD Mart
#
cd_mart_client_query <- function(parameter_list)
{

  #
  # Get the include patient identifiers variable
  #
  include_patient_identifiers = parameter_list$include_patient_identifiers

  # Variable for holding identifialbe sql
  pt_ident_sql <- ""

  select_sql  <- "SELECT DISTINCT
	cli.client_id,
  cli.vch_paris_client_id,
	cli.fha_paris_client_id,"

  if(include_patient_identifiers){
    pt_ident_sql <- "cli.first_name,
    cli.middle_name,
    cli.last_name,
    cli.phn,
    cli.birth_date,"
  }

  from_sql <- "gen.gender,
  gi.gender_identity,
  eth.ethnicity,
  cli.other_ethnicity,
  cli.death_date,
  inv.source_system
FROM phs_cd.vw_pan_investigation inv
  INNER JOIN phs_cd.vw_pan_client cli ON cli.client_id = inv.client_id
  INNER JOIN phs_cd.vw_pan_gender_identity gi ON cli.gender_identity_key = gi.gender_identity_key
  INNER JOIN phs_cd.vw_pan_ethnicity eth ON cli.ethnicity_key =eth.ethnicity_key
  INNER JOIN phs_cd.vw_pan_gender gen ON gen.gender_key = cli.gender_key
  INNER JOIN phs_cd.vw_pan_etiologic_agent ea ON inv.etiologic_agent_key = ea.etiologic_agent_key
  INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
  INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
  INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key"

  sql_query <- paste(select_sql, pt_ident_sql, from_sql)

  #
  # Get the filter clause for this SQL
  #
  sql_filter_clause = cd_mart_tb_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}




#
# Function returns a sql query for pulling investigation level risk factors from the CD Mart
#
cd_mart_risk_factor_query <- function(parameter_list)
{
  sql_query = "SELECT
	inv.investigation_id,
	inv.disease_event_id,
	rf.risk_factor,
	rfc.risk_factor_response,
	rfc.risk_factor_other,
	rfc.risk_factor_start_date,
	rfc.risk_factor_end_date,
	rfc.risk_factor_reported_date,
	rfc.risk_factor_end_reason,
  inv.source_system
FROM phs_cd.vw_pan_investigation inv
	INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
	INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
	INNER JOIN phs_cd.vw_pan_date dd ON inv.surveillance_date_key = dd.date_key
	INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
	INNER JOIN phs_cd.vw_pan_encounter_group eg ON inv.encounter_group_key = eg.encounter_group_key
	INNER JOIN phs_cd.vw_pan_risk_factor_investigation rfi ON rfi.disease_event_id = inv.disease_event_id
	LEFT OUTER JOIN phs_cd.vw_pan_risk_factor_client rfc ON rfc.risk_factor_id = rfi.risk_factor_id
	LEFT OUTER JOIN phs_cd.vw_pan_risk_factor rf ON rf.risk_factor_key = rfc.risk_factor_key"

#
# Get the filter clause for this MDX
#
sql_filter_clause = cd_mart_filter_clause(parameter_list)

sql_query = paste(sql_query,sql_filter_clause)

return (sql_query)
}




#
# Function returns a sql query for pulling Signs & Symptoms data from the CD Mart
#
cd_mart_symptoms_query <- function(parameter_list)
{
  sql_query = "SELECT
	inv.investigation_id,
	inv.disease_event_id,
	sym.symptom_id,
	sym.symptom,
	sym.symptom_response,
  CONVERT(date, sym.symptom_recovery_dt_tm)  AS symptom_recovery_date,
	CONVERT(date,sym.symptom_onset_dt_tm) AS symptom_date,
  inv.source_system
FROM phs_cd.vw_pan_investigation inv
	INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
	INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
	INNER JOIN phs_cd.vw_pan_date dd ON inv.surveillance_date_key = dd.date_key
	INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
	INNER JOIN phs_cd.vw_pan_encounter_group eg ON inv.encounter_group_key = eg.encounter_group_key
	INNER JOIN phs_cd.vw_pan_symptom sym ON inv.disease_event_id = sym.disease_event_id"

  #
  # Get the filter clause for this MDX
  #
  sql_filter_clause = cd_mart_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}



#
# Function returns a sql query for pulling the Observations from Signs & Symptoms data from the CD Mart
#
cd_mart_observations_query <- function(parameter_list)
{
  sql_query = "SELECT
	inv.investigation_id,
	inv.disease_event_id,
	ob.symptom_id,
	ob.observation_id,
  ob.observation_description AS 'observation',
	ob.observation_value,
  ob.observation_unit,
  CONVERT(date,ob.observation_date) AS 'observation_date',
  ob.observed_by,
	ob.observed_by_other,
  inv.source_system
FROM phs_cd.vw_pan_investigation inv
	INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
	INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
	INNER JOIN phs_cd.vw_pan_date dd ON inv.surveillance_date_key = dd.date_key
	INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
	INNER JOIN phs_cd.vw_pan_encounter_group eg ON inv.encounter_group_key = eg.encounter_group_key
	INNER JOIN phs_cd.vw_pan_observation ob ON inv.disease_event_id = ob.disease_event_id"

  #
  # Get the filter clause for this MDX
  #
  sql_filter_clause = cd_mart_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}


#
# Function returns a sql query for pulling UDF data from the CD Mart
#
cd_mart_udf_query <- function(parameter_list)
{
  sql_query = "SELECT
	udf.form_instance_id AS udf_instance_id,
	inv.investigation_id,
  inv.disease_event_id,
  udf.form_name AS udf_name,
  udf.form_template_version AS udf_template_version,
  udf.question_type,
  udf.question_keyword_common,
  udf.answer_row_id,
  udf.answer_value,
  udf.question_sort_id,
  inv.source_system
FROM phs_cd.vw_pan_investigation inv
	INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
	INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
	INNER JOIN phs_cd.vw_pan_date dd ON inv.surveillance_date_key = dd.date_key
	INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
	INNER JOIN phs_cd.vw_pan_encounter_group eg ON inv.encounter_group_key = eg.encounter_group_key
	INNER JOIN phs_cd.vw_pan_udf_all udf ON inv.disease_event_id = udf.form_instance_disease_event_id"

  #
  # Get the filter clause for this MDX
  #
  sql_filter_clause = cd_mart_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}


#
# Function returns a sql query for pulling lab panorama data from the CD Mart
#
cd_mart_lab_query <- function(parameter_list)
{

  sql_query <- "SELECT
	inv.client_id,
	inv.investigation_id,
	inv.disease_event_id,
	inv.surveillance_date,
	enc.encounter_id,
	CONVERT(date, enc.encounter_date) AS 'encounter_date',
	req.requisition_id AS 'lab_requisition_id',
	req.requisition_date,
	req.ordering_provider_use_other,
	lab_test.test_id AS 'lab_test_id',
	lab_test.test_name,
	lab_test.test_status,
	lab_test.test_category,
	lab_result.result_id AS 'lab_result_id',
	lab_result.result_name,
	CONVERT(date, lab_result.result_dt_tm) AS 'result_date',
	lab_result.container_id,
	lab_result.result_status,
	lab_result.interpreted_result,
	lab_result.result_value,
	lab_result.result_unit,
	lab_result.result_flag,
	lab_result.resulting_lab_sdl,
	lab_result.result_description,
	lab_result.etiologic_agent_level_1 AS 'lab_etiologic_agent_level_1',
	lab_result.etiologic_agent_level_2 AS 'lab_etiologic_agent_level_2',
	lab_result.etiologic_agent_level_3 AS 'lab_etiologic_agent_level_3',
	lab_result.report_id AS 'lab_report_id',
	lab_result.report_date AS 'lab_report_date',
	lab_result.accession_number,
	lab_result.report_type AS 'lab_report_type',
	lab_spec.requisition_specimen_id AS 'specimen_id',
	CONVERT(date, lab_spec.specimen_collect_dt_tm) AS 'specimen_collected_date',
	lab_spec.specimen_type,
	lab_spec.specimen_site,
	lab_spec.specimen_description,
  inv.source_system
FROM phs_cd.vw_pan_investigation inv
	INNER JOIN phs_cd.vw_pan_client cli ON cli.client_id = inv.client_id
	INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
	INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
	INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
	INNER JOIN phs_cd.vw_pan_investigation_encounter enc ON inv.investigation_id = enc.investigation_id

	INNER JOIN phs_cd.vw_pan_investigation_lab_requisition req ON enc.encounter_id = req.encounter_id
	LEFT OUTER JOIN phs_cd.vw_pan_investigation_lab_test lab_test ON req.requisition_id = lab_test.requisition_id
	LEFT OUTER JOIN phs_cd.vw_pan_investigation_lab_result lab_result ON lab_test.test_id = lab_result.test_id
	LEFT OUTER JOIN phs_cd.vw_pan_investigation_lab_requisition_specimen lab_spec
		ON req.requisition_id = lab_spec.requisition_id
		AND lab_spec.requisition_specimen_id = lab_test.requisition_specimen_id"



  #
  # Get the filter clause for this SQL
  #
  sql_filter_clause = cd_mart_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}






######################################################################
#
# CD Mart Contact Queries
#
######################################################################


#
# Function returns a sql query for pulling Transmission events from CD Mart
#

cd_mart_transmission_event_query <- function(parameter_list)
{
  sql_query = "SELECT
	te.transmission_event_id,
te.investigation_id,
te.disease_event_id,
te.transmission_event_exposure_start_date,
te.transmission_event_exposure_end_date,
te.transmission_event_exposure_name,
te.transmission_event_setting_type,
te.transmission_event_setting,
te.transmission_event_exposure_location_name,
te.mode_of_transmission,
te.nature_of_transmission,
te.anonymous_contact_count,
inv.source_system
FROM phs_cd.vw_pan_transmission_event te
INNER JOIN phs_cd.vw_pan_investigation inv ON te.disease_event_id = inv.disease_event_id
INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON sc.surveillance_condition_key = inv.surveillance_condition_key
INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key"


  #
  # Get the filter clause for this SQL
  #
  sql_filter_clause = cd_mart_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}




#
# Function returns a sql query for pulling the Source Contact investigations from CD Mart
#

cd_mart_contact_query <- function(parameter_list)
{

  #
  # Get the include patient identifiers variable
  # Get the include indigenous patient identifiers variable
  #
  include_patient_identifiers = parameter_list$include_patient_identifiers
  include_indigenous_identifiers = parameter_list$include_indigenous_identifiers

  # Variables for holding identifiable sql
  pt_ident_sql <- ""
  indigenous_ident_sql <- ""
  indigenous_ident_join_sql <- ""


  select_sql = "SELECT
	inv.investigation_id,
	inv.disease_event_id,
	cli.client_id,
	tec.transmission_event_id,
	inv.surveillance_date,
	inv.surveillance_reported_date,
	inv.classification_date,
  inv.is_a_contact_investigation_date,
	inv.ever_a_contact_client_date,"

  if(include_patient_identifiers){
    pt_ident_sql <- "cli.first_name,
    cli.middle_name,
    cli.last_name,
    cli.phn,
    cli.birth_date,"
  }


  if(include_indigenous_identifiers){
    indigenous_ident_sql <- "abo.indigenous_self_identify,
	abo.indigenous_identity,
	abo.first_nation_status,
	abo.on_reserve_administered_by AS 'address_located_on_reserve_administered_by',
	abo.indigenous_organizational_unit_name AS 'indigenous_organization',"

  }

  from_sql <- "gen.gender,
  cli.death_date,
  inv.cause_of_death,
  inv.age_at_time_of_case_years AS 'age_at_surveillance_date_years',
  inv.disease,
  sc.surveillance_condition,
  cla.classification_group,
  cla.classification,
  sr.health_authority AS surveillance_region_ha,
  sr.health_services_delivery_area AS surveillance_region_hsda,
  sr.local_health_area AS surveillance_region_lha,
  inv.surveillance_region_based_on,
  inv.address_at_time_of_case_city,
  ea.etiologic_agent,
  stg.stage_of_infection,
  inv.method_of_detection,
  inv.source_system
  FROM phs_cd.vw_pan_investigation inv
  INNER JOIN phs_cd.vw_pan_client cli ON cli.client_id = inv.client_id
  INNER JOIN phs_cd.vw_pan_gender gen ON gen.gender_key = cli.gender_key
  INNER JOIN phs_cd.vw_pan_etiologic_agent ea ON inv.etiologic_agent_key = ea.etiologic_agent_key
  INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
  INNER JOIN phs_cd.vw_pan_stage_of_infection stg ON stg.stage_of_infection_key = inv.stage_of_infection_key
  INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
  INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
  LEFT JOIN phs_cd.vw_pan_te_contact_investigation tec ON inv.disease_event_id = tec.contact_disease_event_id"

  if(include_indigenous_identifiers){
    indigenous_ident_join_sql <- "LEFT OUTER JOIN phs_cd.vw_pan_investigation_indigenous abo
    ON inv.disease_event_id = abo.disease_event_id"
  }


  where_sql <- "WHERE
  (
  (is_a_contact_investigation_flag = 'Yes' AND inv.surveillance_date BETWEEN ? AND ?)
  OR
  (ever_a_contact_client_flag = 'Yes' AND inv.surveillance_date BETWEEN ? AND ?)
  )"



  # Paste together each peice of the SQL query
  sql_query <- paste(select_sql, pt_ident_sql, indigenous_ident_sql, from_sql, indigenous_ident_join_sql, where_sql)


  #
  # Add the optional parameters to the WHERE clause
  #
  disease = parameter_list$disease
  disease_sql <- sql_where_clause_filter(field_name = "inv.disease",
                                         filter_values = disease)

  surveillance_condition = parameter_list$surveillance_condition
  surveillance_condition_sql <- sql_where_clause_filter(field_name = "sc.surveillance_condition",
                                                        filter_values = surveillance_condition)

  classification = parameter_list$classification
  classification_sql <- sql_where_clause_filter(field_name = "cla.classification",
                                                filter_values = classification)

  surveillance_region_ha = parameter_list$surveillance_region_ha
  surveillance_region_ha_sql <- sql_where_clause_filter(field_name = "sr.health_authority",
                                                        filter_values = surveillance_region_ha)


  #
  # Add the optional parameters to the main SQL
  #
  sql_query = paste0(sql_query, disease_sql, surveillance_condition_sql, classification_sql, surveillance_region_ha_sql,"ORDER BY inv.surveillance_date")

  return (sql_query)
}


#
# Function returns a sql query for pulling Outbreak data from the CD Mart
#
cd_mart_outbreak_query <- function(parameter_list)
{
  sql_query = "SELECT
	     inv.investigation_id,
       inv.client_id,
	     ob.outbreak_id,
       ob.outbreak_name,
       ob.outbreak_classification,
       ob.outbreak_classification_date,
       ob.outbreak_authority,
       ob.outbreak_status,
       CONVERT(date, ob.outbreak_status_date) AS outbreak_status_date,
       CONVERT(date, ob.first_case_onset_date) AS first_case_onset_date,
       CONVERT(date, ob.date_outbreak_declared) AS date_outbreak_declared,
       CONVERT(date, ob.date_outbreak_declared_over) AS date_outbreak_declared_over,
       CONVERT(date, ob.outbreak_surveillance_reported_date) AS outbreak_surveillance_reported_date,
       inv.source_system
FROM phs_cd.vw_pan_investigation inv
	INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
	INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
	INNER JOIN phs_cd.vw_pan_date dd ON inv.surveillance_date_key = dd.date_key
	INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
	INNER JOIN phs_cd.vw_pan_encounter_group eg ON inv.encounter_group_key = eg.encounter_group_key
	INNER JOIN phs_cd.vw_pan_investigation_outbreak ob ON inv.investigation_id = ob.investigation_id"

  #
  # Get the filter clause for this MDX
  #
  sql_filter_clause = cd_mart_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}




#
# Function returns a sql query for pulling complication data from the CD Mart
#
cd_mart_complication_query <- function(parameter_list)
{
  sql_query = "SELECT
  inv.investigation_id AS investigation_id,
  inv.disease_event_id AS disease_event_id,
  comp.encounter_id as encounter_id,
  comp.complication_id AS complication_id,
  comp.other_complication AS other_complication,
  comp.complication AS complication,
  comp.response AS response,
  comp.complication_date AS complication_date,
  inv.source_system
  FROM
phs_cd.vw_pan_investigation inv
	INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
	INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
	INNER JOIN phs_cd.vw_pan_date dd ON inv.surveillance_date_key = dd.date_key
	INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
  INNER JOIN phs_cd.vw_pan_investigation_encounter enc ON (inv.investigation_id = enc.investigation_id)
  INNER JOIN phs_cd.vw_pan_complication comp ON (enc.encounter_id = comp.encounter_id)"
  #
  # Get the filter clause for this MDX
  #
  sql_filter_clause = cd_mart_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}









