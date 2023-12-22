####################################################################
# fn_cd_mart_tb_sql_queries - SQL queries for use against the CD Mart
# with specialized TB requirements
#
# Each function contains the "Select" part of the query. The filter
# parameters are built separately
#
#
# Author: Darren Frizzell
# Created: 2018-05-09
#
# Modification: See Git Comments
#
####################################################################



#
# Function returns the filter/where condition for a sql query
#
cd_mart_tb_filter_clause <- function(parameter_list)
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


  filter_clause = paste0("WHERE inv.surveillance_date BETWEEN ? AND ? ", disease_sql,
                         surveillance_condition_sql,
                         classification_sql,
                         surveillance_region_ha_sql)

  return(filter_clause)

}   


#
# Function returns a sql query for pulling TB transmission events from CD Mart
#

cd_mart_tb_transmission_event_query <- function(parameter_list)
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
te.anonymous_contact_count
FROM phs_cd.vw_pan_transmission_event te
INNER JOIN phs_cd.vw_pan_investigation inv ON te.disease_event_id = inv.disease_event_id
INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON sc.surveillance_condition_key = inv.surveillance_condition_key
INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key"


#
# Get the filter clause for this MDX
#
sql_filter_clause = cd_mart_tb_filter_clause(parameter_list)

sql_query = paste(sql_query,sql_filter_clause)

return (sql_query)
}




#
# Function returns a sql query for pulling active and latent TB investigations from CD Mart
#

cd_mart_tb_investigation_query <- function(parameter_list)
{

  #
  # Get the include indigenous identifiers variable
  #
  include_indigenous_identifiers = parameter_list$include_indigenous_identifiers

  # Variable for holding identifialbe sql
  indigenous_ident_sql <- ""
  indigenous_ident_join_sql <- ""

  select_sql <- "SELECT
	inv.investigation_id,
  inv.disease_event_id,
  inv.client_id,
  inv.surveillance_date,
  inv.surveillance_reported_date,
  gen.gender,
  geni.gender_identity,"


  if(include_indigenous_identifiers){
    indigenous_ident_sql <- "
   abo.indigenous_self_identify,
  abo.indigenous_identity,
  abo.first_nation_status,
  abo.on_reserve_administered_by AS 'address_located_on_reserve_administered_by',
  abo.indigenous_organizational_unit_name AS 'indigenous_organization',"
  }

  from_sql <- "inv.classification_date,
  CONVERT(date, inv.investigation_record_created_dt_tm) AS 'investigation_created_date',
  inv.age_at_time_of_case_years AS 'age_at_surveillance_date_years',
  inv.disease,
  sc.surveillance_condition,
  cla.classification_group,
  cla.classification,
  inv.is_a_contact_investigation_flag,
  inv.is_a_contact_investigation_date,
  inv.ever_a_contact_client_flag,
  inv.ever_a_contact_client_date,
  sr.health_authority AS surveillance_region_ha,
  sr.health_services_delivery_area AS surveillance_region_hsda,
  sr.local_health_area AS surveillance_region_lha,
  inv.surveillance_region_based_on,
  inv.address_at_time_of_case_city,
  inv.address_at_time_of_case_postal_code,
  sta.investigation_status,
  ea.etiologic_agent_level_1,
  ea.etiologic_agent_level_2,
  ea.etiologic_agent_level_3,
  stg.stage_of_infection,
  inv.body_site_all,
  inv.tb_body_site_category_2,
  tb_body_site_category_3,
  tb_body_site_category_6,
  tb_body_site_category_phac,
  inv.treatment_start_date,
  inv.treatment_end_date,
  inv.treatment_status,
  inv.reason_for_treatment,
  inv.reason_treatment_ended,
  inv.major_mode_of_treatment,
  inv.treatment_outcome,
  inv.cause_of_death,
  inv.cause_of_death_other,
  inv.method_of_detection,
  inv.source_system
  FROM phs_cd.vw_pan_investigation inv
  INNER JOIN phs_cd.vw_pan_client cli ON cli.client_id = inv.client_id
  INNER JOIN phs_cd.vw_pan_gender gen ON gen.gender_key = cli.gender_key
  INNER JOIN phs_cd.vw_pan_gender_identity geni ON geni.gender_identity_key = cli.gender_identity_key
  INNER JOIN phs_cd.vw_pan_etiologic_agent ea ON inv.etiologic_agent_key = ea.etiologic_agent_key
  INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
  INNER JOIN phs_cd.vw_pan_stage_of_infection stg ON stg.stage_of_infection_key = inv.stage_of_infection_key
  INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
  INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
  INNER JOIN phs_cd.vw_pan_investigation_status sta ON inv.investigation_status_key = sta.investigation_status_key"

  if(include_indigenous_identifiers){
    indigenous_ident_join_sql <- "LEFT OUTER JOIN phs_cd.vw_pan_investigation_indigenous abo ON inv.disease_event_id = abo.disease_event_id"
  }

  # Paste together each peice of the SQL query
  sql_query <- paste(select_sql, indigenous_ident_sql, from_sql, indigenous_ident_join_sql)


  #
  # Get the filter clause for this SQL
  #
  sql_filter_clause = cd_mart_tb_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}




#
# Function returns a sql query for pulling TB Clients from CD Mart
#

cd_mart_tb_client_query <- function(parameter_list)
{


  #
  # Get the include patient identifiers variable
  #
  include_patient_identifiers = parameter_list$include_patient_identifiers

  # Variable for holding identifiable sql
  pt_ident_sql <- ""


  select_sql  <- "SELECT DISTINCT
	cli.client_id,"

  if(include_patient_identifiers){
    pt_ident_sql <- "cli.first_name,
    cli.middle_name,
    cli.last_name,
    cli.phn,
    cli.birth_date,
    cli.iphis_tb_number  AS 'bccdc_services_id',"
  }

  from_sql <- "gen.gender,
  eth.ethnicity,
  cli.vch_paris_client_id,
	cli.fha_paris_client_id,
  cli.country_of_birth,
  cli.immigration_arrival_date,
  cli.death_date,
  cli.origin,
  cli.origin_discordance_flag
  FROM phs_cd.vw_pan_investigation inv
  INNER JOIN phs_cd.vw_pan_client cli ON cli.client_id = inv.client_id
  INNER JOIN phs_cd.vw_pan_gender gen ON gen.gender_key = cli.gender_key
  INNER JOIN phs_cd.vw_pan_etiologic_agent ea ON inv.etiologic_agent_key = ea.etiologic_agent_key
  INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
  INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
  INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
  INNER JOIN phs_cd.vw_pan_ethnicity eth ON cli.ethnicity_key = eth.ethnicity_key"

  sql_query <- paste(select_sql, pt_ident_sql, from_sql)

  #
  # Get the filter clause for this SQL
  #
  sql_filter_clause = cd_mart_tb_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}







#
# Function returns a sql query for pulling TB Source Contact investigations from CD Mart
#

cd_mart_tb_contact_query <- function(parameter_list)
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
    cli.birth_date,
    cli.iphis_tb_number  AS 'bccdc_services_id',"
  }


  if(include_indigenous_identifiers){
    indigenous_ident_sql <- "abo.indigenous_self_identify,
	abo.indigenous_identity,
	abo.first_nation_status,
	abo.on_reserve_administered_by AS 'address_located_on_reserve_administered_by',
	abo.indigenous_organizational_unit_name AS 'indigenous_organization',"

  }

  from_sql <- "gen.gender,
  cli.origin,
  cli.origin_discordance_flag,
  cli.country_of_birth,
  cli.immigration_arrival_date,
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
  inv.body_site_all,
  inv.tb_body_site_category_2,
  inv.tb_body_site_category_3,
  inv.tb_body_site_category_6,
  inv.tb_body_site_category_phac,
  inv.treatment_start_date,
  inv.treatment_end_date,
  inv.treatment_status,
  inv.reason_for_treatment,
  inv.reason_treatment_ended,
  inv.major_mode_of_treatment,
  inv.treatment_outcome,
  inv.contact_priority AS 'tb_contact_priority',
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
# Function returns a sql query for pulling TB TSTs data for investigations from CD Mart
#

cd_mart_tb_tst_investigation_query <- function(parameter_list)
{

  sql_query <- "SELECT
	inv.client_id,
	inv.investigation_id,
	inv.disease_event_id,
	enc.encounter_id,
	tst.tb_skin_test_id,
	inv.surveillance_date,
	cla.classification,
  tst.tst_given_date,
  tst.tst_given_time,
  tst.tst_read_date,
  tst.tst_read_time,
	enc.encounter_type,
	enc.encounter_reason,
	tst.reason_for_tst,
	tst.historical_flag,
  tst.tst_given_organizational_unit_name AS 'tst_given_responsible_organization',
  tst.tst_given_sdl AS 'tst_given_service_delivery_location',
  tst.tst_read_organizational_unit_name  AS 'tst_read_responsible_organization',
  tst.tst_read_sdl AS 'tst_read_service_delivery_location',
  tst.tst_read_interpreted_result,
  tst.tst_reaction_size,
  tst.is_followup_indicator AS 'tb_tst_follow_up_indicator',
  tst.tst_followup,
  tst.tst_followup_detail,
  tst.tst_reason_no_xray,
  tst.other_tb_case_contact,
  tst.other_tb_exposure_date,
  tst.other_tb_exposure_date_partial_flag,
  tst.recent_illness,
  tst.recent_illness_date,
  tst.recent_illness_date_partial_flag
  FROM phs_cd.vw_pan_investigation inv
  INNER JOIN phs_cd.vw_pan_client cli ON cli.client_id = inv.client_id
  INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
  INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
  INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
  INNER JOIN phs_cd.vw_pan_investigation_encounter enc ON inv.investigation_id = enc.investigation_id
  INNER JOIN phs_cd.vw_pan_tb_tst tst ON enc.encounter_id = tst.encounter_id"


  #
  # Get the filter clause for this SQL
  #
  sql_filter_clause = cd_mart_tb_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  return (sql_query)
}



#
# Function returns a sql query for pulling all TB TST data for clients in CD Mart
#

cd_mart_tb_tst_client_query <- function(parameter_list)
{


  sql_query = "SELECT
	cli.client_id,
	enc.investigation_id,
  enc.encounter_id,
  tst.tb_skin_test_id,
  tst.tst_given_date,
  tst.tst_given_time,
  tst.tst_read_date,
  tst.tst_read_time,
  enc.encounter_type,
  enc.encounter_reason,
  tst.reason_for_tst,
  tst.historical_flag,
  tst.tst_given_organizational_unit_name AS 'tst_given_responsible_organization',
  tst.tst_given_sdl AS 'tst_given_service_delivery_location',
  tst.tst_read_organizational_unit_name  AS 'tst_read_responsible_organization',
  tst.tst_read_sdl AS 'tst_read_service_delivery_location',
  tst.tst_read_interpreted_result,
  tst.tst_reaction_size,
  tst.is_followup_indicator AS 'tb_tst_follow_up_indicator',
  tst.tst_followup,
  tst.tst_followup_detail,
  tst.tst_reason_no_xray,
  tst.other_tb_case_contact,
  tst.other_tb_exposure_date,
  tst.other_tb_exposure_date_partial_flag,
  tst.recent_illness,
  tst.recent_illness_date,
  tst.recent_illness_date_partial_flag
  FROM phs_cd.vw_pan_tb_tst tst
  INNER JOIN phs_cd.vw_pan_client cli ON cli.client_id = tst.client_id
  LEFT OUTER JOIN phs_cd.vw_pan_investigation_encounter enc ON tst.encounter_id = enc.encounter_id

  WHERE
  (
  tst.tst_given_date BETWEEN ? AND ?
  OR
  tst.tst_read_date BETWEEN ? AND ?
  )"


  #
  # Add an order by clause
  #
  sql_query = paste0(sql_query,"ORDER BY tst.tst_given_date")

  return (sql_query)
}



#
# Function returns a sql query for pulling all TB Lab data for clients in CD Mart
#

cd_mart_tb_lab_query <- function(parameter_list)
{


  sql_query = "SELECT
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
  lab_result.result_description,
  lab_result.etiologic_agent_level_1 AS 'lab_etiologic_agent_level_1',
	lab_result.etiologic_agent_level_2 AS 'lab_etiologic_agent_level_2',
	lab_result.etiologic_agent_level_3 AS 'lab_etiologic_agent_level_3',
  lab_sens.antimicrobial_drug_name,
  lab_sens.sensitivity_interpretation,
  lab_sens.sensitivity_value,
  lab_result.report_id AS 'lab_report_id',
  lab_result.report_date AS 'lab_report_date',
  lab_result.accession_number,
  lab_result.report_type AS 'lab_report_type',
  lab_spec.requisition_specimen_id AS 'specimen_id',
  CONVERT(date, lab_spec.specimen_collect_dt_tm) AS 'specimen_collected_date',
  lab_spec.specimen_type,
  lab_spec.specimen_site,
  lab_spec.specimen_description

  FROM phs_cd.vw_pan_investigation inv
  INNER JOIN phs_cd.vw_pan_client cli ON cli.client_id = inv.client_id
  INNER JOIN phs_cd.vw_pan_lha sr ON inv.surveillance_region_lha_key = sr.lha_key
  INNER JOIN phs_cd.vw_pan_classification cla ON inv.classification_key = cla.classification_key
  INNER JOIN phs_cd.vw_pan_surveillance_condition sc ON inv.surveillance_condition_key = sc.surveillance_condition_key
  INNER JOIN phs_cd.vw_pan_investigation_encounter enc ON inv.investigation_id = enc.investigation_id

  INNER JOIN phs_cd.vw_pan_investigation_lab_requisition req ON enc.encounter_id = req.encounter_id
  LEFT OUTER JOIN phs_cd.vw_pan_investigation_lab_test lab_test ON req.requisition_id = lab_test.requisition_id
  LEFT OUTER JOIN phs_cd.vw_pan_investigation_lab_result lab_result ON lab_test.test_id = lab_result.test_id
  LEFT OUTER JOIN phs_cd.vw_pan_investigation_lab_sensitivity lab_sens ON lab_result.result_id = lab_sens.result_id
  LEFT OUTER JOIN phs_cd.vw_pan_investigation_lab_requisition_specimen lab_spec
  ON req.requisition_id = lab_spec.requisition_id
  AND lab_spec.requisition_specimen_id = lab_test.requisition_specimen_id"


  #
  # Get the filter clause for this SQL
  #
  sql_filter_clause = cd_mart_tb_filter_clause(parameter_list)

  sql_query = paste(sql_query,sql_filter_clause)

  #
  # Add an order by clause
  #
  sql_query = paste0(sql_query,"ORDER BY inv.surveillance_date,	inv.investigation_id,	enc.encounter_date")


  return (sql_query)
}
