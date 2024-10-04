# Run this to refresh sysdata.rda

require(tidyverse)
devtools::load_all()

servers <-
  list(
    sql =
      tidyr::crossing(
        mart   = 'CD',
        driver =
          forcats::fct_inorder(
            c(
              "{SQL Server}",
              "{ODBC Driver 18 for SQL Server}",
              "{ODBC Driver 17 for SQL Server}"
            )
          ),
        dplyr::bind_rows(
          dplyr::tibble(
            type     = 'prod',
            server   = "SPRDBSBI003.phsabc.ehcnet.ca\\PRIDBSBIEDW",
            database = "SPEDW"
          ),
          dplyr::tibble(
            type     = 'su',
            server   = "SNPDBSBI001\\NPIDBSBICMB",
            database = "SUEDW"
          ),
          dplyr::tibble(
            type     = 'sa',
            server   = "SNPDBSBI001\\NPIDBSBICMB",
            database = "SAEDW"
          ),
        )
      ) %>%
      dplyr::mutate(
        phrdw_datamart = dplyr::case_when(
          type == 'su' ~ paste(mart, 'Mart SU'),
          TRUE         ~ paste(mart, 'Mart')
        )
      ),
    olap =
      tidyr::crossing(
        dplyr::tribble(
          ~ mart,        ~ phrdw_datamart, ~ initial_catalog,           ~ type,
          'CDI',         'CDI',            "CDI",                       'prod',
          'Enteric',     'Enteric',        "PHRDW_Enteric_Panorama",    'prod',
          'Enteric',     'Enteric SU',     "SU_PHRDW_Enteric_Panorama", 'su',
          'Respiratory', 'Respiratory',    "PHRDW_Respiratory",         'prod',
          'STIBBI',      'STIBBI',         "PHRDW_STIBBI",              'prod',
          'STIBBI',      'STIBBI SU',      "SU_PHRDW_STIBBI",           'su',
          'STIBBI',      'STIBBI SA',      "SA_PHRDW_STIBBI",           'sa',
          'VPD',         'VPD',            "PHRDW_VPD",                 'prod',
          'VPD',         'VPD SU',         "SU_PHRDW_VPD",              'su',
          'TAT',         'TAT',            "PHRDW_TAT",                 'prod',
        ),
        data_source = 'SPRSASBI001.phsabc.ehcnet.ca\\PRISASBIM',
        provider    = 'MSOLAP',
        packet_size = '32767'
      )
  )


filter_rules <-
  readxl::read_excel(
    'data-raw/phrdwRdata_filter_list.xlsx',
    sheet = 'filters',
    col_names = T,
    guess_max = 2000
  )

# available_prebuilt_datasets <-
#   list(
#     'CDI'         = c('Vital Stats',
#                       'Vital Stats CCD',
#                       'Vital Stats CCD Dashboard'
#                       ),
#
#     'Respiratory' = c('LIS Tests',
#                       'LIS Episodes'),
#
#     'STIBBI'      = c('Case',
#                       'Investigation',
#                       'Client',
#                       'PHS Body Site',
#                       'PHS Client Risk Factor',
#                       'PHS Investigation Risk Factor',
#                       'PHS UDF Long',
#                       'LIS Tests',
#                       'LIS Test Providers',
#                       'LIS Episodes',
#                       'LIS POC'),
#
#     'Enteric'     = c('Case Investigation',
#                       'Risk Factor',
#                       'Symptom',
#                       'UDF',
#                       'LIS Data'),
#
#     'VPD'         = c('Case Investigation',
#                       'Symptoms',
#                       'Symptoms Long',
#                       'Risk Factors',
#                       'UDF',
#                       'UDF Long',
#                       'Immunizations',
#                       'Special Considerations',
#                       'LIS Tests'),
#
#     'CD'          = c('Investigation',
#                       'Client',
#                       'Risk Factor',
#                       'Symptom',
#                       'Observation',
#                       'UDF',
#                       'Lab',
#                       'Transmission Events',
#                       'Contacts',
#                       'Outbreaks',
#                       'TB Contacts',
#                       'TB Investigation',
#                       'TB Transmission Events',
#                       'TB Client',
#                       'TB TST Investigation',
#                       'TB TST Client',
#                       'TB Lab',
#                       'Complication')
#   )
  )

sql_query_info <-
  readr::read_tsv(
    'data-raw/sql_queries.csv',
    col_types = readr::cols(.default = 'character')
  )

mdx_query_info <-
  available_prebuilt_datasets %>%
  discard_at('CD') %>%
  map(as_tibble) %>%
  map(rename, dataset_name = value) %>%
  bind_rows(.id = 'mart')
# Building MDX query info -------------------------------------------------

# mdx_query_info <-
#   available_prebuilt_datasets %>%
#   discard_at('CD') %>%
#   map(as_tibble) %>%
#   map(rename, dataset_name = value) %>%
#   bind_rows(.id = 'mart')

new_filter_rules <-
  filter_rules %>%
  select(
    mart         = datamart,
    dataset_name = dataset,
    dim          = dimension_name,
    attr_hier    = dimension_name_level_2,
    lvl_memb     = dimension_name_level_2,
    param_name   = parameter
  ) %>%
    mutate(
      param_name =
        case_when(
          str_detect(param_name, 'query.*date') ~ 'query_date',
          TRUE ~ param_name
        )
    ) %>%
    distinct %>%
    mutate(
      .after = dataset_name,
      field_type =
        case_when(
          str_detect(attr_hier, 'Date') ~ 'filter_r',
          TRUE                          ~ 'filter_d'
        ),
      check = 'none'
    )
    # count(param_name)

clean_mdx_strings <- function(list_of_mdx_strings, mart, cube) {

  map(list_of_mdx_strings, str_split_1, '\\n') %>%
  map(str_trim) %>%
  map(discard, ~ nchar(.x) == 0) %>%
  map(str_remove_all, '\\[|\\]|\\,|\\"|\\*') %>%
  map(str_trim) %>%
  map(map, str_split_1, '\\.') %>%
  imap(
    ~ map2_dfr(
      .x, .y,
      ~ tibble(
        mart         = mart,
        cube         = cube,
        dataset_name = .y,
        dim          = .x[1],
        attr_hier    = .x[2],
        tbd          = .x[3],
        all_memb     = .x[4],
      )
    )
  ) %>%
  map(
    ~ {

      df_temp <- .x

      mutate(
        df_temp,
        lvl_or_prop =
          pmap(
            df_temp,
            \(mart, cube, dataset_name, dim, attr_hier, tbd, all_memb) {

              df_temp <-
                df_olap_map %>%
                select(-mea) %>%
                distinct %>%
                filter(
                  tolower(.data$cube)      == tolower(.env$cube),
                  tolower(.data$dim)       == tolower(.env$dim),
                  tolower(.data$attr_hier) == tolower(.env$attr_hier)
                )

              lvl_prop <-
                map(
                  list(
                    lvl  = tolower(df_temp$lvl),
                    prop = tolower(df_temp$prop)
                  ),
                  ~ if (any(tolower(tbd) %in% .x)) { tbd } else { NA_character_ }
                )

              if (all(is.na(lvl_prop))) lvl_prop$lvl <- tbd

              return(lvl_prop)

            }
          )
      ) %>%
        unnest_wider(lvl_or_prop) %>%
        select(-tbd) %>%
        mutate(
          .after = 2,
          field_type =
            case_when(
              dim == 'Measures' ~ 'columns',
              !is.na(lvl) & is.na(prop) ~ 'rows',
              is.na(lvl) & !is.na(prop) ~ 'dim_prop',
              TRUE ~ 'rows'
            )
        ) %>%
        mutate(
          .after = attr_hier,
          lvl_memb = coalesce(lvl, prop),
          attr_hier =
            case_when(
              field_type == 'rows' & attr_hier != lvl_memb & is.na(all_memb) ~
                lvl_memb,
              .default = attr_hier
            )
        ) %>%
        select(-c(lvl, prop))

    }
  )

}

# CDI ----
list_cdi_mdx <-
  list(
    `Vital Stats` =
      r"(

  [Measures].[VS Death Count]

  [VS - ID].[Unique ID].[Unique ID]
  [VS - Cause of Death - Underlying].[UCD Code Hierarchy].[UCD 5Char Code].ALLMEMBERS
  [VS - Date - Death].[Date].[Date]
  [CDI - Gender].[Sex].[Sex]
  [VS - Age at Death].[Age Year].[Age Year]
  [VS - Age at Death].[Age Group 07].[Age Group 07]
  [VS - Age at Death].[Age Group Population].[Age Group Population]
  [VS - Place of Injury Type].[Place of Injury Type].[Place of Injury Type]
  [VS - Geo - Residential Location Region].[VS Residential Location Health Authorities].[VS Residential Location LHA].ALLMEMBERS
  [VS - Geo - Death Location Region].[VS Death Location Health Authorities].[VS Death Location LHA].ALLMEMBERS
  [VS - Geo - Residential Location Census - 2011].[VS Residential Location Census - 2011].[VS Residential Dissemination Area].ALLMEMBERS
  [VS - Geo - Death Location Census - 2011].[VS Death Location Census - 2011].[VS Death Location Dissemination Area].ALLMEMBERS
    )",
`Vital Stats CCD` =
  r"(

  [Measures].[VS Death Count]

  [VS - ID].[Unique ID].[Unique ID]
  [VS - Cause of Death - Contributing].[CCD Code Hierarchy].[CCD 5Char Code].ALLMEMBERS
    )",
`Vital Stats CCD Dashboard` =
  r"(

  [Measures].[VS Death Count]

  [VS - ID].[Unique ID].[Unique ID]
  [VS - Cause of Death - Underlying].[UCD Code Hierarchy].[UCD 5Char Code].ALLMEMBERS
  [VS - Cause of Death - Contributing].[CCD Code Hierarchy].[CCD 4Char Code]
  [VS - Date - Death].[Date].[Date]
  [CDI - Gender].[Sex].[Sex]
  [VS - Age at Death].[Age Year].[Age Year]
  [VS - Age at Death].[Age Group 07].[Age Group 07]
  [VS - Age at Death].[Age Group Population].[Age Group Population]
  [VS - Place of Injury Type].[Place of Injury Type].[Place of Injury Type]
  [VS - Geo - Residential Location Region].[VS Residential Location Health Authorities].[VS Residential Location LHA].ALLMEMBERS
  [VS - Geo - Death Location Region].[VS Death Location Health Authorities].[VS Death Location LHA].ALLMEMBERS
  [VS - Geo - Residential Location Census - 2011].[VS Residential Location Census - 2011].[VS Residential Dissemination Area].ALLMEMBERS
  [VS - Geo - Death Location Census - 2011].[VS Death Location Census - 2011].[VS Death Location Dissemination Area].ALLMEMBERS
    )"
  ) %>%
  clean_mdx_strings(mart = 'CDI', cube = 'CDI') %>%
  map(
    bind_rows,
    tribble(
      ~ field_type,  ~ dim,                                    ~ attr_hier,                  ~ param_name,
      'filter_d',    'VS - Cause of Death - Underlying',       'UCD 3Char Code',             'ucd_3_char_code',
      'filter_d',    'VS - Cause of Death - Contributing',     'CCD 3Char Code',             'ccd_3_char_code',
      'filter_d',    'VS - Geo - Residential Location Region', 'VS Residential Location HA', 'residential_location_ha',
      'filter_d',    'VS - Geo - Death Location Region',       'VS Death Location HA',       'death_location_ha',
      'filter_r',    'VS - Date - Death',                      'Date',                       'query_date',
    ) %>%
      mutate(lvl_memb = attr_hier) %>%
      mutate(.after = field_type, check = 'none')
  ) %>%
  map(fill, c(mart, cube, dataset_name)) %>%
  map(replace_na, list(check = 'default')) %>%
  map(relocate, check, .after = field_type)

# Respiratory ----
list_respiratory_mdx <-
  list(
    `LIS Tests`    =
      r"(

  [Measures].[LIS Test Count]

  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [LIS - IDs].[Test ID].[Test ID]
  [LIS - IDs].[Accession Number].[Accession Number]
  [LIS - IDs].[Container ID].[Container ID]
  [LIS - Episode IDs].[Episode ID].[Episode ID]
  [LIS - Patient].[Gender].[Gender]
  [LIS - Patient].[Species].[Species]
  [LIS - Infection Group].[Infection Group].[Infection Group]
  [LIS - Date - Collection].[Date].[Date]
  [LIS - Date - Receive].[Date].[Date]
  [LIS - Date - Result].[Date].[Date]
  [LIS - Age at Collection].[Age Years].[Age Years]
  [LIS - Test].[Tests].[Test Code].ALLMEMBERS
  [LIS - Order Item].[Orders].[Order Code].ALLMEMBERS
  [LIS - Test].[Source System ID].[Source System ID]
  [LIS - Result - Test Outcome].[Test Outcome].[Test Outcome]
  [LIS - Result - Organism].[Organisms].[Organism Level 4].ALLMEMBERS
  [LIS - Geo - Patient Region].[LIS Patient Health Authorities].[LIS Patient LHA].ALLMEMBERS
  [LIS - Geo - Patient Address].[LIS Patient Addresses].[LIS Patient Postal Code].ALLMEMBERS
  [LIS - Geo - Ordering Provider Region ATOT].[LIS Ordering Provider Health Authorities].[LIS Ordering Provider LHA].ALLMEMBERS
  [LIS - Geo - Ordering Provider Address ATOT].[LIS Ordering Provider Addresses].[LIS Ordering Provider Postal Code].ALLMEMBERS
  [LIS - Lab Location - Result].[Lab Name].[Lab Name]
  [LIS - Flag - Proficiency Test].[Proficiency Test].[Proficiency Test]
  [LIS - Flag - Test Performed].[Test Performed].[Test Performed]
  [LIS - Flag - Test Valid Status].[Valid Test].[Valid Test]
  [LIS - Result Attributes].[Result Full Description].[Result Full Description]
    )",

  `LIS Episodes` =
    r"(

  [Measures].[LIS Episode Count]

  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [LIS - Episode IDs].[Episode ID].[Episode ID]
  [LIS - Infection Group].[Infection Group].[Infection Group]
  [LIS - Date - Episode Start].[Flu Season].[Date].ALLMEMBERS
  [LIS - Episode Attributes].[Episode Testing Pattern].[Episode Testing Pattern]
  [LIS - Rule Engine - Episode Status].[Rule Engine - Episode Status].[Rule Detail Description].ALLMEMBERS
    )"
  ) %>%
  clean_mdx_strings(mart = 'Respiratory', cube = 'RespiratoryDM') %>%
  map(mutate, check = 'default', .after = field_type) %>%
  imap(
    ~ bind_rows(
      .x,
      new_filter_rules %>%
        filter(
          .data$mart         == 'Respiratory',
          .data$dataset_name == .y,
        )
    )
  ) %>%
  map(fill, cube, .direction = 'down')

# STIBBI ----
list_stibbi_mdx <-
  list(
    Case =
      r"(
	[Measures].[Case Count]

  [Case - IDs].[Case Combined ID].[Case Combined ID]
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [Patient - Patient Master].[Gender].[Gender]
  [Patient - Patient Master].[Birth Year].[Birth Year]
  [Case - Disease].[Case Disease].[Case Disease]
  [Case - Date - Earliest].[Date].[Date]
  [Case - Date - Earliest From].[Case Earliest Date From].[Case Earliest Date From]
  [Case - Age at Earliest Date].[Age Years].[Age Years]
  [Case - Age at Earliest Date].[Age Group 10].[Age Group 10]
  [Case - Geo - Surveillance Region].[Case Surveillance Region Health Authorities].[Case Surveillance Region LHA].ALLMEMBERS
  [Case - Geo - Surveillance Region Based On].[Case Surveillance Region Based On].[Case Surveillance Region Based On]
  [Case - Geo - Testing Region].[Case Testing Region Health Authorities].[Case Testing Region LHA]
  [Case - Geo - Testing Region Based On].[Case Testing Region Based On].[Case Testing Region Based On]
  [Case - Sources].[Case Source].[Case Source]
  [Case - Status].[Case Status].[Case Status]
  [Case - Status - LIS].[Case Status LIS].[Case Status LIS]
  [Case - Status - PHS].[Case Status PHS].[Case Status PHS]
  [Case - Status Confirmed By].[Case Status Confirmed By].[Case Status Confirmed By]
  [Case - Status Determined By].[Case Status Determined By].[Case Status Determined By]

  [Case - HCV - Flag Acute].[Yes No].[Yes No]
  [Case - HCV - Flag Acute From].[HCV Acute Flag From].[HCV Acute Flag From]
  [Case - HCV - Flag Reinfection].[Yes No].[Yes No]
  [Case - HCV - Interval - Seroconversion Days].[HCV Days Since Prior Negative].[HCV Days Since Prior Negative]
    )",

	`Investigation` = r"(
	[Measures].[PHS Disease Event Count]

  [PHS - IDs].[Disease Event ID].[Disease Event ID]
  [Case - IDs].[Case Combined ID].[Case Combined ID]
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [PHS - Client].[Client ID].[Client ID]
  [PHS - Classification].[PHS Classification].[PHS Classification]
  [PHS - Date - Surveillance].[Date].[Date]
  [PHS - Date - Surveillance Reported].[Date].[Date]
  [PHS - Disease].[PHS Disease].[PHS Disease]
  [PHS - Stage of Infection - Earliest].[Earliest Stage of Infection].[Earliest Stage of Infection]
  [PHS - Surveillance Condition].[Surveillance Condition].[Surveillance Condition]
  [PHS - Etiologic Agent].[Etiologic Agent Level 1].[Etiologic Agent Level 1]
  [PHS - Etiologic Agent].[Etiologic Agent Level 2].[Etiologic Agent Level 2]
  [PHS - Geo - Surveillance Region].[PHS Surveillance Region Health Authorities].[PHS Surveillance Region LHA].ALLMEMBERS
  [PHS - Geo - Surveillance Region Based On].[Surveillance Region Based On].[Surveillance Region Based On]
  [PHS - Geo - Client Address ATOC Region].[PHS Client Address ATOC Health Authorities].[PHS Client Address ATOC LHA].ALLMEMBERS
  [PHS - Geo - Client Address ATOC].[PHS Client Addresses ATOC].[PHS Client Address ATOC Postal Code].ALLMEMBERS
  [PHS - Geo - Earliest Positive Ordering Provider Region].[PHS Earliest Positive Ordering Provider Health Authorities].[PHS Earliest Positive Ordering Provider LHA].ALLMEMBERS
  [PHS - Geo - Earliest Positive Ordering Provider Address].[PHS Earliest Positive Ordering Provider Address].[PHS Earliest Positive Ordering Provider Postal Code].ALLMEMBERS
  [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Key]
  [PHS - Source System].[Source System].[Source System]
  [PHS - Gender Of Partners].[Gender Of Partners].[Gender Of Partners]
  [PHS - Sexual Orientation].[Sexual Orientation].[Sexual Orientation]
  [PHS - Age at Surveillance Date].[Age Group 10].[Age Group 10]
  [PHS - Age at Surveillance Date].[Age Years].[Age Years]
  [PHS - Stage of Infection].[Stage of Infection].[Stage of Infection]

  [PHS - Indigenous First Nations Status].[First Nations Status].[First Nations Status]
  [PHS - Indigenous Identity].[Indigenous Identity].[Indigenous Identity]
  [PHS - Indigenous On Reserve Administered By].[On Reserve Administered By].[On Reserve Administered By]
  [PHS - Indigenous Organization].[Indigenous Organization].[Indigenous Organization]
  [PHS - Indigenous Self Identify].[Indigenous Self Identify].[Indigenous Self Identify]
  [PHS - HAISYS On Reserve].[HAISYS On Reserve].[HAISYS On Reserve]

  [PHS - STI Stage of Infection Category].[Stage of Infection Category].[Stage of Infection Category]

  [PHS - STIIS Risk Category].[STIIS Risk Category].[STIIS Risk Category]
  [PHS - STI Body Site Category].[STI Body Site Category].[STI Body Site Category]
  [PHS - Flag - Pregnant at Time of Case].[Pregnant at Time of Case Flag].[Pregnant at Time of Case Flag]

  [PHS - HIV - Client Stage of Infection].[HIV Client Stage of Infection].[HIV Client Stage of Infection]
  [PHS - HIV - Client Stage of Infection].[HIV Stage of Infection Based On].[HIV Stage of Infection Based On]
  [PHS - HIV - Diagnosis Test Type].[HIV Diagnosis Test Type].[HIV Diagnosis Test Type]
  [PHS - HIV - Exposure Category].[HAISYS Exposure Category].[HAISYS Exposure Category]
  [PHS - HIV - Flag - Non Nominal].[HIV Non Nominal Flag].[HIV Non Nominal Flag]
  [PHS - HIV - First AIDS Defining Illness].[First AIDS Defining Illnesses].[First AIDS Defining Illness].ALLMEMBERS
  [PHS - HIV - Reporting Category].[HIV Reporting Category].[HIV Reporting Category]
  [PHS - HIV - Source of Diagnosis].[HIV Source of Diagnosis].[HIV Source of Diagnosis]
  [PHS - HIV - When AIDS Diagnosed].[When AIDS Diagnosed].[When AIDS Diagnosed]
  [PHS - HIV - Flag - Prenatal Screen].[HIV Prenatal Screen Flag].[HIV Prenatal Screen Flag]

  [PHS - IDs].[Disease Event ID].[Investigation ID]
  [PHS - IDs].[Disease Event ID].[EMR Form ID]

  [PHS - IDs].[Disease Event ID].[Date - Investigation Created],
  [PHS - IDs].[Disease Event ID].[Date - Investigation Updated],
  [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Code],
  [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Clinic Name],
  [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Clinic Code],
  [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Address Flag],
  [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider Name]

  [PHS - Earliest Positive Ordering Provider].[Earliest Positive Ordering Provider Key].[Earliest Positive Ordering Provider EMR Form Tab]
    )",

	`Client` = r"(

	[Measures].[PHS Client Count]

  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [Patient - Patient Master].[Birth Year].[Birth Year]
  [PHS - Client].[Client ID].[Client ID]
  [PHS - Client].[Ethnicity].[Ethnicity]
  [PHS - Client].[Gender].[Gender]
  [PHS - Client].[Gender Identity].[Gender Identity]
  [PHS - Client].[Other Ethnicity].[Other Ethnicity]

  [PHS - Client].[HAISYS Chart Number].[HAISYS Chart Number]
  [PHS - Client].[Haisys STI ID].[Haisys STI ID]

  [PHS - Client].[STIIS Client Number].[STIIS Client Number]
  [PHS - Client].[EMR Client ID].[EMR Client ID]
    )",

	`PHS Body Site` = r"(

	[Measures].[PHS Disease Event Count]

  [PHS - IDs].[Disease Event ID].[Disease Event ID]
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [PHS - Client].[Client ID].[Client ID]
  [PHS - Body Site and Drug Resistance].[Body Site Affected].[Body Site Affected]
  [PHS - Body Site and Drug Resistance].[Sensitivity Interpretation].[Sensitivity Interpretation]
  [PHS - Body Site and Drug Resistance].[Antimicrobial Drug].[Antimicrobial Drug]

  [PHS - IDs].[Disease Event ID].[Investigation ID]
    )",

	`PHS Client Risk Factor` = r"(

	[Measures].[PHS Disease Event Count]

  [PHS - IDs].[Disease Event ID].[Disease Event ID]
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [PHS - Client].[Client ID].[Client ID]
  [PHS - Risk Factor - Client].[Client Risk Factor].[Client Risk Factor]
  [PHS - Risk Factor - Client].[Client Risk Factor Response].[Client Risk Factor Response]
  [PHS - Risk Factor - Client].[Client Risk Factor Specify]
  [PHS - Risk Factor - Client].[Client Risk Factor End Reason].[Client Risk Factor End Reason]
  [PHS - Risk Factor - Client].[Date - Client Risk Factor Start].[Date - Client Risk Factor Start]
  [PHS - Risk Factor - Client].[Date - Client Risk Factor End].[Date - Client Risk Factor End]
  [PHS - Risk Factor - Client].[Date - Client Risk Factor Reported].[Date - Client Risk Factor Reported]

  [PHS - IDs].[Disease Event ID].[Investigation ID]
    )",

	`PHS Investigation Risk Factor` = r"(

	[Measures].[PHS Disease Event Count]

  [PHS - IDs].[Disease Event ID].[Disease Event ID]
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [PHS - Client].[Client ID].[Client ID]
  [PHS - Risk Factor - Investigation].[Investigation Risk Factor].[Investigation Risk Factor]
  [PHS - Risk Factor - Investigation].[Investigation Risk Factor Response].[Investigation Risk Factor Response]
  [PHS - Risk Factor - Investigation].[Investigation Risk Factor Specify].[Investigation Risk Factor Specify]
  [PHS - Risk Factor - Investigation].[Investigation Risk Factor End Reason].[Investigation Risk Factor End Reason]
  [PHS - Risk Factor - Investigation].[Date - Investigation Risk Factor Start].[Date - Investigation Risk Factor Start]
  [PHS - Risk Factor - Investigation].[Date - Investigation Risk Factor End].[Date - Investigation Risk Factor End]
  [PHS - Risk Factor - Investigation].[Date - Investigation Risk Factor Reported].[Date - Investigation Risk Factor Reported]

  [PHS - IDs].[Disease Event ID].[Investigation ID]
    )",

	`PHS UDF Long` = r"(

	[Measures].[PHS Disease Event Count]

	[PHS - IDs].[Disease Event ID].[Disease Event ID]
	[Patient - Patient Master].[Patient Master Key].[Patient Master Key]
	[PHS - Client].[Client ID].[Client ID]
	[PHS - UDF - All].[UDF All Key].[UDF All Key]

  [PHS - IDs].[Disease Event ID].[Investigation ID]
  [PHS - UDF - All].[UDF All Key].[Section Name]
  [PHS - UDF - All].[UDF All Key].[Question Keyword Common]
  [PHS - UDF - All].[UDF All Key].[Answer Value]
  [PHS - UDF - All].[UDF All Key].[Answer Row ID]
  [PHS - UDF - All].[UDF All Key].[Section Sort ID]
  [PHS - UDF - All].[UDF All Key].[Question Sort ID]
  [PHS - UDF - All].[UDF All Key].[UDF Name]
  [PHS - UDF - All].[UDF All Key].[UDF Template Version]
    )",

	`LIS Tests` = r"(

  [Measures].[LIS Test Count]

  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [LIS - IDs].[Test ID].[Test ID]
  [LIS - Patient].[Patient Key].[Patient Key]
  [LIS - Infection Group].[Infection Group].[Infection Group]
  [LIS - Date - Collection].[Date].[Date]
  [LIS - Date - Receive].[Date].[Date]
  [LIS - Date - Result].[Date].[Date]
  [LIS - Age at Collection].[Age Years].[Age Years]
  [LIS - Age at Collection].[Age Group 09].[Age Group 09]
  [LIS - Age at Collection].[Age Group 10].[Age Group 10]
  [LIS - Age at Collection].[Age Group 24].[Age Group 24]
  [LIS - Test].[Tests].[Test Name].ALLMEMBERS
  [LIS - Test].[Order Code].[Order Code]
  [LIS - Test].[Test Code].[Test Code]
  [LIS - Test].[Source System ID].[Source System ID]
  [LIS - Result Attributes].[Test Outcome].[Test Outcome]
  [LIS - Result - Organism].[Organisms].[Organism Level 3].ALLMEMBERS
  [LIS - Geo - Patient Region ATOT].[LIS Patient Health Authorities ATOT].[LIS Patient LHA ATOT].ALLMEMBERS
  [LIS - Geo - Patient Address ATOT].[LIS Patient Addresses ATOT].[LIS Patient Postal Code ATOT].ALLMEMBERS
  [LIS - Patient Location at Order].[Hospital Code].[Hospital Code]
  [LIS - Patient Location at Order].[Location Type].[Location Type]
  [LIS - Patient Location at Order].[Patient Location Code].[Patient Location Code]
  [LIS - Patient Location at Order].[Patient Location Name].[Patient Location Name]
  [LIS - Lab Location - Result].[Lab Locations].[Hospital Code]
  [LIS - Lab Location - Result].[Lab Code].[Lab Code]
  [LIS - Lab Location - Result].[Lab Name].[Lab Name]
  [LIS - Lab Location - Result].[Lab Location Code].[Lab Location Code]
  [LIS - Lab Location - Result].[Lab Location Name].[Lab Location Name]
  [LIS - Lab Location - Result].[Lab Location Description].[Lab Location Description]
  [LIS - Lab Location - Order Entry].[Lab Locations].[Hospital Code]
  [LIS - Lab Location - Order Entry].[Lab Code].[Lab Code]
  [LIS - Lab Location - Order Entry].[Lab Name].[Lab Name]
  [LIS - Lab Location - Order Entry].[Lab Location Code].[Lab Location Code]
  [LIS - Lab Location - Order Entry].[Lab Location Name].[Lab Location Name]
  [LIS - Lab Location - Order Entry].[Lab Location Description].[Lab Location Description]
  [LIS - Flag - Prenatal Test].[Prenatal Flag].[Prenatal Flag]
  [LIS - Flag - Proficiency Test].[Yes No].[Yes No]
  [LIS - Flag - Test Performed].[Test Performed].[Test Performed]
  [LIS - Result Attributes].[Result Full Description].[Result Full Description]

  [LIS - IDs].[Test ID].[Episode ID]
  [LIS - IDs].[Test ID].[Accession Number]
  [LIS - IDs].[Test ID].[Container ID]
  [LIS - Patient].[Patient Key].[Birth Year]
  [LIS - Patient].[Patient Key].[Gender]
    )",

	`LIS Test Providers` = r"(

  [Measures].[LIS Test Count]

  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [LIS - Patient].[Patient Key].[Patient Key]
  [LIS - IDs].[Test ID].[Test ID]
  [LIS - Date - Collection].[Date].[Date]
  [LIS - Date - Receive].[Date].[Date]
  [LIS - Date - Episode Start].[Date].[Date]
  [LIS - Infection Group].[Infection Group].[Infection Group]
  [LIS - Provider - Ordering].[Provider Key].[Provider Key]
  [LIS - Geo - Ordering Provider Region ATOT].[LIS Ordering Provider Health Authorities ATOT].[LIS Ordering Provider LHA ATOT].ALLMEMBERS
  [LIS - Geo - Ordering Provider Address ATOT].[LIS Ordering Provider Addresses ATOT].[LIS Ordering Provider Postal Code ATOT].ALLMEMBERS
  [LIS - Provider - Copy To 1].[Provider Key].[Provider Key]
  [LIS - Provider - Copy To 2].[Provider Key].[Provider Key]
  [LIS - Provider - Copy To 3].[Provider Key].[Provider Key]

  [LIS - IDs].[Test ID].[Episode ID]
  [Patient - Patient Master].[Patient Master Key].[Birth Year]
  [LIS - IDs].[Test ID].[Accession Number]
  [LIS - IDs].[Test ID].[Container ID]
  [LIS - Provider - Ordering].[Provider Key].[Provider Code]
  [LIS - Provider - Ordering].[Provider Key].[Provider Type]
  [LIS - Provider - Ordering].[Provider Key].[Provider Group]
  [LIS - Provider - Ordering].[Provider Key].[Provider Name]
  [LIS - Provider - Ordering].[Provider Key].[Full Address]
  [LIS - Provider - Copy To 1].[Provider Key].[Provider Code]
  [LIS - Provider - Copy To 1].[Provider Key].[Provider Type]
  [LIS - Provider - Copy To 1].[Provider Key].[Provider Group]
  [LIS - Provider - Copy To 1].[Provider Key].[Provider Name]
  [LIS - Provider - Copy To 1].[Provider Key].[Full Address]
  [LIS - Provider - Copy To 2].[Provider Key].[Provider Code]
  [LIS - Provider - Copy To 2].[Provider Key].[Provider Type]
  [LIS - Provider - Copy To 2].[Provider Key].[Provider Group]
  [LIS - Provider - Copy To 2].[Provider Key].[Provider Name]
  [LIS - Provider - Copy To 2].[Provider Key].[Full Address]
  [LIS - Provider - Copy To 3].[Provider Key].[Provider Code]
  [LIS - Provider - Copy To 3].[Provider Key].[Provider Type]
  [LIS - Provider - Copy To 3].[Provider Key].[Provider Group]
  [LIS - Provider - Copy To 3].[Provider Key].[Provider Name]
  [LIS - Provider - Copy To 3].[Provider Key].[Full Address]
    )",

	`LIS Episodes` = r"(

  [Measures].[LIS Episode Count]

  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [Patient - Patient Master].[Birth Year].[Birth Year]
  [LIS - IDs].[Test ID].[Test ID]
  [Case - IDs].[Case Combined ID].[Case Combined ID]
  [LIS - Infection Group].[Infection Group].[Infection Group]
  [LIS - Date - Episode Start].[Date].[Date]
  [LIS - Date - Episode End].[Date].[Date]
  [LIS - Gender at Episode Start].[Gender].[Gender]
  [LIS - Gender at Episode Start].[Gender Code].[Gender Code]
  [LIS - Age at Episode Start].[Age Years].[Age Years]
  [LIS - Age at Episode Start].[Age Group 09].[Age Group 09]
  [LIS - Age at Episode Start].[Age Group 10].[Age Group 10]
  [LIS - Age at Episode Start].[Age Group 24].[Age Group 24]
  [LIS - Geo - Patient Region].[LIS Patient Health Authorities].[LIS Patient LHA].ALLMEMBERS
  [LIS - Geo - Patient Region Proxy].[LIS Patient Health Authorities Proxy].[LIS Patient LHA Proxy].ALLMEMBERS
  [LIS - Geo - Patient Address].[LIS Patient Addresses].[LIS Patient Postal Code].ALLMEMBERS
  [LIS - Geo - Ordering Provider Region].[LIS Ordering Provider Health Authorities].[LIS Ordering Provider LHA].ALLMEMBERS
  [LIS - Geo - Ordering Provider Region Proxy].[LIS Ordering Provider Health Authorities Proxy].[LIS Ordering Provider LHA Proxy].ALLMEMBERS
  [LIS - Geo - Ordering Provider Address].[LIS Ordering Provider Addresses].[LIS Ordering Provider Postal Code].ALLMEMBERS
  [LIS - Episode Testing Pattern].[Episode Testing Pattern].[Episode Testing Pattern]
  [LIS - Flag - Prenatal Episode].[Prenatal Flag].[Prenatal Flag]
  [LIS - Flag - Reinfection].[Yes No].[Yes No]
  [LIS - Flag - Repeat Tester].[Yes No].[Yes No]
  [LIS - Interval - Days Since First Episode].[Interval in Days].[Interval in Days]
  [LIS - Interval - Days Since First Episode].[Interval Group 1].[Interval Group 1]
  [LIS - Interval - Days Since Prior Episode].[Interval in Days].[Interval in Days]
  [LIS - Interval - Days Since Prior Episode].[Interval Group 1].[Interval Group 1]
  [LIS - Interval - Days Since Prior Pos or Neg Episode].[Interval in Days].[Interval in Days]
  [LIS - Interval - Days Since Prior Pos or Neg Episode].[Interval Group 1].[Interval Group 1]
  [LIS - Rule Engine - Episode Status].[Rule Engine - Episode Status].[Rule Detail Description].ALLMEMBERS

  [LIS - IDs].[Test ID].[Episode ID]
  [LIS - IDs].[Test ID].[Episode Seq]
    )",

	`LIS POC` = r"(

  [Measures].[POC Test Count]
  [Measures].[Non Reactive POC Test Count]
  [Measures].[Indeterminate POC Test Count]
  [Measures].[Reactive POC Test Count]

	[LIS - Infection Group].[Infection Group].[Infection Group]
	[LIS - POC - Test Location].[Health Authority].[Health Authority]
	[LIS - POC - Test Location].[HSDA].[HSDA]
	[LIS - POC - Test Location].[City].[City]
	[LIS - POC - Test Location].[Site Name].[Site Name]
	[LIS - Date - POC Reporting].[Date].[Date]
    )"
  ) %>%
  clean_mdx_strings(mart = 'STIBBI', cube = 'StibbiDM') %>%
  ## checks ----
  imap(
    ~ {

      if (.y == 'Case') {

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(dim, regex('hcv', ignore_case = T)) ~ 'hvc',
              TRUE                                           ~ 'default'
            )
        )

      } else if (.y == 'Investigation') {

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(dim, regex('indige|haisys', ignore_case = T)) ~
                'indigenous_id',
              str_detect(dim, regex('sti stage of inf', ignore_case = T)) ~
                'syphilis',
              str_detect(dim, regex('stiis|sti body|flag - preg', ignore_case = T)) ~
                'syphilis|chlamydia|gonorrhea|lymphogranuloma',
              str_detect(lvl_memb, regex('emr form', ignore_case = T)) ~
                'syphilis|chlamydia|gonorrhea|lymphogranuloma',
              str_detect(dim, regex('hiv', ignore_case = T)) ~
                'hiv|aids',
              TRUE ~ 'default'
            )
        )

      } else if (.y == 'Client') {

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(attr_hier, regex('haisys', ignore_case = T)) ~
                'hiv|aids',
              str_detect(attr_hier, regex('stiis', ignore_case = T))  ~
                'syphilis|chlamydia|gonorrhea|lymphogranuloma',
              TRUE ~ 'default'
            )
        )

      } else {

        mutate(
          .x,
          .after = field_type,
          check = 'default'
        )

      }

    }
  )

list_stibbi_mdx %<>%
  imap(
    ~ bind_rows(
      .x,
      new_filter_rules %>%
        filter(
          .data$mart         == 'STIBBI',
          .data$dataset_name == .y,
        )
    )
  ) %>%
  map(fill, cube, .direction = 'down')

# STIBBI QA ----

# Enteric ----
list_enteric_mdx <-
  list(
    `Case Investigation` =
      r"(

	[Measures].[Case Count]

  [Case - IDs].[Case Combined ID].[Case Combined ID]
  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [Case - Date - Earliest].[Epi-Year-Week].[Date]
  [Case - Date Earliest Based On].[Earliest Date Based On].[Earliest Date Based On]
  [Patient - Patient Master].[Gender].[Gender]
  [Case - Match Status].[Case Match Description].[Case Match Description]
  [Case - Disease].[Case Disease].[Case Disease]
  [Case - Surveillance Condition].[Surveillance Condition].[Surveillance Condition]
  [Pan - Date - Surveillance Reported].[Epi-Year-Week].[Date]
  [Pan - Date - Onset Symptom].[Date].[Date]
  [Pan - Investigation Classification].[Classification].[Classification]
  [Pan - Investigation Status].[Investigation Status].[Investigation Status]
  [Pan - Investigation Outcome].[Last Outcome].[Last Outcome]
  [Pan - Surveillance Region].[Pan Surveillance Region HA].[Pan Surveillance Region HA]
  [Pan - Surveillance Region].[Pan Surveillance Region HSDA].[Pan Surveillance Region HSDA]
  [Pan - Surveillance Region].[Pan Surveillance Region LHA].[Pan Surveillance Region LHA]
  [Pan - Address at Time of Case].[Address Location].[Address At Time Of Case Postal Code].ALLMEMBERS
  [Pan - Age At Surveillance Reported Date].[Age Years].[Age Years]
  [Pan - Age At Surveillance Reported Date].[Age Group 5].[Age Group 5]
  [Pan - Age At Surveillance Reported Date].[Age Group 10].[Age Group 10]
  [Pan - Etiologic Agent].[Etiologic Agent Level].[Etiologic Agent Level 2].ALLMEMBERS
  [Pan - Etiologic Agent].[Etiologic Agent Further Differentiation].[Etiologic Agent Further Differentiation]

  [Case - IDs].[FHA PARIS Client Id].[FHA PARIS Client Id]
  [Case - IDs].[VCH PARIS Client Id].[VCH PARIS Client Id]

  [Case - IDs].[Case Combined ID].[Case ID]

  [Case - IDs].[Case Combined ID].[Investigation ID]
  [Case - IDs].[Case Combined ID].[Disease Event ID]
  [Case - IDs].[Case Combined ID].[Pan Client ID]
  [Case - IDs].[Case Combined ID].[Paris Assessment Number]
    )",

	`Symptom` =
	  r"(

  [Measures].[Case Count]

  [Case - IDs].[Case Combined ID].[Case Combined ID]
  [Pan - Symptom].[Symptom].[Symptom]
  [Pan - Symptom].[Symptom Response].[Symptom Response]

  [Case - IDs].[Case Combined ID].[Case ID]
    )",

	`Risk Factor` =
	  r"(

  [Measures].[Case Count]

  [Case - IDs].[Case Combined ID].[Case Combined ID]
  [Pan - Risk Factor - Investigation].[Risk Factor].[Risk Factor]
  [Pan - Risk Factor - Investigation].[Risk Factor Response].[Risk Factor Response]

  [Case - IDs].[Case Combined ID].[Case ID]
    )",

	`UDF` =
	  r"(

  [Measures].[Case Count]

  [Case - IDs].[Case Combined ID].[Case ID]

  [Case - IDs].[Case Combined ID].[Case Combined ID]
  [Pan - UDF - All].[UDF Question].[Question Keyword Common].ALLMEMBERS
  [Pan - UDF - All].[Answer Value].[Answer Value]
  [Pan - UDF - All].[Answer Row ID].[Answer Row ID]
  [Pan - UDF - All].[Question Sort ID].[Question Sort ID]
  [Pan - UDF - All].[UDF Template Version].[UDF Template Version]
  [Pan - UDF - All].[UDF Name].[UDF Name]

  [Case - IDs].[Case Combined ID].[Case ID],
  [Case - IDs].[Case Combined ID].[Disease Event ID]
    )",

	`LIS Data` =
	  r"(

  [Measures].[Case Count]

  [Case - IDs].[Case Combined ID].[Case Combined ID]
  [LIS - Date - Collection Date].[Date].[Date]
  [LIS - Date - Order Entry].[Date].[Date]
  [LIS - Date - Receive Date].[Date].[Date]
  [LIS - Date - Result Date].[Date].[Date]
  [LIS - Age At Collection].[Age Years].[Age Years]
  [LIS - Flag - Proficiency Test].[Proficiency Test].[Proficiency Test]
  [LIS - Flag - Test Performed].[Test Performed].[Test Performed]
  [LIS - Specimen].[Specimen Description].[Specimen Description]
  [LIS - Microorganism].[Organism].[Serotype].ALLMEMBERS
  [LIS - Patient City].[Patient City Name].[Patient City Name]
  [LIS - Patient Health Authority].[Patient Health Authorities].[Patient Local Health Area].ALLMEMBERS
  [LIS - Result Attributes].[Organism Isolated].[Organism Isolated]
  [LIS - Result Attributes].[Result Full Description].[Result Full Description]
  [LIS - Result Attributes - Direct Exam].[Direct Exam Result Full Description].[Direct Exam Result Full Description]
  [LIS - Result Attributes - Direct Exam].[Shiga Toxin Result].[Shiga Toxin Result]
  [LIS - Test].[Order Code].[Order Code]
  [LIS - Test].[Test Code].[Test Code]
  [LIS - Copy To Provider 1].[Provider Types].[Provider Name].ALLMEMBERS
  [LIS - Order Entry Lab Location].[Lab Location].[Lab Location Code].ALLMEMBERS
  [LIS - Ordering Provider].[Provider Types].[Provider Name].ALLMEMBERS
  [LIS - Ordering Provider City].[Ordering Provider City Name].[Ordering Provider City Name]
  [LIS - Ordering Provider Health Authority].[Ordering Provider Health Authorities].[Ordering Provider Local Health Area].ALLMEMBERS
  [LIS - Bionumerics Result WGS].[WGS Cluster Code].[WGS Cluster Code]
  [LIS - Bionumerics Result].[Pfge Apai Pattern].[Pfge Apai Pattern]
  [LIS - Bionumerics Result].[Pfge Asci Pattern].[Pfge Asci Pattern]
  [LIS - Bionumerics Result].[Pfge Blnl Pattern].[Pfge Blnl Pattern]
  [LIS - Bionumerics Result].[Pfge Notl Pattern].[Pfge Notl Pattern]
  [LIS - Bionumerics Result].[Pfge Xbal Pattern].[Pfge Xbal Pattern]
  [LIS - Bionumerics Result].[Phage Type].[Phage Type]

	[Case - IDs].[Case Combined ID].[Case ID]

  [Case - IDs].[Case Combined ID].[Container ID]
  [Case - IDs].[Case Combined ID].[Accession Number]
  [Case - IDs].[Case Combined ID].[Parent Container ID]
    )"
  ) %>%
  clean_mdx_strings(mart = 'Enteric', cube = 'EntericDM') %>%
  ## checks ----
  imap(
    ~ {

      if (.y == 'Case Investigation') {

        sys_ids <-
          c(
            'Investigation ID',
            'Disease Event ID',
            'Pan Client ID',
            'Paris Assessment Number'
          ) %>% paste(collapse = '|')

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(lvl_memb, sys_ids) ~ 'system_ids',
              TRUE ~ 'default'
            )
        )

      } else if (.y == 'LIS Data') {

        sys_ids <-
          c(
            'Container ID',
            'Accession Number',
            'Parent Container ID'
          ) %>% paste(collapse = '|')

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(lvl_memb, sys_ids) ~ 'system_ids',
              TRUE ~ 'default'
            )
        )

      } else {

        mutate(
          .x,
          .after = field_type,
          check = 'default'
        )

      }

    }
  ) %>%
  map(
    ~ bind_rows(
      .x,
      tribble(
        ~ field_type,  ~ check,   ~ dim,
        'filter_d',    'none',    'Case - Disease',
        'filter_d',    'none',    'Case - Surveillance Condition',
        'filter_d',    'none',    'Pan - Investigation Classification',
        'filter_d',    'none',    'Pan - Surveillance Region',
        'filter_r',    'none',    'Case - Date - Earliest',
        'filter_d',    'default', 'Pan - Investigation Classification',
        'filter_d',    'default', 'Pan - Investigation Classification',
        'filter_d',    'default', 'LIS - Patient',
        'filter_d',    'default', 'LIS - Patient',
        'filter_d',    'default', 'Patient - Match Level',
      ) %>%
        bind_cols(
          tribble(
            ~ attr_hier,                  ~ param_name,
            'Case Disease',               'disease',
            'Surveillance Condition',     'surveillance_condition',
            'Classification',             'classification',
            'Pan Surveillance Region HA', 'surveillance_region_ha',
            'Date',                       'query_date',
            'Classification Group',       NA_character_,
            'Classification Group',       NA_character_,
            'Species Category',           NA_character_,
            'Species Category',           NA_character_,
            'Match Level Description',    NA_character_,
          )
        ) %>%
        bind_cols(
          tribble(
            ~ all_memb,
            NA_character_,
            NA_character_,
            NA_character_,
            NA_character_,
            NA_character_,
            '**No Investigation',
            'Case',
            'Human',
            '**No Test',
            '3',
          )
        ) %>%
        mutate(lvl_memb = attr_hier)
    ) %>%
      fill(c(mart, cube, dataset_name))
  )

# VPD ----
list_vpd_mdx <-
  c(
    `Case Investigation` =
      r"(

  [Measures].[Case Count]

  [Patient - Patient Master].[Patient Master Key].[Patient Master Key]
  [Case - IDs].[Case ID].[Case ID]
  [Case - IDs].[Investigation ID].[Investigation ID]
  [Case - IDs].[Disease Event ID].[Disease Event ID]
  [Case - IDs].[PHS Client ID].[PHS Client ID]
  [Case - IDs].[VCH PARIS Client Id].[VCH PARIS Client Id]
  [Case - IDs].[FHA PARIS Client Id].[FHA PARIS Client Id]
  [Case - IDs].[PARIS Assessment Number].[PARIS Assessment Number]
  [Case - Date].[Date].[Date]
  [Case - Date].[Epi-Year].[Epi-Year]
  [Case - Date].[Epi-Week].[Epi-Week]
  [Case - Date - Based On].[Case Date Based On].[Case Date Based On]
  [Case - Disease].[Case Disease].[Case Disease]

  [Case - Serotype].[Case Serotype].[Case Serotype]

  [Patient - Patient Master].[Gender].[Gender]
  [Patient - Patient Master].[Birth Date].[Birth Date]
  [Case - Age at Case Date].[Age Group 10].[Age Group 10]
  [Case - Age at Case Date].[Age Year].[Age Year]
  [Case - Age at Case Date].[Age Month].[Age Month]

  [Case - Age at Case Date].[Age Day].[Age Day]
  [Case - Age at Case Date].[NGBS].[Age Group Ngbs 2].ALLMEMBERS

  [Case - Age at Case Date].[IPD].[Age Group Ipd 2].ALLMEMBERS

  [Case - Age at Case Date].[Age Group Mumps].[Age Group Mumps]

  [Case - Age at Case Date].[Age Group Pertussis].[Age Group Pertussis]
  [PHS - Date - Paroxysmal Onset].[Date].[Date]

  [Case - Sources].[Case Source].[Case Source]

  [Case - Status - LIS].[Case Status LIS].[Case Status LIS]

  [Case - Classification - PHS].[Case Classifications].[Case Classification].ALLMEMBERS
  [Case - Status].[Case Status].[Case Status]
  [Case - Geo - Surveillance Region].[Case Surveillance Region Health Authorities].[Case Surveillance Region LHA].ALLMEMBERS
  [Case - Geo - Surveillance Region Based On].[Case Surveillance Region Based On].[Case Surveillance Region Based On]
  [PHS - Geo - Client Address ATOC].[PHS Client Addresses ATOC].[PHS Client Address ATOC Postal Code].ALLMEMBERS

  [Case - Discordant Status].[Discordant Classification].[Discordant Classification]
  [Case - Discordant Status].[Discordant Etiologic Agent 2].[Discordant Etiologic Agent 2]

  [Case - Discordant Status].[Discordant Infection Group].[Discordant Infection Group]

  [PHS - Etiologic Agent].[Etiologic Agent Levels].[Etiologic Agent Level 2].ALLMEMBERS
  [PHS - Etiologic Agent].[Etiologic Agent Further Differentiation].[Etiologic Agent Further Differentiation]
  [PHS - Date - Onset Symptom].[Date].[Date]
  [PHS - Date - Surveillance Reported].[Date].[Date]
  [PHS - Symptom - All].[Onset Symptom].[Onset Symptom]

  [PHS - Investigation Last Outcome].[Last Outcome].[Last Outcome]
  [PHS - Date - Last Outcome].[Date].[Date]

  [PHS - Investigation Last Outcome].[Cause of Death].[Cause of Death]

  [PHS - Stage of Infection].[Stage of Infection].[Stage of Infection]

  [PHS - Complication].[Encephalitis].[Encephalitis]
  [PHS - Complication].[Meningitis].[Meningitis]

  [PHS - Complication].[Permanent Hearing Loss].[Permanent Hearing Loss]

  [PHS - Complication].[Pneumonia].[Pneumonia]
   )",

  `Symptoms` =
    r"(

  [Measures].[Case Count]

  [Case - IDs].[Case ID].[Case ID]
  [Case - IDs].[Investigation ID].[Investigation ID]
  [Case - Disease].[Case Disease].[Case Disease]

  [PHS - Symptom].[Arthralgia].[Arthralgia]

  [PHS - Symptom].[Arthritis].[Arthritis]

  [PHS - Symptom].[Bacteremia].[Bacteremia]

  [PHS - Symptom].[Cellulitis].[Cellulitis]

  [PHS - Symptom].[Conjunctivitis].[Conjunctivitis]

  [PHS - Symptom].[Coryza].[Coryza]

  [PHS - Symptom].[Cough].[Cough]

  [PHS - Symptom].[Cough Associated With Apnea].[Cough Associated With Apnea]
  [PHS - Symptom].[Cough Ending In Inspriatory Whoop].[Cough Ending In Inspriatory Whoop]
  [PHS - Symptom].[Cough Ending In Vomiting Or Gagging].[Cough Ending In Vomiting Or Gagging]
  [PHS - Symptom].[Cough Lasting More Than 2 Weeks].[Cough Lasting More Than 2 Weeks]
  [PHS - Symptom].[Cough Paroxysmal].[Cough Paroxysmal]

  [PHS - Symptom].[Fever].[Fever]

  [PHS - Symptom].[Lymphadenopathy].[Lymphadenopathy]

  [PHS - Symptom].[Lymphadenopathy Occipital].[Lymphadenopathy Occipital]
  [PHS - Symptom].[Lymphadenopathy Post - Auricular].[Lymphadenopathy Post - Auricular]
  [PHS - Symptom].[Lymphadenopathy Posterior Cervical].[Lymphadenopathy Posterior Cervical]

  [PHS - Symptom].[Meningitis].[Meningitis]

  [PHS - Symptom].[Necrotizing Fasciiti - Myositis - Gangrene].[Necrotizing Fasciiti - Myositis - Gangrene]

  [PHS - Symptom].[Orchitis].[Orchitis]

  [PHS - Symptom].[Other].[Other]

  [PHS - Symptom].[Parotitis Bilateral].[Parotitis Bilateral]
  [PHS - Symptom].[Parotitis Unilateral].[Parotitis Unilateral]

  [PHS - Symptom].[Peri - Partum Fever].[Peri - Partum Fever]

  [PHS - Symptom].[Pneumonia].[Pneumonia]

  [PHS - Symptom].[Purpura Fulminans - Meningococcemia].[Purpura Fulminans - Meningococcemia]

  [PHS - Symptom].[Rash Maculopapular].[Rash Maculopapular]

  [PHS - Symptom].[Rash Petechial].[Rash Petechial]

  [PHS - Symptom].[Toxic Shock Syndrome].[Toxic Shock Syndrome]

   )",

  `Symptoms Long` =
    r"(

  [Measures].[Case Count]

  [Case - IDs].[Case ID].[Case ID]
  [Case - IDs].[Investigation ID].[Investigation ID]
  [Case - Disease].[Case Disease].[Case Disease]
  [PHS - Symptom - All].[Symptom].[Symptom]
  [PHS - Symptom - All].[Symptom Response].[Symptom Response]
  [PHS - Symptom - All].[Symptom Date].[Symptom Date]
   )",

  `Risk Factors` =
    r"(

  [Measures].[Case Count]

  [Case - IDs].[Case ID].[Case ID]
  [Case - IDs].[Investigation ID].[Investigation ID]
  [Case - Disease].[Case Disease].[Case Disease]

  [PHS - Risk Factor].[Alcohol Use].[Alcohol Use]
  [PHS - Risk Factor].[Chronic Cardiac Condition].[Chronic Cardiac Condition]

  [PHS - Risk Factor].[Chronic CSF Leak].[Chronic CSF Leak]
  [PHS - Risk Factor].[Chronic Liver Disease].[Chronic Liver Disease]
  [PHS - Risk Factor].[Chronic Liver Disease Specify].[Chronic Liver Disease Specify]
  [PHS - Risk Factor].[Chronic Renal Disease].[Chronic Renal Disease]
  [PHS - Risk Factor].[Cystic Fibrosis].[Cystic Fibrosis]

  [PHS - Risk Factor].[Chronic Respiratory Pulmonary Condition].[Chronic Respiratory Pulmonary Condition]
  [PHS - Risk Factor].[Diabetes Mellitus].[Diabetes Mellitus]

  [PHS - Risk Factor].[Homelessness Underhoused].[Homelessness Underhoused]

  [PHS - Risk Factor].[Immunocompromised Other].[Immunocompromised Other]
  [PHS - Risk Factor].[Immunocompromised Other Specify].[Immunocompromised Other Specify]
  [PHS - Risk Factor].[Immunocompromised Transplant].[Immunocompromised Transplant]
  [PHS - Risk Factor].[Immunocompromised Transplant Specify].[Immunocompromised Transplant Specify]
  [PHS - Risk Factor].[Immunocompromised Treatment].[Immunocompromised Treatment]
  [PHS - Risk Factor].[Immunocompromised Treatment Specify].[Immunocompromised Treatment Specify]
  [PHS - Risk Factor].[Immunocompromising Condition].[Immunocompromising Condition]
  [PHS - Risk Factor].[Immunocompromising Condition Specify].[Immunocompromising Condition Specify]

  [PHS - Risk Factor].[Injection Drug Use].[Injection Drug Use]

  [PHS - Risk Factor].[Malignancies Cancer].[Malignancies Cancer]

  [PHS - Risk Factor].[MSM].[MSM]

  [PHS - Risk Factor].[Other].[Other]
  [PHS - Risk Factor].[Other Specify].[Other Specify]

  [PHS - Risk Factor].[Pregnancy].[Pregnancy]

  [PHS - Risk Factor].[Sickle Cell Disease].[Sickle Cell Disease]
   )",

  `UDF` =
    r"(

  [Measures].[Case Count]

  [Case - IDs].[Case ID].[Case ID]
  [Case - IDs].[Investigation ID].[Investigation ID]
  [Case - Disease].[Case Disease].[Case Disease]

  [PHS - UDF - Immunization].[Immunization Status Prior To Onset].[Immunization Status Prior To Onset]

  [PHS - UDF - Pregnancy].[Pregnancy Outcome].[Pregnancy Outcome]
  [PHS - UDF - Pregnancy].[Pregnancy Was Infant Affected].[Pregnancy Was Infant Affected]

  [PHS - UDF - Hospitalization].[ER Visit].[ER Visit]

  [PHS - UDF - Hospitalization].[ER Visit Hospital Name].[ER Visit Hospital Name]

  [PHS - UDF - Hospitalization].[Admitted To Hospital].[Admitted To Hospital]

  [PHS - UDF - Hospitalization].[Name Of Hospital For Admission].[Name Of Hospital For Admission]

  [PHS - UDF - Hospitalization].[Admission Date].[Admission Date]
  [PHS - UDF - Hospitalization].[Admitted To ICU].[Admitted To ICU]

  [PHS - UDF - Hospitalization].[ICU Hospital Name].[ICU Hospital Name]

  [PHS - UDF - Hospitalization].[Surgery].[Surgery]
  [PHS - UDF - Predisposing Conditions].[Predisposing Condition Chickenpox].[Predisposing Condition Chickenpox]
  [PHS - UDF - Predisposing Conditions].[Predisposing Condition Skin Infection].[Predisposing Condition Skin Infection]
  [PHS - UDF - Predisposing Conditions].[Predisposing Condition Wound].[Predisposing Condition Wound]
  [PHS - UDF - Predisposing Conditions].[Wound Type].[Wound Type]

  [PHS - UDF - Exposures].[Contact With Known Case].[Contact With Known Case]

  [PHS - UDF - Exposures].[Did Case Travel During Incubation Period].[Did Case Travel During Incubation Period]
  [PHS - UDF - Exposures].[Where Case Travelled During Incubation Period].[Where Case Travelled During Incubation Period]

  [PHS - UDF - Exposures].[Travel Locations During Incubation Period].[Travel Locations During Incubation Period]
  [PHS - UDF - Exposures].[Source Of Infection].[Source Of Infection]

  [PHS - UDF - Exposures].[Hospital Associated Infection].[Hospital Associated Infection]
  [PHS - UDF - Exposures].[Hospital Associated Infection Specify].[Hospital Associated Infection Specify]

  [PHS - UDF - Exposures].[Previous Pregnancies].[Previous Pregnancies]

  [PHS - UDF - Settings].[Healthcare Worker].[Healthcare Worker]
  [PHS - UDF - Settings].[Setting Name Type Location].[Setting Name Type Location]

  [PHS - UDF - Settings].[Child Care School University].[Child Care School University]
  [PHS - UDF - Settings].[Lives In Communal Setting].[Lives In Communal Setting]

  [PHS - UDF - Contacts].[Infants Under One Year].[Infants Under One Year]
  [PHS - UDF - Contacts].[Pregnant Women Third Trimester].[Pregnant Women Third Trimester]
  [PHS - UDF - Contacts].[Household Contact].[Household Contact]
  [PHS - UDF - Contacts].[Family Daycare Contacts].[Family Daycare Contacts]

  [PHS - UDF - Travel Communicability Period].[Did Case Travel During Communicability].[Did Case Travel During Communicability]
  [PHS - UDF - Travel Communicability Period].[Where Case Travelled During Communicability].[Where Case Travelled During Communicability]

  [PHS - UDF - Notes].[General Comments].[General Comments]
   )",

  `UDF Long` =
    r"(

  [Measures].[Case Count]

  [Case - IDs].[Case ID].[Case ID]
  [Case - IDs].[Investigation ID].[Investigation ID]
  [Case - Disease].[Case Disease].[Case Disease]
  [PHS - UDF - All].[UDF All Key].[UDF All Key]

  [PHS - UDF - All].[UDF All Key].[Answer Row ID]
  [PHS - UDF - All].[UDF All Key].[Answer Value]
  [PHS - UDF - All].[UDF All Key].[Question Keyword Common]
  [PHS - UDF - All].[UDF All Key].[Question Sort ID]
  [PHS - UDF - All].[UDF All Key].[Section Name]
  [PHS - UDF - All].[UDF All Key].[Section Sort ID]
  [PHS - UDF - All].[UDF All Key].[Form Name]
  [PHS - UDF - All].[UDF All Key].[Form Template Version]
   )",

  `Immunizations` =
    r"(

  [Measures].[Case Count]

  [Case - IDs].[Case ID].[Case ID]
  [Case - IDs].[Investigation ID].[Investigation ID]
  [Case - Disease].[Case Disease].[Case Disease]
  [PHS - Imms - Investigation].[Immunization Id].[Immunization Id]
  [PHS - Imms - Investigation].[Administered Date].[Administered Date]
  [PHS - Imms - Investigation].[Administered Date Estimated Flag].[Administered Date Estimated Flag]
  [PHS - Imms - Age at Time of Dose].[Age Year].[Age Year]
  [PHS - Imms - Age at Time of Dose].[Age Month].[Age Month]
  [PHS - Imms - Age at Time of Dose].[Age Day].[Age Day]
  [PHS - Imms - Investigation].[Agent].[Agent]
  [PHS - Imms - Investigation].[Antigen].[Antigen]
  [PHS - Imms - Investigation].[Antigen Status].[Antigen Status]
  [PHS - Imms - Investigation].[Agent Dose Number].[Agent Dose Number]
  [PHS - Imms - Investigation].[Dose Status].[Dose Status]
  [PHS - Imms - Investigation].[Historical Flag].[Historical Flag]
  [PHS - Imms - Investigation].[Immunization Reason].[Immunization Reason]
  [PHS - Imms - Investigation].[Override Flag].[Override Flag]
  [PHS - Imms - Investigation].[Agent Revised Dose Number].[Agent Revised Dose Number]
  [PHS - Imms - Investigation].[Agent Dose Number Derived].[Agent Dose Number Derived]
  [PHS - Imms - Investigation].[Revised Dose Reason].[Revised Dose Reason]

  [PHS - Imms - Investigation].[Trade Name].[Trade Name]
   )",

  `Special Considerations` =
    r"(

	[Measures].[Case Count]

  [Case - IDs].[Case ID].[Case ID]
  [Case - IDs].[Investigation ID].[Investigation ID]
  [Case - Disease].[Case Disease].[Case Disease]
  [PHS - Imms - Special Consideration].[Antigen].[Antigen]
  [PHS - Imms - Special Consideration].[Created Date].[Created Date]
  [PHS - Imms - Special Consideration].[Special Consideration Type].[Special Consideration Type]
  [PHS - Imms - Special Consideration].[Special Consideration Reason].[Special Consideration Reason]
  [PHS - Imms - Special Consideration].[Special Consideration Reason Comment].[Special Consideration Reason Comment]
  [PHS - Imms - Special Consideration].[Effective From Date].[Effective From Date]
  [PHS - Imms - Special Consideration].[Effective To Date].[Effective To Date]
  [PHS - Imms - Special Consideration].[Source Evidence].[Source Evidence]
  [PHS - Imms - Special Consideration].[Disease Date].[Disease Date]
  [PHS - Imms - Special Consideration].[Accurate Disease Date Indicator].[Accurate Disease Date Indicator]
   )",

  `LIS Tests` =
    r"(

	[Measures].[Case Count]

  [Case - IDs].[Case ID].[Case ID]
  [Case - IDs].[Container ID].[Container ID]
  [Case - IDs].[Investigation ID].[Investigation ID]
  [LIS - IDs].[Episode ID].[Episode ID]
  [LIS - IDs].[Accession Number].[Accession Number]
  [LIS - Date - Collection].[Date].[Date]
  [LIS - Date - Result].[Date].[Date]
  [LIS - Rule Engine - Episode Status].[Episode Status].[Episode Status]
  [LIS - Infection Group].[Infection Group].[Infection Group]
  [LIS - Specimen].[Specimen Description].[Specimen Description]

  [LIS - Specimen].[Sterile Status].[Sterile Status]

  [LIS - Result - Organism].[Organisms].[Level 3].ALLMEMBERS
  [LIS - Test].[Order Code].[Order Code]
  [LIS - Test].[Test Code].[Test Code]
  [LIS - Special Request].[SREQ Description].[SREQ Description]

  [LIS - Bacterial Typing].[AOF].[AOF]
  [LIS - Bacterial Typing].[SOF].[SOF]
  [LIS - Bacterial Typing].[T Type].[T Type]

  [LIS - Bacterial Typing].[Biotype].[Biotype]

  [LIS - Bacterial Typing].[ET Type].[ET Type]
  [LIS - Bacterial Typing].[Sero-subtype].[Sero-subtype]

  [LIS - Result Attributes - Test Outcome].[Test Outcome].[Test Outcome]
  [LIS - Flag - Test Valid Status].[Test Valid Status].[Test Valid Status]
  [LIS - Rule Engine - Episode Status].[Rule Description].[Rule Description]
  [LIS - Flag - Proficiency Test].[Proficiency Test].[Proficiency Test]
  [LIS - Flag - Test Performed].[Test Performed].[Test Performed]
  [LIS - Result Attributes].[Organism Identified].[Organism Identified]
  [LIS - Result Attributes].[Result Full Description].[Result Full Description]

  [LIS - Result Attributes].[Diphtheria Toxin Result].[Diphtheria Toxin Result]
   )"
  ) %>%
  clean_mdx_strings(mart = 'VPD', cube = 'VPD') %>%
  ## check ----
  imap(
    ~ {

      if (.y == "Case Investigation") {

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(attr_hier, 'Case Serotype') &
                attr_hier == lvl_memb ~
                paste(
                  'group_b_strep',
                  'igas',
                  'ipd',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'Case - Age at Case Date') &
                str_detect(attr_hier, 'Age Day|NGBS') ~
                paste(
                  'group_b_strep',
                  'other_disease',
                  sep = '|'
                ),
              str_detect(attr_hier, 'IPD') ~
                'ipd',
              str_detect(attr_hier, 'Age Group Mumps') ~
                'mumps',
              str_detect(dim, 'Case - Age at Case Date') &
                str_detect(attr_hier, 'Age Group Pertussis') |
              str_detect(dim, 'PHS - Date - Paroxysmal Onset') &
                str_detect(attr_hier, 'Date') |
              str_detect(dim, 'Case - Discordant Status') &
                str_detect(attr_hier, 'Discordant Infection Group') ~
                'bordetella',
              str_detect(dim, 'Case - Status - LIS') ~
                paste(
                  'group_b_strep',
                  'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),

              str_detect(dim, 'Discordant Status') &
                str_detect(attr_hier, 'Discordant Classification|Agent 2') ~
                paste(
                  'group_b_strep',
                  'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(attr_hier, 'Last Outcome') ~
                paste(
                  'group_b_strep',
                  'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Date - Last Outcome') &
                str_detect(attr_hier, 'Date') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Investigation Last Outcome') &
                str_detect(attr_hier, 'Cause of Death') ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Stage of Infection') &
                str_detect(attr_hier, 'Stage of Infection') ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Complication') &
                str_detect(attr_hier, 'Encephalitis|Meningitis') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Complication') &
                str_detect(attr_hier, 'Permanent Hearing Loss') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Complication') &
                str_detect(attr_hier, 'Pneumonia') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              .default = 'default'
            )
        )

      } else if (.y == "Symptoms") {

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Arthralgia$') ~
                'measles',
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, '(Arthritis|Cellulitis)$') ~
                'igas',
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Bacteremia$') ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Conjunctivitis$') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  # 'mumps',
                  'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Coryza$') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Cough$') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  # 'mumps',
                  # 'rubella',
                  'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier,
                           '^Cough.*(Apnea|Whoop|Gagging|2 Weeks|Paroxysmal)') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Fever$') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Lymphadenopathy$') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  # 'mumps',
                  'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier,
                           'Lymphadenopathy.*(Occipital|Post - Auricular|Cervical)$') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Meningitis$') ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Necrotizing Fasciiti') ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Orchitis$') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Other$') ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Parotitis (Bi|Uni)lateral$') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Peri - Partum Fever$') ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Pneumonia$') ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Purpura Fulminans') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Rash Maculopapular$') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  # 'mumps',
                  'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Rash Petechial$') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Symptom') &
                str_detect(attr_hier, 'Toxic Shock Syndrome$') ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),

              .default = 'default'
            )
        )

      } else if (.y == "Risk Factors") {

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(dim, 'PHS - Risk Factor') &
                str_detect(attr_hier,
                           paste(
                             '(Alcohol Use',
                             'Homelessness Underhoused',
                             'Injection Drug Use)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Risk Factor') &
                str_detect(attr_hier,
                           paste(
                             '(Chronic Cardiac Condition',
                             'Chronic Respiratory Pulmonary Condition',
                             'Diabetes Mellitus)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Risk Factor') &
                str_detect(attr_hier,
                           paste(
                             '(Chronic CSF Leak',
                             'Chronic Liver Disease',
                             'Chronic Liver Disease Specify',
                             'Chronic Renal Disease',
                             'Malignancies Cancer',
                             'Sickle Cell Disease',
                             'Cystic Fibrosis)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Risk Factor') &
                str_detect(attr_hier,
                           paste(
                             '(Immunocompromised Other',
                             'Immunocompromised Other Specify',
                             'Immunocompromised Transplant',
                             'Immunocompromised Transplant Specify',
                             'Immunocompromised Treatment',
                             'Immunocompromised Treatment Specify',
                             'Immunocompromising Condition',
                             'Immunocompromising Condition Specify)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Risk Factor') &
                str_detect(attr_hier,
                           paste(
                             '(MSM)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Risk Factor') &
                str_detect(attr_hier,
                           paste(
                             '(Other',
                             'Other Specify)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  # 'rubella',
                  'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - Risk Factor') &
                str_detect(attr_hier,
                           paste(
                             '(Pregnancy)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),

              .default = 'default'
            )
        )

      } else if (.y == "UDF") {

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(dim, 'PHS - UDF - Immunization') &
                str_detect(attr_hier, 'Immunization Status Prior To Onset') ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Pregnancy') &
                str_detect(attr_hier,
                           paste(
                             '(Pregnancy Outcome',
                             'Pregnancy Was Infant Affected)$',
                             sep = '|'
                           )) |
              str_detect(dim, 'PHS - UDF - Hospitalization') &
                str_detect(attr_hier,
                           paste(
                             'Surgery',
                             sep = '|'
                           )) |
              str_detect(dim, 'PHS - UDF - Exposures') &
                str_detect(attr_hier,
                           paste(
                             '(Hospital Associated Infection',
                             'Hospital Associated Infection Specify)$',
                             sep = '|'
                           )) |
              str_detect(dim, 'PHS - UDF - Predisposing Conditions') &
                str_detect(attr_hier,
                           paste(
                             '(Predisposing Condition Chickenpox',
                             'Predisposing Condition Skin Infection',
                             'Predisposing Condition Wound',
                             'Wound Type)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Hospitalization') &
                str_detect(attr_hier,
                           paste(
                             'ER Visit$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  # 'mumps',
                  'rubella',
                  'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Hospitalization') &
                str_detect(attr_hier,
                           paste(
                             'ER Visit Hospital Name$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  # 'mumps',
                  # 'rubella',
                  'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Hospitalization') &
                str_detect(attr_hier,
                           paste(
                             'Admitted To Hospital$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Hospitalization') &
                str_detect(attr_hier,
                           paste(
                             'Name Of Hospital For Admission$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  'mumps',
                  # 'rubella',
                  'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Hospitalization') &
                str_detect(attr_hier,
                           paste(
                             '(Admission Date',
                             'Admitted To ICU)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Hospitalization') &
                str_detect(attr_hier,
                           paste(
                             'ICU Hospital Name$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  'mumps',
                  # 'rubella',
                  'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Exposures') &
                str_detect(attr_hier,
                           paste(
                             'Contact With Known Case$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  # 'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Exposures') &
                str_detect(attr_hier,
                           paste(
                             '(Did Case Travel During Incubation Period',
                             'Where Case Travelled During Incubation Period)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  # 'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Exposures') &
                str_detect(attr_hier,
                           paste(
                             '(Travel Locations During Incubation Period',
                             'Source Of Infection)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Exposures') &
                str_detect(attr_hier,
                           paste(
                             '(Previous Pregnancies)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Settings') &
                str_detect(attr_hier,
                           paste(
                             '(Healthcare Worker',
                             'Setting Name Type Location)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Settings') &
                str_detect(attr_hier,
                           paste(
                             '(Child Care School University',
                             'Lives In Communal Setting)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  # 'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  # 'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Contacts') &
                str_detect(attr_hier,
                           paste(
                             '(Infants Under One Year',
                             'Pregnant Women Third Trimester',
                             'Household Contact',
                             'Family Daycare Contacts)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Travel Communicability Period') &
                str_detect(attr_hier,
                           paste(
                             '(Did Case Travel During Communicability',
                             'Where Case Travelled During Communicability)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'PHS - UDF - Notes') &
                str_detect(attr_hier,
                           paste(
                             '(General Comments)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),

              .default = 'default'
            )
        )

      } else if (.y == "Immunizations") {

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(dim, 'PHS - Imms - Investigation') &
                str_detect(attr_hier,
                           paste(
                             'Trade Name$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),

              .default = 'default'
            )
        )

      } else if (.y == "LIS Tests") {

        mutate(
          .x,
          .after = field_type,
          check =
            case_when(
              str_detect(dim, 'LIS - Specimen') &
                str_detect(attr_hier,
                           paste(
                             'Sterile Status$',
                             sep = '|'
                           )) ~
                paste(
                  'group_b_strep',
                  'igas',
                  'ipd',
                  # 'measles',
                  # 'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'LIS - Result - Organism') &
                str_detect(attr_hier,
                           paste(
                             'Organisms',
                             sep = '|'
                           )) |
                str_detect(dim, 'LIS - Test') &
                str_detect(attr_hier,
                           paste(
                             '(Order Code',
                             'Test Code)$',
                             sep = '|'
                           )) |
                str_detect(dim, 'LIS - Special Request') &
                str_detect(attr_hier,
                           paste(
                             'SREQ Description$',
                             sep = '|'
                           )) ~
                paste(
                  'group_b_strep',
                  'igas',
                  'ipd',
                  'measles',
                  'mumps',
                  'rubella',
                  'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'LIS - Bacterial Typing') &
                str_detect(attr_hier,
                           paste(
                             '(AOF|SOF|T Type)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'LIS - Bacterial Typing') &
                str_detect(attr_hier,
                           paste(
                             '(Biotype)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'LIS - Bacterial Typing') &
                str_detect(attr_hier,
                           paste(
                             '(ET Type|Sero-subtype)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  # 'rubella',
                  # 'bordetella',
                  'meningo',
                  'reportable_vpd',
                  sep = '|'
                ),
              str_detect(dim, 'LIS - Result Attributes') &
                str_detect(attr_hier,
                           paste(
                             '(Diphtheria Toxin Result)$',
                             sep = '|'
                           )) ~
                paste(
                  # 'group_b_strep',
                  # 'igas',
                  # 'ipd',
                  # 'measles',
                  # 'mumps',
                  'rubella',
                  # 'bordetella',
                  # 'meningo',
                  # 'reportable_vpd',
                  sep = '|'
                ),

              .default = 'default'
            )
        )

      } else {

        mutate(
          .x,
          .after = field_type,
          check = 'default'
        )

      }

    }
  ) %>%
  map(
    ~ bind_rows(
      .x,
      tribble(
        ~ field_type,  ~ check,   ~ dim,
        'filter_d',    'none',    'Case - Disease',
        'filter_d',    'none',    'Case - Classification - PHS',
        'filter_d',    'none',    'Case - Geo - Surveillance Region',
        'filter_d',    'none',    'Case - Status - LIS',
        'filter_r',    'none',    'Case - Date',
        'filter_d',    'default', 'Case - Classification - PHS',
        'filter_d',    'default', 'Case - Classification - PHS',
        'filter_d',    'default', 'Patient - Match Level',
      ) %>%
        bind_cols(
          tribble(
            ~ attr_hier,                   ~ param_name,
            'Case Disease',                'disease',
            'Case Classification',         'classification',
            'Case Surveillance Region HA', 'surveillance_region_ha',
            'Case Status LIS',             'lis_status',
            'Date',                        'query_date',
            'Case Classification Group',   NA_character_,
            'Case Classification Group',   NA_character_,
            'Match Level Description',     NA_character_,
          )
        ) %>%
        bind_cols(
          tribble(
            ~ all_memb,
            NA_character_,
            NA_character_,
            NA_character_,
            NA_character_,
            NA_character_,
            'Case',
            'No PHS Data',
            '3',
          )
        ) %>%
        mutate(lvl_memb = attr_hier)
    ) %>%
      fill(c(mart, cube, dataset_name))
  )

mdx_query_info <-
  list(
    list_cdi_mdx,
    list_respiratory_mdx,
    list_stibbi_mdx,
    list_enteric_mdx,
    list_vpd_mdx
  ) %>%
  reduce(append) %>%
  map(mutate, check = map(check, str_split_1, '\\|')) %>%
  map(unnest_longer, check) %>%
  map(distinct) %>%
  bind_rows() %>%
  drop_na(dim)

  )

usethis::use_data(
  overwrite = T, internal = T,
  servers,
  filter_rules,
  available_prebuilt_datasets,
  # sql_query_info,
  # mdx_query_info,
  list_query_info,
  col_name_dict,
  df_olap_map
)

