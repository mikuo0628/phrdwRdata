# Run this to refresh sysdata.rda

require(tidyverse)

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

available_prebuilt_datasets <-
  list(
    'CDI'         = c('Vital Stats',
                      'Vital Stats CCD',
                      'Vital Stats CCD Dashboard'
                      ),

    'Respiratory' = c('LIS Tests',
                      'LIS Episodes'),

    'STIBBI'      = c('Case',
                      'Investigation',
                      'Client',
                      'PHS Body Site',
                      'PHS Client Risk Factor',
                      'PHS Investigation Risk Factor',
                      'PHS UDF Long',
                      'LIS Tests',
                      'LIS Test Providers',
                      'LIS Episodes',
                      'LIS POC'),

    'Enteric'     = c('case Investigation',
                      'Risk Factor',
                      'Symptom',
                      'UDF',
                      'LIS Data'),

    'VPD'         = c('Case Investigation',
                      'Symptoms',
                      'Symptoms Long',
                      'Risk Factors',
                      'UDF',
                      'UDF Long',
                      'Immunizations',
                      'Special Considerations',
                      'LIS Tests'),

    'CD'          = c('Investigation',
                      'Client',
                      'Risk Factor',
                      'Symptom',
                      'Observation',
                      'UDF',
                      'Lab',
                      'Transmission Events',
                      'Contacts',
                      'Outbreaks',
                      'TB Contacts',
                      'TB Investigation',
                      'TB Transmission Events',
                      'TB Client',
                      'TB TST Investigation',
                      'TB TST Client',
                      'TB Lab',
                      'Complication')
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
  bind_rows(.id = 'cube')

mdx_query_info <-
  tibble(
    cube = 'CDI',
    dataset_name = 'Vital Stats CCD Dashboard'
  ) %>%
  bind_cols(
    tribble(
      ~ field_type, ~ dim,                                           ~ attr_hier,
      'columns',    'Measures',                                      'VS Death Count',
      'rows',       'VS - ID',                                       'Unique ID',
      'rows',       'VS - Cause of Death - Underlying',              'UCD 5Char Code',
      'rows',       'VS - Cause of Death - Contributing',            'CCD 5Char Code',
      'rows',       'VS - Date - Death',                             'Date',
      'rows',       'CDI - Gender',                                  'Sex',
      'rows',       'VS - Age at Death',                             'Age Year',
      'rows',       'VS - Age at Death',                             'Age Group 07',
      'rows',       'VS - Age at Death',                             'Age Group Population',
      'rows',       'VS - Place of Injury Type',                     'Place of Injury Type',
      'rows',       'VS - Geo - Residential Location Region',        'VS Residential Location LHA',
      'rows',       'VS - Geo - Death Location Region',              'VS Death Location LHA',
      'rows',       'VS - Geo - Residential Location Census - 2011', 'VS Residential Dissemination Area',
      'rows',       'VS - Geo - Death Location Census - 2011',       'VS Death Location Dissemination Area',
      'filter_d',   'VS - Case of Death - Underlying',               'UCD 3Char Code',
      'filter_d',   'VS - Case of Death - Contributing',             'CCD 3Char Code',
      'filter_d',   'VS - Geo - Residential Location Region',        'VS Residential Location HA',
      'filter_d',   'VS - Geo - Death Location Region',              'VS Death Location HA',
      'filter_r',   'VS - Date - Death',                             'Date',
    )
  ) %>%
  full_join(mdx_query_info) %>%
  full_join(
    tribble(
      ~ field_type, ~ attr_hier,                  ~ param_name,
      'filter_d',   'UCD 3Char Code',             'ucd_3_char_code',
      'filter_d',   'CCD 3Char Code',             'ccd_3_char_code',
      'filter_d',   'VS Residential Location HA', 'residential_location_ha',
      'filter_d',   'VS Death Location HA',       'death_location_ha',
      'filter_r',   'Date',                       'query_date',
    )
  )

mdx_query_info <-
  tibble(
    cube = 'CDI',
    dataset_name = 'Vital Stats'
  ) %>%
  bind_cols(
    tribble(
      ~ field_type, ~ dim,                                           ~ attr_hier,
      'columns',    'Measures',                                      'VS Death Count',
      'rows',       'VS - ID',                                       'Unique ID',
      'rows',       'VS - Cause of Death - Underlying',              'UCD 5Char Code',
      'rows',       'VS - Date - Death',                             'Date',
      'rows',       'CDI - Gender',                                  'Sex',
      'rows',       'VS - Age at Death',                             'Age Year',
      'rows',       'VS - Age at Death',                             'Age Group 07',
      'rows',       'VS - Age at Death',                             'Age Group Population',
      'rows',       'VS - Place of Injury Type',                     'Place of Injury Type',
      'rows',       'VS - Geo - Residential Location Region',        'VS Residential Location LHA',
      'rows',       'VS - Geo - Death Location Region',              'VS Death Location LHA',
      'rows',       'VS - Geo - Residential Location Census - 2011', 'VS Residential Dissemination Area',
      'rows',       'VS - Geo - Death Location Census - 2011',       'VS Death Location Dissemination Area',
      'filter_d',   'VS - Case of Death - Underlying',               'UCD 3Char Code',
      'filter_d',   'VS - Case of Death - Contributing',             'CCD 3Char Code',
      'filter_d',   'VS - Geo - Residential Location Region',        'VS Residential Location HA',
      'filter_d',   'VS - Geo - Death Location Region',              'VS Death Location HA',
      'filter_r',   'VS - Date - Death',                             'Date',
    )
  ) %>%
  full_join(mdx_query_info) %>%
  full_join(
    tribble(
      ~ field_type, ~ attr_hier,                  ~ param_name,
      'filter_d',   'UCD 3Char Code',             'ucd_3_char_code',
      'filter_d',   'CCD 3Char Code',             'ccd_3_char_code',
      'filter_d',   'VS Residential Location HA', 'residential_location_ha',
      'filter_d',   'VS Death Location HA',       'death_location_ha',
      'filter_r',   'Date',                       'query_date',
    )
  )

mdx_query_info <-
  tibble(
    cube = 'CDI',
    dataset_name = 'Vital Stats CCD'
  ) %>%
  bind_cols(
    tribble(
      ~ field_type, ~ dim,                                           ~ attr_hier,
      'columns',    'Measures',                                      'VS Death Count',
      'rows',       'VS - Cause of Death - Contributing',            'CCD 5Char Code',
      'filter_d',   'VS - Case of Death - Underlying',               'UCD 3Char Code',
      'filter_d',   'VS - Case of Death - Contributing',             'CCD 3Char Code',
      'filter_d',   'VS - Geo - Residential Location Region',        'VS Residential Location HA',
      'filter_d',   'VS - Geo - Death Location Region',              'VS Death Location HA',
      'filter_r',   'VS - Date - Death',                             'Date',
    )
  ) %>%
  full_join(mdx_query_info) %>%
  full_join(
    tribble(
      ~ field_type, ~ attr_hier,                  ~ param_name,
      'filter_d',   'UCD 3Char Code',             'ucd_3_char_code',
      'filter_d',   'CCD 3Char Code',             'ccd_3_char_code',
      'filter_d',   'VS Residential Location HA', 'residential_location_ha',
      'filter_d',   'VS Death Location HA',       'death_location_ha',
      'filter_r',   'Date',                       'query_date',
    )
  )

tibble(
  cube = 'Respiratory',
  dataset_name = 'LIS Tests'
)
bind_cols(

  tribble(
    ~ field_type, ~ dim,                                       ~ attr_hier,
    'columns',    'Measures',                                  'LIS Test Count',
    'rows',       'Patient - Patient Master',                  'Patient Master Key',
    'rows',       'LIS - IDs',                                 'Test ID',
    'rows',       'LIS - IDs',                                 'Accession Number',
    'rows',       'LIS - IDs',                                 'Container ID',
    'rows',       'LIS - Episode IDs',                         'Episode ID',
    'rows',       'LIS - Patient',                             'Gender',
    'rows',       'LIS - Patient',                             'Species',
    'rows',       'LIS - Infection Group',                     'Infection Group',
    'rows',       'LIS - Date - Collection',                   'Date',
    'rows',       'LIS - Date - Receive',                      'Date',
    'rows',       'LIS - Date - Result',                       'Date',
    'rows',       'LIS - Age at Collection',                   'Age Years',
    'rows',       'LIS - Test',                                'Test Code',
    'rows',       'LIS - Order Item',                          'Order Code',
    'rows',       'LIS - Test',                                'Source System ID',
    'rows',       'LIS - Result - Test Outcome',               'Test Outcome',
    'rows',       'LIS - Result - Organism',                   'Organism Level 4',
    'rows',       'LIS - Geo - Patient Region',                'LIS Patient LHA',
    'rows',       'LIS - Geo - Patient Address',               'LIS Patient Postal Code',
    'rows',       'LIS - Geo - Ordering Provider Region ATOT', 'LIS Ordering Provider LHA',
    'rows',       'LIS - Geo - Ordering Provider Address ATOT','LIS Ordering Provider Postal Code',
    'rows',       'LIS - Lab Location - Result',               'Lab Name',
    'rows',       'LIS - Flag - Proficiency Test',             'Proficiency Test',
    'rows',       'LIS - Flag - Test Performed',               'Test Performed',
    'rows',       'LIS - Flag - Test Valid Status',            'Valid Test',
    'rows',       'LIS - Result Attributes',                   'Result Full Description',
  )

)

tribble(
  ~ field_type, ~ dim,        ~ attr_hier,
  'Columns',    'Measures',   'Case Count',
  'rows',       'Case - IDs', 'Investigation ID',
  'rows',       'Case - IDs', 'Investigation ID',
  'rows',       'Case - IDs', 'Investigation ID',
  'rows',       'Case - IDs', 'Investigation ID',
)


list_query_info <-
  list(
    sql = sql_query_info,
    olap = mdx_query_info
  )

col_name_dict <-
  tibble::tribble(
    ~ old_name,                  ~ new_name,
    "Hospital Code",             "patient_hospital_code",
    "Location Type",             "patient_location_type",
    "Lab Code",                  "order_entry_lab_code",
    "Hospital Code",             "order_entry_hospital_code",
    "Lab Name",                  "order_entry_lab_name",
    "Lab Location Code",         "order_entry_lab_location_code",
    "Lab Location Name",         "order_entry_lab_location_name",
    "Lab Location Description",  "order_entry_lab_location_descr",
    "Lab Code",                  "result_lab_code",
    "Hospital Code",             "result_hospital_code",
    "Lab Name",                  "result_lab_name",
    "Lab Location Code",         "result_lab_location_code",
    "Lab Location Name",         "result_lab_location_name",
    "Lab Location Description",  "result_lab_location_description",

  )

usethis::use_data(
  overwrite = T, internal = T,
  servers,
  filter_rules,
  available_prebuilt_datasets,
  # sql_query_info,
  # mdx_query_info,
  list_query_info,
  col_name_dict
)


