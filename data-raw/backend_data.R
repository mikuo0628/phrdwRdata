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
          )
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
          'VPD',         'VPD SU',         "SU_PHRDW_VPD",              'su'
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

usethis::use_data(
  overwrite = T, internal = T,
  servers,
  filter_rules
)


