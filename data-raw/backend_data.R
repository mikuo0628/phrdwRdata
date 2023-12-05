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
          ~ mart,           ~ initial_catalog,            ~ type,
          'CDI',            "CDI",                        'prod',
          'Enteric',        "PHRDW_Enteric_Panorama",     'prod',
          'Enteric',        "SU_PHRDW_Enteric_Panorama",  'su',
          'Respiratory',    "PHRDW_Respiratory",          'prod',
          'STIBBI',         "PHRDW_STIBBI",               'prod',
          'STIBBI',         "SU_PHRDW_STIBBI",            'su',
          'STIBBI',         "SA_PHRDW_STIBBI",            'sa',
          'VPD',            "PHRDW_VPD",                  'prod',
          'VPD',            "SU_PHRDW_VPD",               'su'
        ),
        data_source = 'SPRSASBI001.phsabc.ehcnet.ca\\PRISASBIM',
        provider    = 'MSOLAP',
        packet_size = '32767'
      )
  )


usethis::use_data(
  overwrite = T, internal = T,
  servers
)

