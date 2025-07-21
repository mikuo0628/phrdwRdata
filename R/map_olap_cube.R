map_olap_cube <- function(conn, cube = NULL, is_visible = F) {

  require(phrdwRdata)
  require(tidyverse)
  require(janitor)

  if (is.null(cube)) cube <- explore(conn)

  dmvs <-
    c(
      # 'MDSCHEMA_ACTIONS',
      # 'MDSCHEMA_CUBES',
      'MDSCHEMA_DIMENSIONS',
      # 'MDSCHEMA_FUNCTIONS',
      'MDSCHEMA_HIERARCHIES',
      # 'MDSCHEMA_INPUT_DATASOURCES',
      # 'MDSCHEMA_KPIS',
      'MDSCHEMA_LEVELS',
      'MDSCHEMA_MEASUREGROUP_DIMENSIONS',
      'MDSCHEMA_MEASUREGROUPS',
      'MDSCHEMA_MEASURES',
      # 'MDSCHEMA_MEMBERS',
      'MDSCHEMA_PROPERTIES'
      # 'MDSCHEMA_SETS'
    )

  measuregroup_dims <-
    c(
      'CATALOG_NAME',
      'SCHEMA_NAME',
      'CUBE_NAME',
      'MEASUREGROUP_NAME',
      'MEASUREGROUP_CARDINALITY',
      'DIMENSION_UNIQUE_NAME',
      'DIMENSION_CARDINALITY',
      'DIMENSION_IS_VISIBLE',
      'DIMENSION_IS_FACT_DIMENSION',
      # 'DIMENSION_PATH',
      'DIMENSION_GRANULARITY'
    )

  dfs_dmvs <-
    map(
      rlang::set_names(dmvs),
      ~ {

        dmv <- .x

        purrr::map_dfr(
          cube,
          ~ stringr::str_glue(
            'SELECT',
            ifelse(
              stringr::str_detect(dmv, 'GROUP_DIMENSIONS'),
              paste('[', measuregroup_dims, ']', sep = '', collapse = ', '),
              '*'
            ),
            'FROM $System.{dmv}',
            "WHERE CUBE_NAME = '{.x}'",
            .sep = '\n'
          ) %>%
            phrdwRdata:::execute2D(conn, .) %>%
            tibble::as_tibble()
        )

      }
    )
  # map(
  #   rlang::set_names(dmvs),
  #   ~ str_glue(
  #     'SELECT',
  #     ifelse(
  #       str_detect(.x, 'GROUP_DIMENSIONS'),
  #       paste('[', measuregroup_dims, ']', sep = '', collapse = ', '),
  #       '*'
  #     ),
  #     'FROM $System.{.x}',
  #     "WHERE CUBE_NAME = '{cube}'",
  #     .sep = '\n'
  #   ) %>%
  #     phrdwRdata:::execute2D(conn, .) %>%
  #     as_tibble
  # )

  dfs_dmvs %>%
    { .[order(purrr::map_int(., nrow), decreasing = F)] } %>%
    purrr::map(janitor::remove_empty, 'cols') %>%
    purrr::map(janitor::clean_names) %>%
    purrr::map(
      dplyr::mutate,
      dplyr::across(tidyselect::where(is.numeric), as.character)
    ) %>%
    # map(mutate, across(matches('description|data_type'), ~ na_if(.x, ''))) %>%
    # map(remove_empty, 'cols') %>%
    purrr::map(dplyr::select,  !tidyselect::matches('description|data_type')) %>%
    purrr::map(
      dplyr::filter,
      dplyr::if_all(tidyselect::matches('is_visible'),        ~ .x != is_visible),
      dplyr::if_any(tidyselect::matches('^hierarchy_origin'), ~ .x != 1 | is.na(.x)),
      dplyr::if_any(tidyselect::matches('^property_origin'),  ~ .x != 1),
      dplyr::if_any(tidyselect::matches('^level_type'),       ~ .x != 1),
      # if_any(matches('^expression'),       ~ is.na(.x)),
      dplyr::if_any(tidyselect::matches('^dimension_type'),   ~ .x != 2),
      dplyr::if_all(tidyselect::matches('^level_name$'),      ~ !stringr::str_detect(.x, '\\(All\\)')),
    ) %>%
    purrr::map(dplyr::select, !tidyselect::matches('is_(virtual|readwrite|visible)')) %>%
    purrr::map(dplyr::select, !tidyselect::matches('dimension_(type|unique_setting)')) %>%
    purrr::map(dplyr::select, !tidyselect::matches('(all|default)_member')) %>%
    purrr::map(dplyr::distinct) %>%
    {

      dfs_temp <-
        purrr::map(., \(df) {

          remove_these_cols <-
            dplyr::select(df, tidyselect::matches('cardinality')) %>%
            purrr::map(\(col) any(stringr::str_detect(col, '[:alpha:]'))) %>%
            purrr::discard(isFALSE) %>%
            names()

          if (length(remove_these_cols) != 0) {

            df <-
              df %>% dplyr::select(!tidyselect::all_of(remove_these_cols))

          }

          return(df)

        })

      left_join(
        dfs_temp$MDSCHEMA_MEASURES,
        dfs_temp$MDSCHEMA_MEASUREGROUP_DIMENSIONS,
        relationship = 'many-to-many'
      ) %>%
        dplyr::select(-tidyselect::matches('cardinality')) %>%
        dplyr::left_join(
          dfs_temp$MDSCHEMA_DIMENSION,
        ) %>%
        dplyr::left_join(
          dfs_temp$MDSCHEMA_HIERARCHIES,
          relationship = 'many-to-many'
        ) %>%
        dplyr::left_join(
          dfs_temp$MDSCHEMA_LEVELS
        ) %>%
        tidyr::drop_na(measuregroup_name, dimension_name)

    }

}
