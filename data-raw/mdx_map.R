############################################################################## #
#' Purpose: Mapping out cube structure and relationships
#' AUthor:  Michael Kuo
#' R ver:   4.3
############################################################################## #



# Workspace setup ---------------------------------------------------------

sapply(
  c(
    'tidyverse',
    'magrittr',
    'lubridate',
    'janitor',
    'phrdwRdata'
  ),
  require,
  character.only = T
)



# Parse for cube structures -----------------------------------------------

dmvs <-
  c(
    'MDSCHEMA_DIMENSIONS',
    'MDSCHEMA_HIERARCHIES',
    # 'MDSCHEMA_LEVELS',
    'MDSCHEMA_MEASUREGROUP_DIMENSIONS',
    'MDSCHEMA_MEASUREGROUPS',
    'MDSCHEMA_MEASURES'
    # 'MDSCHEMA_PROPERTIES'
  )

mg_dim <-
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

marts <-
  available_prebuilt_datasets %>%
  discard_at('cd') %>%
  names

system.time(
  dmv_outputs <-
    map(
      rlang::set_names(dmvs),
      ~ {

        dmv <- .x
        cat('Processing DMV:', dmv, '\n')

        map_dfr(
          rlang::set_names(marts),
          ~ {

            mart <- tolower(.x)
            conn <- connect_to_phrdw(mart = mart)
            cube_names <- explore(conn)

            cat('Processing mart:', mart, '\n')

            map_dfr(
              rlang::set_names(cube_names),
              ~ {

                cube_name <- .x
                cat('Processing cube:', cube_name, '\n')

                query <-
                  str_glue(
                    'SELECT',
                    ifelse(
                      str_detect(dmv, 'GROUP_DIMEN'),
                      paste('[', mg_dim, ']', sep = '', collapse = ', '),
                      '*'
                    ),
                    'FROM $System.{dmv}',
                    "WHERE CUBE_NAME = '{cube_name}'",
                    .sep = '\n'
                  ) %>%
                  sql()

                try(as_tibble(execute2D(conn, query)), silent = T)

              }
            )

          }
        )

      }
    )
)



# Clean up ----------------------------------------------------------------

dmv_outputs %>%
  { .[order(map_int(., nrow), decreasing = F)] } %>%
  map(select, !matches('UNIQUE')) %>%
  map(remove_empty, 'cols') %>%
  map(clean_names) %>%
  map(mutate, across(where(is.numeric), as.character)) %>%
  map(select, -matches('description|data_type')) %>%
  map(
    filter,
    if_all(matches('is_visible'),        ~ .x != F),
    if_any(matches('^hierarchy_origin'), ~ .x != 1 | is.na(.x)),
    if_any(matches('^property_origin'),  ~ .x != 1),
    if_any(matches('^level_type'),       ~ .x != 1),
    if_any(matches('^expression'),       ~ is.na(.x)),
    if_any(matches('^dimension_type'),   ~ .x != 2),
    if_any(matches('^level_name$'),      ~ !str_detect(.x, '\\(All\\)'))
  ) %>%
  map(select, -matches('is_(virtual|readwrite|visible)')) %>%
  map(select, -matches('dimension_(type|unique_setting)')) %>%
  map(select, -matches('all|default)_member')) %>%
  map(distinct) %>%
  {

    dfs_temp <- .
    browser()

    left_join(
      dfs_temp$MDSCHEMA_MEASURES,
      dfs_temp$MDSCHEMA_MEASUREGROUP_DIMENSIONS,
      relationship = 'many-to-many'
    ) %>%
      select(-matches('cardinality')) %>%
      left_join(dfs_temp$MDSCHEMA_DIMENSION, relationship = 'many-to-many') %>%
      left_join(dfs_temp$MDSCHEMA_HIERARCHIES, relationship = 'many-to-many')

    list(
      dfs_temp$MDSCHEMA_DIMENSIONS,
      dfs_temp$MDSCHEMA_HIERARCHIES,
      dfs_temp$MDSCHEMA_MEASUREGROUP_DIMENSIONS
    )

  }


