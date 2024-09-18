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
    'MDSCHEMA_LEVELS',
    'MDSCHEMA_MEASUREGROUP_DIMENSIONS',
    'MDSCHEMA_MEASUREGROUPS',
    'MDSCHEMA_MEASURES',
    'MDSCHEMA_PROPERTIES'
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
  discard_at('CD') %>%
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
  discard_at('MDSCHEMA_PROPERTIES') %>%
  map(
    ~ list(
      .x,
      dmv_outputs$MDSCHEMA_PROPERTIES
    ) %>%
      map(names) %>%
      reduce(intersect)
  )

df_olap_map <-
  dmv_outputs %>%
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
  map(distinct) %>%
  { .[order(map_int(., nrow), decreasing = F)] } %>%
  {

    list(
      .$MDSCHEMA_MEASURES,
      .$MDSCHEMA_MEASUREGROUPS,
      .$MDSCHEMA_MEASUREGROUP_DIMENSIONS,
      .$MDSCHEMA_DIMENSIONS,
      .$MDSCHEMA_HIERARCHIES,
      .$MDSCHEMA_LEVELS,
      .$MDSCHEMA_PROPERTIES
    ) %>%
      map(select, -matches('cardinality')) %>%
      reduce(full_join) %>%
      distinct %>%
      select(
        catalog   = catalog_name,
        cube      = cube_name,
        mea       = measure_name,
        dim       = dimension_name,
        hier_attr = hierarchy_name,
        lvl       = level_name,
        prop      = property_name
      ) %>%
      drop_na(dim) %>%
      drop_na(mea) %>%
      mutate(
        prop =
          case_when(
            str_detect(prop, 'KEY\\d|NAME|MEMBER') ~ NA,
            TRUE ~ prop
          )
      ) %>%
      distinct

  }

