#' Retrieve full mapping of views and columns using *sys.schema*.
#'
#' @param conn An `odbc` connection object. Defaults to `NULL`.
#'   If `NULL`, will connect to `CD mart`.
#' @param catalog Catalog name if known. Default to `NULL`.
#' @param schema
#' @param tbl_vw_dependencies
#' @param include_datatype If `TRUE`, will make an additional join with `types`
#'   in **sys** schema. Defaults to `FALSE`.
#'
#' @return A named list: `map` is a `data.frame` of SQL schema, view, and
#'   columns; `dep` is `data.frame` of dependencies.
#'
#' @examples
map_sql_view <-
  function(
    conn    = NULL,
    catalog = NULL,
    schema  = NULL,
    include_datatype = F,
    tbl_vw_dependencies = F
  ) {

    require(withr)
    require(magrittr)
    require(dplyr)
    require(odbc)
    require(janitor)
    require(tidyr)
    require(purrr)

    if (is.null(conn)) {

      conn <-
        withr::local_db_connection(con = connect_to_phrdw(mart = 'cd'))

    }

    dfs_sql_meta <-
      c(
        'schemas',
        # 'tables',
        'types',
        # 'indexes',
        # 'partitions',
        # 'allocation_units',
        # 'sql_modules',
        'views',
        'columns'
      ) %>%
      rlang::set_names() %>%
      purrr::map(
        ~ odbc::dbGetQuery(
          conn,
          sprintf('SELECT * FROM sys.%s', .x)
        ) %>%
          tibble::as_tibble() %>%
          janitor::clean_names()
      ) %>%
      purrr::imap(
        ~ dplyr::rename_with(
          .x,
          .cols = tidyselect::matches('^(name|type)$|date'),
          .fn   =
            function(names, type = .y) {

              type <-
                ifelse(
                  type == 'indexes', 'index', stringr::str_sub(type, 1, -2)
                )

              if (rlang::is_empty(names)) return(names)

              return(paste0(type, '_', names))

            }
        )
      )

    dfs_sql_meta <-
      dfs_sql_meta %>%
      purrr::imap(
        ~ dplyr::select(
          .x,
          tidyselect::matches('object_id'),
          tidyselect::matches('^(schema|table|view|column|type)_.*(date|id|name)'),
          tidyselect::matches('^(max_length|precision)$'),
          tidyselect::matches('user_type_id'),
          tidyselect::matches('parent|correlation|encryption|default|rule'),
          tidyselect::matches('system_type_id|type_name'),
        )
      )

    list_output <-
      list(
        map =
          dplyr::inner_join(
            dfs_sql_meta$views,
            dfs_sql_meta$columns,
            by = 'object_id'
          ) %>%
          dplyr::left_join(dfs_sql_meta$schemas, by = 'schema_id') %>%
          dplyr::select(
            tidyselect::matches('schema'),
            tidyselect::matches('name$'),
            tidyselect::matches('date$'),
            tidyselect::matches('id$'),
          )

      )

    if (include_datatype) {

      list_output <-
        list_output %>%
        purrr::map(
          ~ dplyr::left_join(
            .x,
            dplyr::select(
              dfs_sql_meta$types,
              system_type_id, type_name
            ),
            by = 'system_type_id',
            relationship = 'many-to-many'
          )
        )

    }

    if (tbl_vw_dependencies) {

      df_dep <-
        odbc::dbGetQuery(
          conn,
          sprintf('SELECT * FROM INFORMATION_SCHEMA.VIEW_TABLE_USAGE')
        ) %>%
        tibble::as_tibble() %>%
        janitor::clean_names()

      list_output <-
        list_output %>%
        append(list(dep = df_dep))

    }

    if (!is.null(schema)) {

      list_output$map <- dplyr::filter(list_output$map, schema_name == schema)

    }

    list_output$map <- dplyr::select(list_output$map, -tidyselect::matches('id$'))

    return(list_output)

  }
