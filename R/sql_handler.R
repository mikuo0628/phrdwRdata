#' Helper function for handling and building SQL queries.
#'
#' @description
#' Takes arguments from [phrdwRdata::get_phrdw_data()] and processes with
#' `phrdwRdata:::list_query_info$sql` leverging `dbplyr` and NSE to produce
#' SQL query accordingly.
#'
#' @return Depending on user input, `tibble`, SQL string, or print dataset
#'   metadata.
#'
#' @noRd
#'
sql_handler <- function() {

  schema <-
    ifelse(
      any(stringr::str_detect(names(Sys.getenv()), '^WVD')),
      '',
      'phs_cd'
    )

  checks <-
    list(
      default = T,
      user    = T,
      `indigenous_id` = include_indigenous_identifiers,
      `patient_id`    = include_patient_identifiers
    ) %>%
    purrr::keep(isTRUE) %>%
    names

  # .query_info at this point should be ready to go without needing to filter
  # for dataset_name or anything
  .query_info <-
    .query_info %>%
    dplyr::filter(.data$check %in% checks)

  if (isTRUE(.check_params)) {

    n_pad <- floor(max(nchar(.query_info$col)) * 1.15)

    cat('===== The following fields are included in this dataset:\n\n')
    subset(.query_info, sql_func == 'select') %>%
      dplyr::select(col, as) %>%
      purrr::pwalk(
        function(col, as) {

          if (!is.na(as)) {

            cat(
              stringr::str_pad(paste('  -', col), n_pad, 'right', '.'),
              'renamed as',
              paste0('[', as, ']'),
              '\n'
            )

          } else { cat('  -', col, '\n') }

        }
      )

    cat('\n\n')

    cat('===== The following fields are default filters:\n\n')
    .query_info %>%
      dplyr::filter(stringr::str_detect(sql_func, 'where')) %>%
      dplyr::select(col, as) %>%
      purrr::pwalk(
        function(col, as) {

          if (!is.na(as)) {

            cat(
              stringr::str_pad(paste('  -', col), n_pad, 'right', '.'),
              'renamed as',
              paste0('[', as, ']'),
              '\n'
            )

          } else { cat('  -', col, '\n') }

        }
      )

    cat('\n')

    if (all(isFALSE(.return_data), isFALSE(.return_query))) return()

  }

  withr::with_db_connection(
    list(conn = connect_to_phrdw(mart = mart, type = type)),
    {

      dfs_views <-
        .query_info %>%
        dplyr::select(tidyselect::matches('^(alias|view|col|as)$')) %>%
        dplyr::distinct() %>%
        dplyr::group_by(alias, view) %>%
        dplyr::summarise(
          dplyr::across(c(col, as), ~ list(.x)),
          .groups = 'drop'
        ) %>%
        dplyr::mutate(
          .keep = 'none',
          alias, view,
          col, as
        ) %>%
        {

          rlang::set_names(
            purrr::pmap(
              .,
              function(alias, view, col, as) {

                # for each view used, select the columns listed in
                # list_query_info
                dplyr::tbl(conn, dbplyr::in_schema(schema, view)) %>%
                  dplyr::select(
                    tidyselect::all_of(
                      col
                      # rlang::set_names(col, as) # rename cols
                    )
                  ) %>%
                  dplyr::distinct()

              }

            ),
            .$alias
          )

        }

      if (is.character(.check_params)) {

        cat(paste0('===== Levels for `', .check_params, '` are:\n\n'))
        .query_info %>%
          dplyr::filter(
            .data$col == .check_params | .data$as == .check_params
          ) %>%
          dplyr::mutate(
            .keep = 'none',
            alias, col = dplyr::coalesce(as, col)
          ) %>%
          dplyr::distinct() %>%
          dplyr::group_by(alias) %>%
          dplyr::summarise(col = list(col)) %>%
          purrr::pmap(
            function(alias, col) {

              dfs_views[[alias]] %>%
                dplyr::select(
                  tidyselect::matches(paste0('^', col, '$'))
                )

            }
          ) %>%
          purrr::map(~ dplyr::count(.x, !!rlang::sym(colnames(.x)))) %>%
          purrr::map(dplyr::pull, 1) %>%
          unlist() %>%
          unique() %>%
          sort(na.last = T) %>%
          paste('  -', ., collapse = '\n') %>%
          cat
        cat('\n\n')

        if (all(isFALSE(.return_data), isFALSE(.return_query))) return()

      }

      # WHEREs for each lzy_df before join
      ## Discrete
      filters <-
        purrr::map2(
          list(param_name = default_params, col = user_params),
          list(
            subset(.query_info, sql_func == 'where')$param_name,
            subset(.query_info, sql_func == 'select')$col
          ),
          ~ purrr::keep_at(.x, .y)
        ) %>%
        purrr::map(purrr::discard, is.null)

      filters %>%
        purrr::map(names) %>%
        purrr::imap_dfr(
          ~ .query_info %>%
            dplyr::filter(
              !!rlang::sym(.y) %in% .x |
                .data$sql_func == 'where' & .data$check == 'default'
            )
        ) %>%
        dplyr::select(
          logic, order, alias, view, col, default_val, param_name
        ) %>%
        {

          if (!all(purrr::map_lgl(filters, ~ length(.x) == 0))) {

            dplyr::left_join(
              .,
              filters %>%
                purrr::imap_dfr(
                  ~ {

                    purrr::imap_dfr(
                      .x,
                      ~ tibble::tibble(col = .y, user_val = .x)
                    )

                  }
                )
            ) %>%
              dplyr::mutate(
                .keep = 'none',
                logic, order, alias, view, col,
                use_val = dplyr::coalesce(default_val, user_val)
              )

          } else {

            dplyr::mutate(
              .,
              .keep = 'none',
              logic, order, alias, view, col,
              use_val = default_val
            ) %>%
              dplyr::arrange(order, col)

          }

        } %>%
        dplyr::distinct() %>%
        dplyr::group_by(
          !!!rlang::syms(stringr::str_subset(names(.), 'val$', negate = T))
        ) %>%
        dplyr::summarise(val = list(use_val)) %>%
        split(.$alias) %>%
        purrr::walk(
          ~ {

            df_filter <- tidyr::replace_na(.x, list(logic = 'and'))

            filter_clause <-
              df_filter %>%
              dplyr::group_by(order) %>%
              purrr::pmap(
                function(logic, order, alias, view, col, val) {

                  paste(
                    col,
                    '%in%',
                    paste0(
                      "c(",
                      paste0("'", val, "'", collapse = ', '),
                      ")"
                    )
                  )

                }
              ) %>%
              tibble::tibble(expr = .) %>%
              dplyr::bind_cols(df_filter, .) %>%
              dplyr::group_by(order, alias) %>%
              dplyr::summarise(
                .groups = 'drop',
                expr = list(expr),
                logic = list(logic)
              ) %>%

              # process by sub order first
              purrr::pmap(
                function(order, alias, expr, logic) {

                  logic[1] <- ''
                  paste(
                    purrr::modify(
                      logic,
                      function(x) {

                        if (!x %in% c('and', 'or')) return('')
                        dplyr::if_else(x == 'and', '&', '|')

                      }
                    ),
                    expr
                  ) %>%
                    stringr::str_trim() %>%
                    paste(collapse = ' ') %>%
                    { if (length(expr) > 1) paste0('(', ., ')') else { . } }

                }
              ) %>%

              # handle by order
              purrr::reduce2(
                .x = .,
                .y =
                  df_filter %>%
                  dplyr::group_by(order) %>%
                  dplyr::slice(1) %>%
                  dplyr::pull(logic) %>%
                  .[-1],
                .f = function(clause_1, clause_2, operator) {

                  paste(
                    clause_1, clause_2,
                    sep = ifelse(operator == 'and', ' &\n  ', ' |\n  ')
                  )

                }
              )


            dfs_views[[unique(df_filter$alias)]] <<-
              dfs_views[[unique(df_filter$alias)]] %>%
              dplyr::filter(!!rlang::parse_expr(filter_clause))
              # dplyr::select(
              #   tidyselect::all_of(
              #     .query_info %>%
              #       dplyr::filter(.data$alias == df_filter$alias) %>%
              #       dplyr::select(col, as) %>%
              #       distinct %>%
              #       purrr::reduce(rlang::set_names)
              #   )
              # )

          }
        )

      ## Date
      purrr::pwalk(
        subset(.query_info, param_name == 'query_date', select = c(alias, col)),
        function(alias, col) {

          filter_date <-
            default_params %>%
            purrr::keep(stringr::str_detect(names(.), 'query_')) %>%
            purrr::discard(is.null) %>%
            purrr::map(lubridate::ymd, quiet = T) %>%
            purrr::map(~ format(.x, "'%Y-%m-%d'")) %>%
            purrr::imap_chr(
              ~ paste(
                ifelse(stringr::str_detect(.y, 'start'), '>=', '<='),
                .x
              )
            )

          if (length(filter_date) == 0) return(NULL)

          filter_date <-
            filter_date %>%
            paste(col, .) %>%
            purrr::map(rlang::parse_expr)

          dfs_views[[alias]] <<-
            dfs_views[[alias]] %>% dplyr::filter(!!!filter_date)

        }
      )

      # JOINs
      df_join <-
        .query_info %>%
        dplyr::filter(stringr::str_detect(.data$sql_func, 'join|select')) %>%
        dplyr::select(
          sql_func, order, tidyselect::matches('^(alias|view|logic|col|as)$')
        ) %>%
        # clean .query_info the proper renaming of columns is consistent
        # for cols across various sql_funcs
        dplyr::group_by(alias, view, col) %>%
        dplyr::mutate(as = stats::na.omit(as)[1]) %>%
        dplyr::ungroup() %>%
        {

          curr_query_info <- .

          join_keys <-
            # handles the new col names
            dplyr::mutate(curr_query_info, col = dplyr::coalesce(as, col)) %>%
            dplyr::filter(stringr::str_detect(.data$sql_func, 'join')) %>%
            dplyr::group_by(sql_func, order, logic) %>%
            dplyr::summarise(
              .groups = 'drop',
              col   = list(col),
              alias = list(alias)
            ) %>%
            dplyr::arrange(order) %>%
            dplyr::mutate(
              col =
                purrr::map2(
                  col, stringr::str_pad(logic, 4, side = 'both', pad = ' '),
                  ~ paste(
                      .x[c(T, F)],
                      .x[c(F, T)],
                      sep = .y
                  )
                )
            ) %>%
            dplyr::mutate(col = purrr::map(col, purrr::map, rlang::parse_expr))

          # rename the columns accordingly
          # this prepares it for joining and addresses the following scenarios:
          # 1. a renamed column is used as key
          # 2. a renamed column is used as key and also kept by SELECT
          dfs_views <-
            dfs_views %>%
            purrr::imap(
              ~ dplyr::select(
                .x,
                tidyselect::all_of(
                  do.call(
                    rlang::set_names,
                    unname(
                      as.list(
                        subset(
                          curr_query_info,
                          alias == .y,
                          c(col, as)
                        )
                      )
                    )
                  )
                )
              )
            )

          df_temp <-
            purrr::reduce2(
              .init = dfs_views[[purrr::map_chr(join_keys$alias, 1)[1]]],
              .x    = dfs_views[ purrr::map_chr(join_keys$alias, 2) ],
              .y    = join_keys$order,
              .f    = function(cur_df, to_join_df, row_n) {

                .join_by <- dplyr::join_by(!!!join_keys$col[[row_n]])

                # default dplyr/dbplyr joins do not keep keys (for good reasons),
                # and some keys are needed/SELECT.
                # the `keep` param in join functions will determine whether
                # keys are kept. For keys that are part of SELECT: T... this is
                # done by checking to see if it's needed in `SELECT` clause or
                # if it does not already exists in cur_df
                # however, this is hard to capture if a row of join_key contains
                # multiple OR...
                cols_to_check <- unique(unlist(.join_by[c('x', 'y')]))
                select_cols   <- subset(.query_info, sql_func == 'select')$col
                .keep         <-
                  if (
                    all(
                      any(cols_to_check %in% select_cols),
                      any(!cols_to_check %in% colnames(cur_df))
                    )
                  ) T

                try_join <-
                  try(
                    do.call(
                      what =
                        getFromNamespace(join_keys$sql_func[row_n], 'dplyr'),
                      args =
                        append(
                          list(cur_df, to_join_df),
                          list(
                            by   = .join_by,
                            keep = .keep
                          )
                        )
                    )
                  )

                if (inherits(try_join, 'try-error')) browser()

                if (any(stringr::str_detect(colnames(try_join), '\\.x$'))) {

                  # if key is kept, and happens to duplicately named as
                  # another column joined or selected upstream, then
                  # the prior is kept and the latter is discarded
                  warning(
                    do.call(
                      sprintf,
                      list(
                        paste(
                          "The `%s` has created naming conflict for column(s):\n",
                          "%s"
                        ),
                        toupper(gsub('_', ' ', join_keys[row_n, 'sql_func'][[1]]))
                      ) %>%
                        append(
                          paste(
                            '  -',
                            stringr::str_subset(colnames(try_join), '\\.(x|y)') %>%
                              gsub('\\.(x|y)', '', .) %>%
                              unique()
                          )
                        )
                    )
                  )

                  try_join <-
                    try_join %>%
                    dplyr::select(
                      -tidyselect::matches('\\.y$')
                    ) %>%
                    dplyr::rename_with(
                      .cols = tidyselect::matches('\\.x$'),
                      .fn   = \(x) gsub('\\.x$', '', x)
                    )

                }

                return(try_join)

              }
            )

          # SELECTs (and rename with AS if needed)
          # TODO: is this necessary?
          df_temp %>%
            dplyr::select(
              tidyselect::all_of(
                dplyr::mutate(
                  subset(curr_query_info, sql_func == 'select'),
                  .keep = 'none',
                  col = dplyr::coalesce(as, col)
                )$col
              )
            )

        }

      if (is.numeric(.head)) df_join <- df_join %>% head(round(.head))

      if (isTRUE(.return_query)) {

        sql_opts <- dbplyr::sql_options(cte = .cte)
        return(dbplyr::sql_render(df_join, sql_options = sql_opts))

      }

      if (isFALSE(.return_data)) return()
      if (isTRUE(.return_data))  return(dplyr::collect(df_join, cte = .cte))

    }
  )

}
