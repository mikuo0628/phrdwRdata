cd_investigation_query <- function(mart, type, param_list) {

  tables <-
    c(
      investigation          ='investigation',
      age                    ='age',
      classification         ='classification',
      client                 ='client',
      date                   ='date',
      etiologic_agent        ='etiologic_agent',
      encounter_group        ='encounter_group',
      gender                 ='gender',
      lha                    ='lha',
      surveillance_region    ='lha',
      investigation_outbreak ='investigation_outbreak',
      surveillance_condition ='surveillance_condition',
      investigation_status   ='investigation_status'
    )

  browser()
  withr::with_db_connection(
    list(conn = connect_to_phrdw(mart = mart, type = type)),
    {

      sql_params <- dplyr::filter(query_info, .data$dataset == tables[[1]])

      #' prevents filtering non-exisitng columns and expandable by
      #' allowing for additional user input that's not listed

      available_filters <-
        param_list[
          intersect(names(param_list), dplyr::pull(sql_params, field_name))
        ]

      browser()
      # TODO: extrapolate to function?
      dfs_lazy <-
        purrr::imap(
          tables,
          ~ {

            browser()
            fields <-
              dplyr::filter(
                sql_params,
                .data$table == .y,
                stringr::str_detect(.data$field_type, 'id$', negate = T)
              ) %>%
                dplyr::bind_rows(
                  dplyr::filter(
                    sql_params,
                    .data$table == .y,
                    stringr::str_detect(.data$field_type, 'id$'),
                    stringr::str_detect(
                      .data$field_type, '^id$',
                      negate = !param_list$include_patient_identifiers
                    ),
                    stringr::str_detect(
                      .data$field_type, '^abo_id$',
                      negate = !param_list$include_indigenous_identifiers
                    )
                  )
                )

            df_temp <-
              dplyr::tbl(
                conn, dbplyr::in_schema('phs_cd', paste0('vw_pan_', .x))
              ) %>%

              {

                # apply date filter on investigation.surveillance_date
                if (tolower(.x) == 'investigation') {

                  #' converts dates to proper SQL format and removes
                  #' bad/non entries
                  user_dates <-
                    param_list %>%
                    purrr::keep(stringr::str_detect(names(.), 'date')) %>%
                    purrr::map(lubridate::ymd, quiet = T) %>%
                    # see https://sqlblog.org/2009/10/16/
                    purrr::map(~ format(.x, '%Y%m%d')) %>%
                    purrr::discard(
                      .p = ~ any(
                        #' this should cover all cases of incorrect
                        #' user input dates
                        is.na(.x), is.null(.x), identical(.x, character())
                      )
                    )

                  if (user_dates[[2]] < user_dates[[1]]) {

                    stop('\n', 'Please check your query dates.', '\n', F)

                  }

                  # creates SQL filter query for date
                  date_filter_conditions <-
                    purrr::imap(
                      user_dates,
                      ~ {

                        paste(
                          "surveillance_date",
                          ifelse(stringr::str_detect(.y, 'start'), '>=', '<=') ,
                          paste0("'", .x, "'")
                        )

                      }
                    ) %>%
                    paste(collapse = ' AND ')

                  dplyr::filter(
                    .,
                    dbplyr::sql(date_filter_conditions)
                    # surveillance_date > local(user_dates[[1]])
                  )

                }

              } %>%

              dplyr::select(dplyr::all_of(unique(fields$field_name))) %>%

              {

                # apply available user input filters
                available_filters %>%
                  purrr::map(

                  )
                dplyr::sql()


              } %>%

              # rename cols according to queries.csv file
              dplyr::rename_with(
                .cols = everything(),
                .fn = function(name_vec) {

                  list_recode <-
                    dplyr::select(
                      dplyr::filter(
                        tidyr::drop_na(fields, field_newname),
                      ),
                      matches('name$')
                    ) %>%
                    {

                      stringr::str_glue(
                        "'{ .$field_name }'",
                        ' ~ ',
                        "'{ .$field_newname }'"
                      )

                    } %>%
                    purrr::map(as.formula)

                  if (identical(list_recode, list())) return(name_vec)

                  dplyr::case_match(
                    name_vec,
                    !!!list_recode,
                    .default = name_vec
                  )

                }
              )

          }
        )

      join_keys <-
        sql_params %>%
        dplyr::filter(.data$field_type == 'key') %>%
        dplyr::group_by(.data$join_order, .data$join_type) %>%
        dplyr::summarise(keys = list(setNames(field_name, table))) %>%
        dplyr::arrange(as.integer(join_order)) %>%
        dplyr::group_by(.data$join_type) %>%
        dplyr::summarise(keys = list(keys))

      df_final <- NULL

      purrr::pwalk(
        join_keys,
        function(join_type, keys) {

          df_final <<-
            purrr::reduce(
              .init = df_final,
              .x = keys,
              .f = function(accu_df, key) {

                if (is.null(accu_df)) accu_df <- dfs_lazy[[names(key)[1]]]

                df_joined <-
                  do.call(
                    what = getFromNamespace(join_type, 'dplyr'),
                    args =
                      list(
                        accu_df,
                        dfs_lazy[[names(key)[2]]],
                        by =
                          setNames(
                            nm     = key[[1]],
                            object = key[[2]]
                          ),
                        suffix = paste0('.', names(key))
                      )
                  )

                return(df_joined)

              }
            )

        }
      )

    }
  )

}
