#' Helper function to process dataset instructions into MDX query.
#'
#' Takes arguments in `get_phrdw_data` and returns MDX query to be executed
#' in `execute2D`.
#'
#' @return MDX query.
#'
olap_handler <- function() {

  mdx_query <-
    .query_info %>%
    dplyr::filter(
      tolower(.data$cube)         == tolower(.env$mart),
      tolower(.data$dataset_name) == tolower(.env$dataset_name),
    ) %>%
    {

      columns <- dplyr::filter(., .data$field_type == 'columns')$attr_hier
      rows    <-
        dplyr::filter(., .data$field_type == 'rows') %>%
        dplyr::summarise(.by = dim, rows = list(attr_hier)) %>%
        dplyr::mutate(
          dplyr::across(
            dplyr::where(is.list),
            ~ rlang::set_names(.x, dim)
          )
        ) %>%
        dplyr::pull(rows)

      if (isTRUE(.check_params)) {

        cat(
          paste(
            '===== The following (hierarchy) fields are',
            'included in this dataset:\n\n'
          )
        )

        purrr::iwalk(
          rows,
          ~ {

            cat(
              paste(stringr::str_pad('* Dimension:',  15, 'right', ' '),
                    .y, '\n'),
              paste(stringr::str_pad('** Hierarchy:', 15, 'right', ' '),
                    .x, '\n'),
              '\n',
              sep = ''
            )
            cat(paste(rep('-', 50), collapse = ''), '\n\n')

          }
        )

        filters <-
          dplyr::filter(
            .,
            stringr::str_detect(.data$field_type, 'filter')
          ) %>%
          dplyr::select(dim, attr_hier) %>%
          dplyr::group_by(dim) %>%
          dplyr::summarise(hier = list(attr_hier)) %>%
          purrr::pmap(function (dim, hier) rlang::set_names(hier, dim)) %>%
          unlist

        cat(
          '\n',
          paste(
            '===== The following fields (hierarchies)',
            'can take filters:\n\n'
          ),
          sep = ''
        )

        purrr::iwalk(
          filters,
          ~ {

            cat(
              paste(stringr::str_pad('* Dimension:',  15, 'right', ' '),
                    .y, '\n'),
              paste(stringr::str_pad('** Hierarchy:', 15, 'right', ' '),
                    .x, '\n'),
              '\n',
              sep = ''
            )
            cat(paste(rep('-', 50), collapse = ''), '\n\n')

          }
        )

      }

      # Discrete filters
      filters_discrete <-
        dplyr::filter(
          .,
          .data$field_type == 'filter_d',
          dplyr::if_any(
            c(.data$attr_hier, .data$param_name),
            ~ .x %in% c(names(default_params), names(user_params))
          )
        ) %>%
        {

          df_temp <- .

          #' In user_params and default_params, there can be some user
          #' inputs for filtering. This selects for them, match them up to
          #' respective dim-hierarchy by hierarchy names or arg names,
          #' rearranges into data frame of columns `dim` for dimension,
          #' `attr` for attribute hierarchy, and `memb` for the user input
          #' part to select for, formatted for mdx_builder.
          purrr::imap(
            c('attr_hier'  = 'user_params',
              'param_name' = 'default_params'),
            ~ tibble::enframe(
              get(.x), name = .y, value = 'memb'
            ) %>%
              dplyr::filter(
                purrr::map_lgl(memb, is.character)
              ) %>%
              tidyr::unnest(
                dplyr::where(is.list), ptype = as.character()
              ) %>%
              dplyr::mutate(
                dplyr::across(dplyr::everything(), as.character)
              ) %>%
              dplyr::filter(
                stringr::str_detect(
                  .data[[.y]],
                  stringr::regex('date\\b', ignore_case = T),
                  negate = T
                )
              )
          ) %>%
            purrr::imap(
              ~ dplyr::full_join(
                by = .y,
                df_temp,
                .x
              )
            ) %>%
            dplyr::bind_rows() %>%
            dplyr::select(dim, attr = attr_hier, memb) %>%
            tidyr::drop_na() %>%
            dplyr::distinct() %>%
            { if (nrow(.) == 0) NULL else . }

        }

      filter_date <-
        dplyr::filter(
          .,
          .data$field_type == 'filter_r',
          any(
            stringr::str_detect(
              .data$attr_hier,
              stringr::regex('date\\b', ignore_case = T)
            ),
            .data$param_name %in% c('query_date')
          )
        ) %>%
          dplyr::bind_cols(
            tibble::tibble(
              memb =
                list(
                  default_params$query_start_date,
                  default_params$query_end_date
                ) %>%
                purrr::map(lubridate::as_date) %>%
                purrr::map(format, '%Y-%m-%d') %>%
                purrr::modify(function(x) if (is.null(x)) 'null' else x) %>%
                unlist
            )
          ) %>%
          dplyr::select(dim, attr = attr_hier, memb) %>%
          { if (nrow(.) == 0) NULL else . }

      # TODO: other range filters

      mdx_build(
        cube_name = unique(.$cube),
        columns   = columns,
        rows      = rows,
        discrete  = filters_discrete,
        range     = filter_date
      )

    }

}

