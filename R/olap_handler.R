#' Helper function for handling and building MDX queries.
#'
#' @description
#' Takes arguments from [phrdwRdata::get_phrdw_data()] and processes with
#' `phrdwRdata:::list_query_info$olap` to produce MDX query accordingly
#'
#' @return Depending on user input, `tibble`, MDX string, or print dataset
#'   metadata.
#'
#' @noRd
#'
olap_handler <- function() {

  list_check_values <-
    list(
      # stibbi
      hiv             = c('Human immunodeficiency virus (HIV) infection'),
      aids            = c('Acquired immunodeficiency syndrome (AIDS)'),
      syphilis        = c('Syphilis','Syphilis (congenital)'),
      hcv             = c('Hepatitis C'),
      chlamydia       = c('Chlamydia'),
      gonorrhea       = c('Gonorrhea'),
      lymphogranuloma = c('Lymphogranuloma venereum'),

      # vpd
      measles         = c('Measles'),
      mumps           = c('Mumps'),
      bordetella      = c('Purtussis', 'Holmesii', 'Parapertussis'),
      group_b_strep   = c('Streptococcal disease (group B)'),
      igas            = c('Streptococcal disease (invasive group A - iGAS)'),
      ipd             = c('Pneumococcal disease (invasive)'),
      meningo         = c('Meningococcal disease (invasive)',
                          'Meningococcal disease (non-invasive)'),
      rubella         = c('Rubella', 'Rubella (congenital)'),
      reportable_vpd  = c('Diphtheria',
                          'Haemophilus influenza (invasive disease)',
                          'Tetanus'),
      other_disease   = c('Diphtheria',
                          'Haemophilus influenzae (invasive disease)',
                          'Tetanus',
                          'Rubella',
                          'Rubella (congenital)',
                          'Meningococcal disease (invasive)',
                          'Meningococcal disease (non-invasive)',
                          'Pneumococcal disease (invasive)',
                          'Streptococcal disease (invasive group A - iGAS)',
                          'Streptococcal disease (group B)',
                          'Pertussis','Holmesii','Parapertussis',
                          'Mumps',
                          'Measles')
    )

  # TODO: what if multiple user disease input?
  checks <-
    append(
      list(
        default         = T,
        `indigenous_id` = include_indigenous_identifiers,
        `patient_id`    = include_patient_identifiers,
        `system_ids`    = any(
          stringr::str_detect(
            retrieve_system_ids,
            stringr::regex('yes', ignore_case = T)
          ),
          isTRUE(retrieve_system_ids)
        )
      ),
      purrr::map_lgl(list_check_values, ~ any(disease %in% .x))
    ) %>%
    purrr::keep(isTRUE) %>%
    names

  # TODO: handle default dim_props and default filters (Enterics)
  mdx_query <-
    .query_info %>%
    dplyr::filter(
      tolower(.data$mart)         == tolower(.env$mart),
      tolower(.data$dataset_name) == tolower(.env$dataset_name),
    ) %>%
    dplyr::group_by(field_type) %>%
    dplyr::distinct(dim, attr_hier, lvl_memb, all_memb, .keep_all = T) %>%
    dplyr::ungroup() %>%
    {

      # measure(s)
      columns <- dplyr::filter(., .data$field_type == 'columns')$attr_hier

      # dimension(s)
      rows    <-
        dplyr::filter(., .data$field_type == 'rows') %>%
        dplyr::filter(.data$check %in% checks) %>%
        dplyr::select(dim, attr_hier, lvl_memb, all_memb)

      # dimension properties member caption
      # basically, a dimension listed above, but all the properties/members
      # to include
      dim_props <-
        dplyr::filter(., .data$field_type == 'dim_prop') %>%
        dplyr::filter(., .data$check %in% checks) %>%
        dplyr::select(dim, attr_hier, lvl_memb)

      if (isTRUE(.check_params)) {

        cat(
          paste(
            '===== The following (hierarchy) fields are',
            'included in this dataset:\n\n'
          )
        )

        rows %>%
          dplyr::group_by(dim) %>%
          dplyr::summarise(hier = list(attr_hier)) %>%
          purrr::pwalk(
            function(...) {

              dots <- rlang::list2(...)

              cat(
                paste(stringr::str_pad('* Dimension:',  15, 'right', ' '),
                      dots$dim, '\n'),
                paste(stringr::str_pad('** Hierarchy:', 15, 'right', ' '),
                      dots$hier, '\n'),
                '\n',
                sep = ''
              )

              cat(paste(rep('-', 50), collapse = ''), '\n\n')

            }
          )

        cat(
          '\n',
          paste(
            '===== The following fields (hierarchies)',
            'are default filters:\n\n'
          ),
          sep = ''
        )

        dplyr::filter(
          .,
          stringr::str_detect(.data$field_type, 'filter')
        ) %>%
          dplyr::select(dim, attr_hier, param_name) %>%
          purrr::pwalk(
            function(...) {

              dots <- rlang::list2(...)

              cat(
                paste(stringr::str_pad('* Dimension:',  15, 'right', ' '),
                      dots$dim, '\n'),
                paste(stringr::str_pad('** Hierarchy:', 15, 'right', ' '),
                      dots$attr_hier, '\n'),
                paste(stringr::str_pad('** Arg name:', 15, 'right', ' '),
                      paste0('`', dots$param_name, '`'), '\n'),
                '\n',
                sep = ''
              )

              cat(paste(rep('-', 50), collapse = ''), '\n\n')

            }
          )

      } else if (is.character(.check_params)) {

        cubes <- explore(connect_to_phrdw(mart = mart, type = type))

        dim <-
          dplyr::filter(
            .query_info,
            tolower(.data$mart) == .env$mart,
            .data$attr_hier     == .check_params
          )$dim %>%
          unique

        levels <-
          purrr::imap(
            rlang::set_names(cubes),
            ~ try(
              explore(
                connect_to_phrdw(mart = mart, type = type),
                .x,
                dim,
                .check_params,
                .check_params
              ),
              silent = T
            )
          ) %>%
          purrr::discard(~ inherits(.x, 'try-error'))

        purrr::iwalk(
          levels,
          ~ cat(
            '',
            paste(stringr::str_pad('Cube:',      13, 'right', ' '), .y),
            paste(stringr::str_pad('Dimension:', 13, 'right', ' '), dim),
            paste(stringr::str_pad('Hierarchy:', 13, 'right', ' '), .check_params),
            '',
            stringr::str_pad('Levels:',    13, 'right', ' '),
            paste('-', .x, collapse = '\n'),
            sep = '\n'
          )
        )

      }

      # Discrete filters
        filters_discrete <-
        dplyr::filter(
          .,
          .data$field_type == 'filter_d' |
            dplyr::if_any(
              c(.data$attr_hier, .data$param_name),
              ~ .x %in% c(names(default_params), names(user_params))
            )
        ) %>%
        {

          df_temp <- .

          # In user_params and default_params, there can be some user
          # inputs for filtering. This selects for them, match them up to
          # respective dim-hierarchy by hierarchy names or arg names,
          # rearranges into data frame of columns `dim` for dimension,
          # `attr` for attribute hierarchy, and `memb` for the user input
          # part to select for, formatted for mdx_builder.
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
                tidyselect::where(is.list), ptype = as.character()
              ) %>%
              dplyr::mutate(
                dplyr::across(tidyselect::everything(), as.character)
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
            dplyr::mutate(memb = dplyr::coalesce(all_memb, memb)) %>%
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
        dim_props = dim_props,
        .head     = .head,
        discrete  = filters_discrete,
        range     = filter_date
      )

    }

  if (isTRUE(.return_query)) return(mdx_query)
  if (isFALSE(.return_data)) return()

  df_query <-
    execute2D(connect_to_phrdw(mart = mart, type = type), mdx_query) %>%
    tibble::as_tibble()

  if (isTRUE(.return_data)) return(df_query)

}

