#' MDX Builder: builds the SELECT statement on `columns` and `rows`.
#'
#' @param columns Character vector of measures, with name of dimension as the
#'   name of the list. Name defaults to `Measures`.
#'
#' @param rows Accepts `data.frame` with columns `dim`, `attr_hier`, and
#'   `lvl_memb`, or character list of hierarchies, with name of dimension as
#'   the name of the list.
#'
#' @param dim_props Must be `data.frame` with columns `dim`, `attr_hier`, and
#'   `lvl_memb`.
#'
#' @param .head `r lifecycle::badge('experimental')`
#'
#'  Optional. Single integer vector to indicate how many rows from the top
#'  to return. Note: `tail` is not supported on database backends.
#'
#' @return A `sql`/`character` object.
#'
#' @examples
#' \dontrun{
#' mdx_select(
#'   'Case Count',
#'   set_names(
#'     list(
#'       c(
#'         "Age Group",
#'         "Age Group 04",
#'         "Age Group 05",
#'         "Age Group 09",
#'         "Age Group 10",
#'         "Age Group 11",
#'         "Age Group 17",
#'         "Age Group 20",
#'         "Age Group 24",
#'         "Age Years"
#'       )
#'     ),
#'     'Case - Age at Earliest Date'
#'   )
#' )
#' }
#'
#' @noRd
#'
mdx_select <- function(columns, rows, dim_props, .head = NULL) {

  if (is.null(names(columns))) columns <- rlang::set_names(columns, 'Measures')
  if (is.null(names(rows))) stop('Please provide dimension for rows as name.')

  if (inherits(rows, 'list')) {

    rows <- purrr::imap(rows, ~ stringr::str_glue('[{.y}].[{.x}].[{.x}]'))

  } else if (inherits(rows, 'data.frame')) {

    rows <-
      rows %>%
      purrr::pmap_chr(
        function(...) {

          dots <- rlang::list2(...)
          purrr::discard(dots, names(dots) == 'all_memb') %>%
            purrr::discard(is.na) %>%
            purrr::map(~ paste0('[', .x, ']')) %>%
            append(dots['all_memb']) %>%
            purrr::discard(is.na) %>%
            paste(collapse = '.')

        }
      )

  }

  dim_props <-
    if (nrow(dim_props) == 0) { NULL } else {

      dim_props %>%
        purrr::pmap(
          function(...) {

            dots <- rlang::list2(...)
            sprintf('[%s].[%s].[%s]', dots$dim, dots$attr_hier, dots$lvl_memb)

          }
        )

    }

  paste(
    'SELECT',
    paste(
      ' ',
      c(
        paste0(
          # i'm a little concern of removing NON EMPTY if .head is used
          # count == NA will be included
          if (is.null(.head)) 'NON EMPTY ' else NULL,
          '{'
        ),
        paste(
          '  ',
          unlist(purrr::imap(columns, ~ stringr::str_glue('[{.y}].[{.x}]'))),
          collapse = ',|'
        ) %>%
          stringr::str_split_1('\\|'),
        '} ON COLUMNS,'
      ),
      collapse = '\n'
    ),
    paste(
      ' ',
      c(
        paste(
          ifelse(is.null(.head), 'NON EMPTY', 'HEAD'),
          '('
        ),
        paste(
          '  ',
          c(
            '{',
            paste(
              '  ',
              unlist(rows),
              collapse = ' *|'
            ) %>%
              stringr::str_split_1('\\|'),
            '}'
          )
        ),
        ifelse(is.null(.head), '', paste(',', .head)),
        ')',

        if (!is.null(dim_props)) {

          paste(
            '  ',
            c(
              'DIMENSION PROPERTIES MEMBER_CAPTION',
              paste('  ', dim_props)
            ),
            collapse = ',\n'
          ) %>%
            stringr::str_split_1('\\n')

        } else { NULL },

        '\n  ON ROWS'
      ),
      collapse = '\n'
    ),
    sep = '\n\n'
  ) %>%
    dplyr::sql()

}

#' MDX Builder: process filters, discrete or range, by date or other data types.
#'
#' @param discrete A `data.frame` object with 3 columns: `dim`, `attr`, and
#'   `memb`, for "dimension", "attribute", and "member". The attribute must
#'   belong to dimension, and member must belong to the attribute hierarchy.
#'   Each member to filter for should have its own row in this `data.frame`.
#'
#' @param range A `data.frame` object with 3 columns: `dim`, `attr`, and
#'   `memb`, for "dimension", "attribute", and "member".
#'   The attribute must belong to dimension, and member must belong to the
#'   attribute hierarchy.

#'   Two rows must be provided here with two different member values as
#'   the `from` and `to`.
#'
#'   If no bounds, use `NULL` or "null".
#'
#' @param ... Reserved for future development.
#'
#' @param .as_lines Boolean value that if `TRUE` (default), returns a character
#'   vector of properly spaced MDX filter clauses. This is needed as input to
#'   `mdx_from` for formatting purposes. If `FALSE`, returns a single element
#'   character vector, for printing purposes.
#'
#' @return Character vector.
#'
#' @examples
#' \dontrun{
#' mdx_filter(
#'   discrete =
#'     tibble(
#'       dim = 'LIS - Test',
#'       attr = 'Test Code',
#'       memb = c('TPE1', 'RPR')
#'     ),
#'   range =
#'     tibble(
#'       dim = 'LIS - Date - Collection',
#'       attr = 'Date',
#'       memb = c('2019-01-01', '2019-02-02')
#'     ),
#'   .as_lines = T
#' )
#' }
#'
#' @noRd
#'
mdx_filter <- function(discrete  = NULL,
                       range     = NULL,
                       ...,
                       .as_lines = T) {

  dots <- rlang::list2(...)

  conditions <-
    purrr::imap(
      append(
        list(
          discrete = discrete,
          range    = range
        ),
        dots
      ) %>%
        purrr::discard(is.null) %>%
        purrr::map(~ split(.x, .x[1:2], drop = T, sep = '|')),
      ~ {

        dfs_temp <- .x

        conds <-
          purrr::map(
            dfs_temp,
            ~ purrr::pmap_chr(
              .x,
              function(dim, attr, memb) {

                if (is.null(memb) | tolower(memb) == 'null') return('null')

                if (
                  stringr::str_detect(
                    attr, stringr::regex('Date', ignore_case = T)
                  )
                ) {

                  memb <-
                    format(
                      lubridate::as_datetime(memb), '%Y-%m-%dT%H:%M:%S'
                    )

                }


                stringr::str_glue('[{dim}].[{attr}].&[{memb}]')

              }

            )
          )

        if (.y == 'discrete') {

          conds <-
            purrr::imap(
              conds,
              ~ paste0(.x, collapse = ',|')
            ) %>%
            purrr::map(stringr::str_split_1, '\\|')

        }

        if (.y == 'range') {

          if (any(purrr::map_int(conds, length) != 2)) {

            stop('Incorrect range bounds.')

          }

          conds <-
            purrr::imap(
              conds,
              ~ paste0(.x, collapse = ' :|')
            ) %>%
            purrr::map(stringr::str_split_1, '\\|')

        }

        conds

      }
    ) %>%
    purrr::imap(
      ~ {

        list_cond <- .x
        type      <- .y

        purrr::map(
          list_cond,
          ~ paste(
            '{',
            paste(
              '  ', .x, collapse = '\n'
            ),
            '}',
            sep = '\n'
          )
        )

      }
    ) %>%
    unlist %>%
    unname %>%
    paste(collapse = ' *\n\n')

  if (.as_lines) return(conditions %>% stringr::str_split_1('\n'))

  return(conditions)

}

#' MDX Builder: builds the `FROM` clause, and incorporate any filters if needed.
#'
#' @param cube_name Cube name.
#'
#' @param ... Character vector of lines, which makes up the filter query
#'   build from `mdx_filters`.
#'
#' @return A `sql`/`character` object.
#'
#' @examples
#' \dontrun{
#' mdx_from(
#'   'StibbiDM',
#'   mdx_filter(
#'     discrete =
#'       tibble(
#'         dim = 'LIS - Test',
#'         attr = 'Test Code',
#'         memb = c('TPE1', 'RPR')
#'       ),
#'     range =
#'       tibble(
#'         dim = 'LIS - Date - Collection',
#'         attr = 'Date',
#'         memb = c('2019-01-01', '2019-02-02')
#'       ),
#'     .as_lines = T
#'   )
#' )
#' }
#'
#' @noRd
#'
mdx_from <- function(cube_name, ...) {

  dots <- rlang::list2(...)

  from_cube <- paste0('FROM [', cube_name, ']')

  if (identical(dots, list()) | identical(unlist(dots), '')) return(from_cube)

  conds <-
    c(
      '(',
      paste('  ', dots[[1]]),
      ') ON COLUMNS'
    )

  select_conds <-
    c(
      'SELECT',
      paste('  ', conds),
      '',
      from_cube
    )

  from_final <-
    c(
      'FROM (',
      paste('  ', select_conds),
      ')'
    ) %>%
    paste(collapse = '\n')

  return(dplyr::sql(from_final))

}


#' MDX builder: takes all MDX functions and build query.
#'
#' @inheritParams mdx_select
#' @inheritParams mdx_from
#' @inheritParams mdx_filter
#' @inherit mdx_select return
#'
#' @examples
#' \dontrun{
#' mdx_build(
#'   cube_name = 'StibbiDM',
#'   columns = 'Case Count',
#'   rows =
#'     set_names(
#'       list(
#'         c(
#'           "Age Group 10",
#'           "Age Group 24",
#'           "Age Years"
#'         )
#'       ),
#'       'Case - Age at Earliest Date'
#'     ),
#'   discrete =
#'     tibble(
#'       dim = 'LIS - Test',
#'       attr = 'Test Code',
#'       memb = c('TPE1', 'RPR')
#'     ),
#'   range =
#'     tibble(
#'       dim = 'LIS - Date - Collection',
#'       attr = 'Date',
#'       memb = c('2019-01-01', '2019-02-02')
#'     )
#' )
#' }
#'
#' @noRd
#'
mdx_build <- function(
    cube_name,
    columns,
    rows,
    dim_props,
    .head,
    discrete = NULL,
    range    = NULL
) {

  paste(
    mdx_select(columns, rows, dim_props, .head),
    mdx_from(
      cube_name,
      mdx_filter(
        discrete = discrete, range = range
      )
    ),
    sep = '\n\n'
  ) %>%
    dplyr::sql()

}

