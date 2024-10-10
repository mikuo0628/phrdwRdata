#' Cast data types appropriately.
#'
#' @description
#' `r lifecycle::badge("experimental")`
#'
#' Instead of heuristically prase dates, it detects date string by preset
#' patterns and apply respective `lubrdate` functions accordingly.
#'
#' @param dataset A `data.frame` or `tibble` object.
#' @param date_col_pattern String pattern to detect column names. Common
#' patterns can be 'date', or 'dt_tm'. Defaults to 'date'.
#'
#' @return A `data.frame` or `tibble` object.
#'
datatype_cols <- function(
    dataset,
    date_col_pattern = 'date'
) {

  patterns <-
    c(
      ymd     = '\\d{4,4}.\\d{1,2}.\\d{1,2}',
      mdy     = '\\d{1,2}.\\d{1,2}.\\d{4,4}',
      mdy_hms = '\\d{1,2}.\\d{1,2}.\\d{4,4} \\d+:\\d+.*',
      ymd_hms = '\\d{4,4}.\\d{1,2}.\\d{1,2} \\d+:\\d+.*'
      # excel   = '\\d{5}'
    ) %>%
    purrr::map(~ paste0('^', .x, '$')) %>%
    purrr::map(stringr::str_replace_all, r'(\\)', r'(\\\\)')

  case_when_date_fmt <-
    purrr::imap_chr(
      patterns,
      ~ paste0(
        "stringr::str_detect(.x, '", .x, "') ~ ",
        dplyr::if_else(
          .y == 'excel',
          'janitor::excel_numeric_to_date(as.numeric(.x))',
          # sprintf("'%s'", .y)
          paste0(
            'lubridate::',
            .y,
            '(.x, quiet = T)'
          )
        )
      )
    )

  dataset %>%
    dplyr::mutate(
      # .keep = 'used',
      dplyr::across(
        dplyr::matches(date_col_pattern),
        ~ dplyr::case_when(
          !!!rlang::parse_exprs(case_when_date_fmt),
          .default = lubridate::as_date(.x),
          .ptype   = lubridate::as_date(.x),
        )
        # .names = '{col}_new'
      )
    )

}

