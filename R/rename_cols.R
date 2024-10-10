#' Renames data retrieved from Data cubes.
#'
#' @param obj A `data.frame` or character vector of names.
#'
#' @return Same as input `obj`.
#'
#' @noRd
#'
rename_cols <- function(obj) {

  if (is.data.frame(obj)) list_names <- names(obj) else list_names <- obj

  list_new_names <-
    list_names %>%
    stringr::str_split('\\]\\.\\[' ) %>% # in case there's "." in the names
    purrr::map(~ stringr::str_remove_all(.x, '\\[|\\]')) %>%
    purrr::map(~ stringr::str_subset(.x, 'MEMBER_CAPTION', negate = T)) %>%
    purrr::map(tibble::as_tibble) %>%
    purrr::map(
      ~ dplyr::bind_cols(
        .x,
        tibble::tibble(name = c('dim', 'hier', 'lvl', 'memb')[1:nrow(.x)])
      )
    ) %>%
    purrr::map(
      ~ tidyr::pivot_wider(.x, names_from = name, values_from = value)
    ) %>%
    dplyr::bind_rows() %>%
    { if (length(.) == 3) dplyr::mutate(., memb = NA_character_) else . } %>%
    dplyr::mutate(
      lvl = dplyr::case_when(!is.na(memb) ~ memb, TRUE ~ lvl)
    ) %>%
    # dplyr::mutate(
    #   new_name =
    #     dplyr::case_match(
    #       lvl,
    #       !!!(col_name_dict %>%
    #         purrr::pmap(
    #           function(old_name, new_name) {
    #
    #             paste(
    #               paste0("'", old_name, "'"),
    #               '~',
    #               paste0("'", new_name, "'")
    #             ) %>% as.formula
    #
    #           }
    #         )
    #       ),
    #       .default = lvl
    #     )
    # ) %>%
    dplyr::mutate(
      new_name =
        dplyr::if_else(
          stringr::str_detect(lvl, 'Hospital Code|Location Type'),
          paste(
            stringr::str_extract(dim, 'Patient|Result|Order Entry'),
            lvl
          ),
          lvl
        ),
    ) %>%
    dplyr::mutate(
      new_name =
        dplyr::case_when(
          stringr::str_detect(dim, 'LIS - Flag')      ~ dim,
          stringr::str_detect(dim, '- Copy To')       ~ paste(dim, lvl),
          dim  == 'Measures'                          ~ hier,
          hier == 'Date'                              ~ dim,
          hier == 'Yes No'                            ~ dim,
          lvl  == 'Interval in Days'                  ~ dim,
          lvl  == 'Interval Group 1'                  ~ paste(dim, lvl),
          TRUE                                        ~ new_name
        ) %>%
        janitor::make_clean_names()
    ) %>%
    dplyr::pull(new_name)

  if (is.data.frame(obj)) {

    names(obj) <- list_new_names
    return(obj)

  } else { return(list_new_names) }

}
