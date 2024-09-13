#' Title
#'
#' @param obj A `data.frame` or character vector.
#' @param dict
#'
#' @return
#'
#' @examples
rename_cols <- function(
    obj,
    dict = col_name_dict
) {

  if (is.data.frame(obj)) list_names <- names(obj) else list_names <- obj

  list_new_names <-
    list_names %>%
    stringr::str_split('\\]\\.\\[' ) %>% # in case there's "." in the names
    purrr::map(~ stringr::str_remove_all(.x, '\\[|\\]')) %>%
    purrr::map(~ stringr::str_subset(.x, 'MEMBER_CAPTION', negate = T)) %>%
    purrr::map(unique) %>%
    purrr::map(~ rlang::set_names(.x, c('dim', 'hier'))) %>%
    dplyr::bind_rows() %>%
    dplyr::mutate(
      hier =
        dplyr::case_match(
          hier,
          !!!(col_name_dict %>%
            purrr::pmap(
              function(old_name, new_name) {

                paste(
                  paste0("'", old_name, "'"),
                  '~',
                  paste0("'", new_name, "'")
                ) %>% as.formula

              }
            )
          ),
          .default = hier
        )
    ) %>%
    dplyr::mutate(
      new_name =
        dplyr::case_when(
          hier == 'Date'     ~ dim,
          TRUE               ~ hier
        ) %>%
        janitor::make_clean_names()
    ) %>%
    dplyr::pull(new_name)

  if (is.data.frame(obj)) {

    names(obj) <- list_new_names
    return(obj)

  } else { return(list_new_names) }

}
