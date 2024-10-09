
# get_phrdw_data ----------------------------------------------------------

test_that(
  "get_phrdw_data() returns correct nrows regardless current or legacy",
  {

    list_datasets <-
      list_query_info$olap %>%
      dplyr::select(mart, dataset_name) %>%
      dplyr::distinct() %>%
      dplyr::group_by(mart) %>%
      dplyr::reframe(dataset_name = list(dataset_name)) %>%
      { rlang::set_names(.$dataset_name, .$mart) } %>%
      purrr::map(stringr::str_subset, 'Dashboard', negate = T)




  }
)
