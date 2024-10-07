# Run this to refresh sysdata.rda

require(tidyverse)
devtools::load_all()

refresh <- T

filter_rules <-
  readxl::read_excel(
    'data-raw/phrdwRdata_filter_list.xlsx',
    sheet = 'filters',
    col_names = T,
    guess_max = 2000
  )

if (refresh) source('U:/Code/R/packages/phrdwRdata/data-raw/build_info.R')

list_query_info <-
  list(
    sql =
      read_csv(
        'data-raw/sql_query_info.csv',
        col_types =
          cols(order = 'i')
      ),
    olap =
      read_csv(
        'data-raw/olap_query_info.csv',
      )
  )

usethis::use_data(
  overwrite = T, internal = T,
  servers,
  filter_rules,
  # available_prebuilt_datasets,
  # sql_query_info,
  # mdx_query_info,
  list_query_info,
  # col_name_dict,
  df_olap_map
)

