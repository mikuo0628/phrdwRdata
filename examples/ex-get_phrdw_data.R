\dontrun{

# The bare minimum required user inputs are `mart` and `dataset_name`.
# User input is simpler and not case-sensitive.
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation'
)

# Unlike the legacy approach, which would require more user input.
# User need to be wary of spelling and case.
# User also need to provide dates, even if the intent is to retrieve
# the entire dataset. Without date boundaries, unexpected output may return.
# Dates would need to go beyond what exist in the database, but user would not
# have known this info.
get_phrdw_data(
  phrdw_datamart_connection = connect_to_phrdw(phrdw_datamart = 'CD Mart'),
  phrdw_datamart = 'CD Mart',
  dataset_name = 'Investigation',
  query_start_date = '1900-01-01',
  query_end_date   = Sys.Date()
)

# Incorrect spelling will generate helpful message to assist user.
get_phrdw_data(
  mart = 'dc',
  dataset_name = 'investigation'
)

get_phrdw_data(
  mart = 'stibbi',
  dataset_name = 'investigations'
)

# Filter
## Some arguments are reserved for filters. These remain unchanged from before.
## They are case- and spelling-sensitive.
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation',
  disease = 'Anthrax'
)

get_phrdw_data(
  phrdw_datamart_connection = connect_to_phrdw(phrdw_datamart = 'CD Mart'),
  phrdw_datamart = 'CD Mart',
  dataset_name = 'Investigation',
  query_start_date = '1900-01-01',
  query_end_date   = Sys.Date(),
  disease = 'Anthrax'
)

get_phrdw_data(
  mart = 'stibbi',
  dataset_name = 'investigation',
  disease = 'Chlamydia'
)

get_phrdw_data(
  phrdw_datamart_connection = connect_to_phrdw(phrdw_datamart = 'STIBBI'),
  phrdw_datamart = 'STIBBI',
  dataset_name = 'Investigation',
  query_start_date = '1900-01-01',
  query_end_date   = Sys.Date(),
  disease = 'Chlamydia'
)

# Troubleshooting
## User can opt to return query instead of data.
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation',
  .return_query = T
)

# User can use `.check_params` to examine basic dataset metainfo such as
# column names and what's being used to filter by default.
# Note: the output format may look different depending on data source being
# RDBMS or OLAP (ie. relational DB or data cube).
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation',
  .check_params = T
)

get_phrdw_data(
  mart = 'stibbi',
  dataset_name = 'investigation',
  .check_params = T
)

# When `.check_params` is supplied with the name of the column (or
# hierarchy), it will return (nominal) levels.
# Use with caution: it can return dates and street addresses and that would be
# a long list.
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation',
  .check_params = 'source_system'
)

get_phrdw_data(
  mart = 'stibbi',
  dataset_name = 'investigation',
  .check_params = 'Source System'
)

# Customize filtering (only available in stable usage)
## Knowing columns and levels can be helpful, as these values can be passed
## into the function for customized filtering:
## 1. Enclose the column name with backticks.
## 2. Pass the value to filter.
get_phrdw_data(
  mart = 'cd',
  dataset_name = 'investigation',
  `source_system` = 'EMR'
)

get_phrdw_data(
  mart = 'stibbi',
  dataset_name = 'investigation',
  `Source System` = 'EMR'
)

}



