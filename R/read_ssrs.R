#' Retrieves data from SSRS URLs.
#'
#' @description
#' SQL Server Reporting Services (SSRS) built by PHSA enables an alternative
#'   way for users to retrieve public health data containing identifiers.
#'
#' @details
#' This function provides an interface to pull data into R environment by
#'   leveraging the following packages/tools:
#'   -  `keyring`: handles user credential elegantly.
#'   -  `httr2`: handles HTTP requests and responses following Microsoft
#'      documentation on REST APIs for Reporting Services
#'      (\url{https://learn.microsoft.com/en-us/sql/reporting-services/developer/rest-api?view=sql-server-ver16}).
#'
#' There are some helper parameters to assist users with the report's built-in
#'   filters and output formats. However, how the report is set up may be very
#'   different from one to another. Please always double check to ensure what
#'   you get is what you intended.
#'
#' The helper to determine filters is `.explore`. If you set it to `TRUE`,
#'   you may get something similar to the following message printed in your
#'   console:
#'
#' ```
#'   Default User Input:
#'
#'     health_authority: No input detected; possibly checkbox?
#'     death_date_from : 1/1/2015
#'     death_date_to   : 5/20/2025
#'
#' ```
#'
#' To use filters in this function, simply refer to what's printed above, and
#'   add them as part of the function:
#'
#' ```r
#' read_ssrs(
#'   url             = YOUR_SSRS_URL,
#'   death_date_from = '1/1/2015',
#'   death_date_to   = '5/20/2025'
#' )
#' ```
#'
#' Your user credential is managed by `keyring` package. This prevents you
#'   from entering your credentials in the console or saving it in the script,
#'   which are both not ideal practices for security.
#'
#' `keyring` will leverage your operating system's credential manager to
#'   handle your saved credential.
#'
#' @param url SSRS url.
#' @param ... SSRS reports' built-in filters. See `Details`.
#' @param username User ID (without email domain). See `Details`.
#' @param format
#'
#'   Some SSRS reports offer multiple formats to download. Currently only
#'   csv is supported and is the default value. May be extended in the future.
#'
#' @param .explore
#'
#'   If you are unsure what filters you could use for your SSRS report,
#'   set this to `TRUE` and a list of currently supported filters and their
#'   default values will be printed in the console.
#'
#' @param .skip
#'
#'   SSRS reports in csv format may contain lines above the headers
#'   (meta info, descriptions, etc). You may not wish to have this in your
#'   data frame. If in your first run you noted there are lines above the
#'   headers, you can enter number of lines to skip here.
#'
#' @returns `tibble` object.
#' @export
#'
read_ssrs <- function(
    url = '',
    ...,
    username,
    format = c('CSV')[1],
    .explore = F,
    .skip = 0
) {

  if (!exists('read_ssrs_skip_warning', envir = the)) {

    the$read_ssrs_skip_warning <- Sys.time()
    message(
      paste(
        sep = '\n',
        "SSRS reports may contain extraneous lines at the top.",
        "If noted, supply integer to `.skip` to skip those lines.",
        '\n',
        "This warning message will appear only once per session."
      )
    )

  }

  user_params <- rlang::list2(...)

  format <- sprintf('?rs:Format=%s', format)

  ua <-
    paste(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
      'AppleWebKit/537.36 (KHTML, like Gecko)',
      'Chrome/133.0.0.0 Safari/537.36'
    )

  url_mod <-
    sub(
      x = url,
      pattern = 'reports/report',
      replacement = 'ReportServer/Pages/ReportViewer.aspx?'
    )
    # httr2::url_parse(url) %>%
    # httr2::url_modify(
    #   path =
    #     stringr::str_replace(
    #       .$path,
    #       'reports/report',
    #       'ReportServer/Pages/ReportViewer.aspx?'
    #     )
    # ) %>%
    # httr2::url_build()

  req <-
    httr2::request(url_mod) %>%
    req_auth_negotiate(user = username)

  if (.explore) {

    resp <-
      req %>%
      httr2::req_perform()

    input_id <-
      resp %>%
      httr2::resp_body_html() %>%
      xml2::xml_find_all(
        # this xpath pulls `data-parameternames`, which are the parameters
        # users can choose, and their default values, if any
        paste(
          "//div[@data-parametername]",
          paste(
            "//input[contains(@id, 'ReportViewerControl')",
            "contains(@type, 'text')",
            "@value]",
            sep = ' and '
          ),
          sep = ' | '
        )
      ) %>%
      xml2::xml_attrs() %>%
      purrr::map(stack) %>%
      purrr::map(
        dplyr::filter, ind %in% c('id', 'value', 'data-parametername')
      ) %>%
      purrr::map(
        dplyr::mutate,
        values = stringr::str_remove(values, '_txtValue$')
      ) %>%
      purrr::map_dfr(
        tidyr::pivot_wider, names_from = 'ind', values_from = 'values'
      ) %>%
      dplyr::group_by(id) %>%
      tidyr::fill(`data-parametername`, value, .direction = 'updown') %>%
      dplyr::ungroup() %>%
      dplyr::distinct()

    input_id %>%
      tidyr::replace_na(
        list(value = 'No input detected; possibly checkbox?')
      ) %>%
      {

        pad <- max(nchar(.[['data-parametername']]))

        sprintf(
          '   %s: %s',
          stringr::str_pad(.[['data-parametername']], width = pad, 'right'),
          sprintf('%s', .[['value']])
        )

      } %>%
      paste(collapse = '\n') %>%
      message(
        'Default User Input:\n\n', .
      )

    # resp %>%
    #   httr2::resp_body_html() %>%
    #   xml2::xml_find_all(
    #     "//*[@id[contains(., 'ctl04_ctl03')]]"
    #   ) %>%
    #   xml2::xml_attrs() %>%
    #   purrr::map(stack)
    #
    #   dplyr::select(-id) %>%
    #   tidyr::replace_na(list(value = 'No default value')) %>%
    #   knitr::kable()

  }

  req$url <- paste0(req$url, format)

  req_with_query <-
    do.call(
      httr2::req_url_query,
      append(
        list(.req = req),
        user_params
      )
    )

  req_with_query$url <- gsub('%3Frs%3A', '&rs:', req_with_query$url)

  please <- req_with_query %>% httr2::req_perform()

  csv_output <-
    readr::read_csv(
      httr2::resp_body_string(please),
      col_types = readr::cols(.default = 'character'),
      skip = .skip
    ) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::where(is.character), rlang::chr_unserialise_unicode
      )
    )

}


# Helpers -----------------------------------------------------------------

#' Title
#'
#' @param req
#' @param user
#' @param reset_pw
#'
#' @returns
#'
#' @noRd
#'
req_auth_negotiate <- function(req, user, reset_pw = F) {

  if (reset_pw) try(keyring::key_delete(user), silent = T)

  if (inherits(try(keyring::key_get(user), silent = T), 'try-error')) {

    warning(
      "A password associated with `user` param is not found.\n",
      call. = F,
      immediate. = T
    )

    message(
      paste(
        "Setting up your password with `keyring` package.",
        "This is done with `keyring::key_set()`, and",
        "will save it in your OS's respective Credential Store.\n",
        "In Windows, see `Control Panel\\User Accounts\\Credential Manager`.\n",
        "For more info, see https://keyring.r-lib.org/\n",
        sep = '\n'
      )
    )

    keyring::key_set(user)

  }

  httr2::req_options(
    .req     = req,
    httpauth = 4L,
    userpwd  = ':::'
  )

}

#' EXPERIMENTAL
#'
#' @param url
#' @param user
#'
#' @returns
#'
#' @noRd
#'
handle_disclaimer <- function(url, user) {

  url <-
    httr2::url_parse(url) %>%
    httr2::url_modify(
      path = stringr::str_replace(.$path, '/reports/report/', '/ReportServer?/')
    ) %>%
    httr2::url_build()

  req <-
    dirname(url) %>%
    httr2::request() %>%
    req_auth_negotiate(user) %>%
    httr2::req_perform()

  req %>%
    httr2::resp_body_string() %>%
    cat


}

#' Title
#'
#' @param user
#'
#' @returns
#'
#' @noRd
#'
catalog_items <- function(user) {

  # require(jsonlite)
  # require(httr2)
  # require(dplyr)
  # require(tibble)

  ua <-
    paste(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
      'AppleWebKit/537.36 (KHTML, like Gecko)',
      'Chrome/133.0.0.0 Safari/537.36'
    )

  file.path(
    "https://reports.phsa.ca/reports/api/v2.0",
    "CatalogItems"
  ) %>%
    httr2::request() %>%
    httr2::req_user_agent(ua) %>%
    req_auth_negotiate(user) %>%
    httr2::req_perform() %>%
    httr2::resp_body_string() %>%
    jsonlite::fromJSON() %>%
    .$value %>%
    tibble::as_tibble()

}

