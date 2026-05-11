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
#' Your user credential, if provided, is managed by `keyring` package.
#'   This prevents you from entering your credentials in the console or
#'   saving it in the script, which are both not ideal practices for security.
#'
#' `keyring` will leverage your operating system's credential manager to
#'   handle your saved credential.
#'
#' @author [Brendan Bakos](mailto:brendan.bakos@vch.ca) contributed the
#'   implementation of the crucial authentication/negotiation,
#'   and is instrumental in the design of the API handling.
#'
#' @param url SSRS url.
#' @param ... SSRS reports' built-in filters. See `Details`.
#' @param username
#'
#'   User ID (without email domain). Not required if you are on PHSA network.
#'   However, providing one allows one to set up batch jobs for scheduled
#'   runs. See `Details`.
#'
#' @param format
#'
#'   Some SSRS reports offer multiple formats to download. Currently only
#'   csv is supported and is the default value. May be extended in the future.
#'
#' @param .explore
#'
#'   Defaults to `FALSE`. If you are unsure what filters you could use for
#'   your SSRS report, set this to `TRUE` and a list of currently supported
#'   filters and their default values (if available; no values will be
#'   shown if dependent on other values, ie. query-based) will be
#'   printed in the console.
#'
#'   Additionally, set to `verbose` will instead of printing to console,
#'   return a `data.frame` of detailed parameter information, which can be used
#'   to assist users to design their scripts.
#'
#' @param .skip
#'
#'   SSRS reports in csv format may contain lines above the headers
#'   (meta info, descriptions, etc). You may not wish to have this in your
#'   data frame. If in your first run you noted there are lines above the
#'   headers, you can enter number of lines to skip here.
#'
#' @param .in_memory
#'
#'  If the body of the response is too large for your environment, you will
#'  run into `curl::curl_fetch_memory()` error. In this case, set this
#'  parameter to `FALSE`, and a `tempfile` will be created for you to
#'  temporarily store the response body while being parsed into a csv.
#'  Alternatively, provide a full path with file name to explicitly direct
#'  the `tempfile` to.
#'
#' @param .return_url For developer troubleshooting.
#' @param .req_options
#'
#'   Name-value list of valid curl option, as found in [curl::curl_options()].
#'
#' @returns A `tibble` object.
#' @export
#'
read_ssrs <- function(
    url          = '',
    ...,
    username            = NULL,
    format              = c('CSV')[1],
    .explore            = list(F, "default", "valid")[[1]],
    .resolve_dependents = F,
    .skip               = 0L,
    .in_memory          = T,
    .return_url         = F,
    .req_options        = list()
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

  # handle/create some base info
  user_params <- rlang::list2(...)
  url_comp    <- httr2::url_parse(url)
  report_host <- paste0(url_comp$scheme, "://", url_comp$hostname)
  report_path <- gsub("%20", " ", sub(".*reports/report", "", url_comp$path))
  report_name <- basename(url)
  report_base <-
    c(
      # API Base for GUIDs and Parameters
      api    = "/Reports/api/v2.0/",
      # For downloading the actual data
      render = "/ReportServer?"
    )

  # 1) GUID
  report_meta <-
    httr2::request(
      sprintf(
        paste0(
          report_host,
          report_base['api'],
          "Reports(Path='%s')"
        ),
        URLencode(report_path, reserved = T)
      )
    ) %>%
    req_auth_negotiate(user = username) %>%
    httr2::req_perform() %>%
    httr2::resp_body_json()

  req_download <-
    httr2::request(
      paste0(
        report_host,
        report_base["render"],
        URLencode(report_path)
      )
    ) %>%
    httr2::req_method("POST") %>%
    req_auth_negotiate(user = username)

  if (length(.req_options) != 0) {

    req <-
      do.call(
        what = httr2::req_options,
        args =
          append(
            list(
              .req = req
            ),
            .req_options
          )
      )

  }

  if (
    !isFALSE(.explore) ||
    length(user_params) > 0 ||
    isTRUE(.resolve_dependents)
  ) {

    # 2) get report inputs. Needed for:
    ## - explore param names
    ## - resolve dependencies
    ## - clean user params
    report_inputs <-
      get_report_inputs(
        report_host,
        report_base,
        report_meta$Id,
        username
      )

    if (!isFALSE(.explore)) {

      print_info <-
        report_inputs %>%
        dplyr::filter(
          ParameterVisibility == "Visible",
          Nullable == FALSE,
          # DefaultValuesIsNull == TRUE
        ) %>%
        dplyr::mutate(
          DefaultValues = purrr::map(DefaultValues, ~ head(.x, 1)),
        ) %>%
        tidyr::unnest(DefaultValues, keep_empty = TRUE) %>%
        tidyr::replace_na(list(DefaultValues = ""))

      message(
        sprintf(
          paste(
            collapse = "\n",
            sep = "\n",
            "Report name: %s",
            "Report path: %s"
          ),
          report_meta$Name,
          report_meta$Path
        )
      )

      df_valid_values <- tidyr::unnest(dplyr::select(print_info, ValidValues))

      dplyr::select(
        print_info,
        name = Name, Value = DefaultValues
      ) %>%
        {

          if (nrow(df_valid_values) > 0) {

            dplyr::left_join(.,  df_valid_values, by = "Value") %>%
              dplyr::mutate(Value = dplyr::coalesce(Label, Value))


          } else { . }

        } %>%
        dplyr::select(name, Value) %>%
        {

          param_names <- .$name
          pad_name <- max(nchar(param_names), na.rm = T)

          sprintf(
            "   %s : %s",
            stringr::str_pad(param_names, width = pad_name, 'right'),
            sprintf("%s", .$Value)
          ) %>%
            paste(collapse = '\n') %>%
            message(
              'Default User Input (showing only 1):\n\n', .
            )

        }

    }

    if (length(user_params) > 0) {

      # 3) ensure user_params consistent with ValidValues
      user_params <-
        user_params %>%
        purrr::imap(
          ~ {

            df_valid_values <-
              report_inputs %>%
              dplyr::filter(Name == .y) %>%
              dplyr::select(Name, ValidValues) %>%
              tidyr::unnest(ValidValues)
            if (nrow(df_valid_values) == 0) {
              return(.x)
            }

            df_valid_values %>%
              dplyr::filter(Label %in% .x) %>%
              dplyr::pull(Value)

          }
        )

    }

    if (isTRUE(.resolve_dependents)) {

      report_inputs <-
        resolve_dependents(
          report_host = report_host,
          report_base = report_base,
          report_id   = report_meta$Id,
          username    = username,
          user_params = user_params
        )

      user_params <-
        report_inputs %>%
        dplyr::filter(DefaultValuesIsNull == FALSE) %>%
        dplyr::select(Name, DefaultValues) %>%
        purrr::pmap(
          \(Name, DefaultValues) setNames(list(DefaultValues), Name)
        ) %>%
        unlist(F)

    }

    if (!isFALSE(.explore)) return(invisible(report_inputs))

  }

  payload <-
    append(
      list(
        `rs:Command` = "Render",
        `rs:Format`  = format
      ),
      create_payload(user_params, "download")
    )

  req_download <- httr2::req_body_form(req_download, !!!payload)

  # req_download <-
  #   req_download %>%
  #   httr2::req_error(is_error = \(resp) F) %>%
  #   httr2::req_perform()

  # ua <-
  #   paste(
  #     'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
  #     'AppleWebKit/537.36 (KHTML, like Gecko)',
  #     'Chrome/133.0.0.0 Safari/537.36'
  #   )

  if (.return_url) return(req_download$url)

  please <-
    req_download %>%
    {

      if (isTRUE(.in_memory)) {

        httr2::resp_body_string(httr2::req_perform(.))

      } else {

        if (isFALSE(.in_memory)) { .in_memory <- tempfile() }

        httr2::req_perform(., path = .in_memory)
        .in_memory

      }

    }

  csv_output <-
    readr::read_csv(
      please,
      col_types = readr::cols(.default = 'character'),
      skip = .skip
    ) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::where(is.character), rlang::chr_unserialise_unicode
      )
    )

  if (is.character(please)) on.exit(unlink(please, force = T), add = T)

  return(csv_output)

}


# Helpers -----------------------------------------------------------------

resolve_dependents <- function(
    report_host,
    report_base,
    report_id,
    username,
    user_params
) {

  url_deps <-
    sprintf(
      paste0(
        report_host, report_base["api"],
        "Reports(%s)/Model.GetParameters"
      ),
      report_id
    )

  payload <- create_payload(user_params, "dependents")

  req <-
    httr2::request(url_deps) %>%
    httr2::req_method("POST") %>%
    req_auth_negotiate(user = username) %>%
    httr2::req_body_json(payload)

  resp <-
    req %>%
    httr2::req_perform()

  report_dependents <-
    httr2::resp_body_string(resp) %>%
    jsonlite::fromJSON() %>%
    .$value %>%
    tibble::as_tibble()

  return(report_dependents)

}

create_payload <- function(user_params, type = c("dependents", "download")[1]) {

  if (length(user_params) == 0) return(NULL)

  payload <-
    if (type == "dependents") {

      list(
        ParameterValues =
          unname(
            purrr::imap(
              user_params,
              ~ {

                if (length(.x) > 1) {

                  return(
                    tibble::tibble(Name = .y, Value = .x) %>%
                      purrr::pmap(
                        \(Name, Value) list(Name = Name, Value = Value)
                      )
                  )

                }

                return(list(list(Name = .y, Value = .x)))

              }
            ) %>%
              unname() %>%
              purrr::reduce(append)
          )
      )

    } else if (type == "download") {

      purrr::imap(
        user_params,
        ~ {

          tibble::tibble(name = .y, value = .x) %>%
            purrr::pmap(\(name, value) setNames(list(value), name)) %>%
            unlist(F)

        }
      ) %>%
        purrr::reduce(append)

    }

  return(payload)

}

get_report_inputs <- function(report_host, report_base, report_id, username) {

  url_defs <-
    sprintf(
      paste0(
        report_host, report_base["api"],
        "Reports(%s)/ParameterDefinitions"
      ),
      report_id
    )

  report_inputs <-
    httr2::request(url_defs) %>%
    req_auth_negotiate(user = username) %>%
    httr2::req_perform() %>%
    httr2::resp_body_string() %>%
    jsonlite::fromJSON() %>%
    { .$value } %>%
    tibble::as_tibble()

  return(report_inputs)

}


#' Title
#'
#' @param req
#' @param user
#' @param reset_pw
#'
#' @returns
#'
#' @author [Brendan Bakos](mailto:brendan.bakos@vch.ca) provided this
#'   implementation.
#'
#' @noRd
#'
req_auth_negotiate <- function(req, user = NULL, reset_pw = F) {

  if (!is.null(user)) {

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
      userpwd  = sprintf('%s:%s', user, keyring::key_get(user))
    )

  } else {

    httr2::req_options(
      .req     = req,
      httpauth = 4L,
      userpwd  = ':::'
    )

  }

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
get_object_model <- function(user) {

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

  c(
    CatalogItems     = "CatalogItems",
    Extensions       = "Extensions",
    DataSources      = "DataSources",
    Resources        = "Resources",
    Subscriptions    = "Subscriptions",
    CacheRefreshPlan = "CacheRefreshPlan"
  )

  file.path(
    "https://reports.phsa.ca/reports/api/v2.0"
  )

  df_cat <-
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

  df_exts <-
    file.path(
      "https://reports.phsa.ca/reports/api/v2.0",
      "Extensions"
    ) %>%
    httr2::request() %>%
    req_auth_negotiate(user) %>%
    httr2::req_error(is_error = \(resp) FALSE) %>%
    httr2::req_perform() %>%
    httr2::resp_body_string() %>%
    jsonlite::fromJSON() %>%
    .$value %>%
    tibble::as_tibble()

}

