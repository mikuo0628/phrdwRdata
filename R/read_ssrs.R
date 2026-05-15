#' Retrieves data from SSRS URLs.
#'
#' @description
#' `read_ssrs` provides a high-level interface to SQL Server Reporting Services
#'   (SSRS). It handles the complex lifecycle of an SSRS request: discovering
#'   report metadata via GUID, resolving cascading (dependent) parameters,
#'   and rendering the final output via the ReportServer engine.
#'
#' @details
#' The function operates in three primary phases:
#' \enumerate{
#'   \item \bold{Discovery}: Converts the human-readable portal URL into a
#'   unique System GUID using the SSRS REST API.
#'   \item \bold{Resolution}: If `.resolve_dependents = TRUE`, it calls the
#'   `Model.GetParameters` bound action. This server-side logic ensures that if
#'   you select a "Disease", the "Serotypes" are automatically filtered and
#'   filled in the background.
#'   \item \bold{Execution}: Sends the final payload as
#'   `application/x-www-form-urlencoded` via a POST request to the
#'   ReportServer. This avoids the "URL too long" errors
#'   common with large MDX parameter sets.
#' }
#'
#' It provides an interface to pull data into R environment by
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
#' Report name: XYZ
#' Report path: /bccdc/XYZ
#' Default User Input (showing only 1):
#'
#'   SurveillanceReportedStartDate : 2026-05-04T00:00:00.0000000
#'   SurveillanceReportedEndDate   : 2026-05-11T00:00:00.0000000
#'
#' ```
#'
#' To use filters in this function, simply refer to what's printed above, and
#'   add them as part of the function:
#'
#' ```r
#' read_ssrs(
#'   url             = YOUR_SSRS_URL,
#'   SurveillanceReportedStartDate = "1/1/2025",
#'   SurveillanceReportedEndDate   = "2/1/2025"
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
#' @param url Character.
#'   The full SSRS portal URL
#'   (e.g., \code{https://reports.phsa.ca/reports/report/...}).
#' @param ... SSRS report filters. You can use the human-readable labels
#'   found in the web UI; the function will automatically map these to
#'   the technical MDX strings required by the back end.
#' @param username Character. Your PHSA/Network user ID.
#'   If NULL, uses current Windows session credentials via NTLM/Negotiate.
#' @param format Character. The output format. Defaults to "CSV".
#' @param .explore Logical. If \code{TRUE}, will print useful information for
#'   user input, and return invisibly a \code{data.frame} of the full detail.
#' @param .check_params Logical. In SSRS UI, the prompt is meant for user
#'   readability (ie. The values in the parameters may not reflect the actual
#'   values to be filtered in the backend). If \code{TRUE}, user can supply
#'   values seen in the UI, and the function will convert them to the backend-
#'   appropriate values.
#' @param .resolve_dependents Logical. If \code{TRUE}, the function will
#'   query the server to resolve cascading dependencies.
#'   This is necessary when one filter (e.g., Health Authority)
#'   restricts the valid values of another (e.g., Community).
#' @param .skip Integer. Number of lines to skip at the top of the CSV
#'   (e.g., for reports with headers/metadata).
#' @param .in_memory Logical or Character. If \code{TRUE}, processes data
#'   in RAM. If \code{FALSE} or a file path, streams the download to disk
#'   to handle large datasets.
#' @param .col_types One of \code{NULL},
#'   a \code{\link[readr:cols]{readr::cols()}} specification,
#'   or a string. Controls how the downloaded columns are parsed.
#'   \itemize{
#'     \item If \code{NULL} (default), column types are guessed based on the
#'           first 1,000 rows.
#'     \item To keep all columns as character data (recommended for manual
#'           cleaning), use \code{readr::cols(.default = "c")}.
#'     \item See \code{\link[readr:read_csv]{readr::read_csv()}} for full
#'           details on supported formats.
#'   }
#'   It is highly recommended that users read data as \code{character} type
#'   and perform explicit cleaning actions instead of relying on heuristics.
#' @param .req_options List. Additional curl options passed to
#'   \code{httr2::req_options}.
#'
#'
#' @return A \code{tibble} containing the report data, or
#'   a metadata \code{data.frame} if \code{.explore} is active.
#' @export
#'
read_ssrs <- function(
    url          = '',
    ...,
    username            = NULL,
    format              = c('CSV')[1],
    .explore            = list(F, "default", "valid")[[1]],
    .check_params       = T,
    .resolve_dependents = F,
    .skip               = 0L,
    .in_memory          = T,
    .col_types          = NULL,
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
    !isFALSE(.explore) || isTRUE(.check_params) || isTRUE(.resolve_dependents)
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

    if (length(user_params) > 0 & isTRUE(.check_params)) {

      # 3) ensure user_params consistent with ValidValues
      user_params <-
        user_params %>%
        purrr::imap(
          ~ {

            df_valid_values <-
              report_inputs %>%
              dplyr::filter(Name == .y) %>%
              dplyr::select(Name, ValidValues) %>%
              tidyr::unnest(cols = c(ValidValues))

            if (nrow(df_valid_values) == 0) return(.x)

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
        tidyr::unnest(cols = c(DefaultValues), keep_empty = TRUE) %>%
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

      df_valid_values <-
        tidyr::unnest(
          dplyr::select(print_info, Name, ValidValues),
          cols = c(ValidValues)
        )

      dplyr::select(
        print_info,
        Name,
        Value = DefaultValues
      ) %>%
        {

          if (nrow(df_valid_values) > 0) {

            dplyr::left_join(.,  df_valid_values, by = c("Name", "Value")) %>%
              dplyr::mutate(Value = dplyr::coalesce(Label, Value))


          } else { . }

        } %>%
        dplyr::select(Name, Value) %>%
        {

          param_names <- .$Name
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

      return(invisible(report_inputs))

    }

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
      # col_types = readr::cols(.default = 'character'),
      col_types = .col_types,
      skip = .skip
    ) %>%
    dplyr::mutate(
      dplyr::across(
        dplyr::where(is.character), rlang::chr_unserialise_unicode
      )
    )

  if (file.exists(please)) on.exit(unlink(please, force = T), add = T)

  return(csv_output)

}


# Helpers -----------------------------------------------------------------

#' Resolve Cascading Parameters via SSRS Bound Action
#'
#' @description
#' Formally invokes the \code{Model.GetParameters} POST action. This is the
#' "Logic Engine" of the function, ensuring that dependent parameters are
#' recalculated based on the current \code{user_params} state.
#'
#' @param report_host The base scheme and domain.
#' @param report_base Named vector containing API and Render paths.
#' @param report_id The GUID of the report.
#' @param username Optional credential string.
#' @param user_params The current list of parameter selections.
#'
#' @return A tibble of updated \code{ParameterDefinitions}.
#' @keywords internal
#' @noRd
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

  resp <- httr2::req_perform(req)

  report_dependents <-
    httr2::resp_body_string(resp) %>%
    jsonlite::fromJSON() %>%
    .$value %>%
    tibble::as_tibble()

  return(report_dependents)

}

#' Create Multi-Part or Form Payloads
#'
#' @description
#' A specialized flattener that transforms R lists into the specific structures
#' required by different SSRS endpoints.
#'
#' @param user_params A named list of parameters.
#' @param type Character. Either \code{"dependents"} (outputs a nested JSON-ready
#'   list of Name/Value objects) or \code{"download"} (outputs a flat list with
#'   duplicate names for Form encoding).
#'
#' @details
#' For \code{"download"}, this function ensures that multi-value parameters
#' are "exploded" into separate list elements so that \code{httr2::req_body_form}
#' produces repeated keys (e.g., \code{&id=1&id=2}).
#'
#' @return A list formatted for \code{req_body_json} or \code{req_body_form}.
#' @keywords internal
#' @noRd
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

#' Fetch Raw Parameter Metadata
#'
#' @description
#' Hits the \code{ParameterDefinitions} REST endpoint to retrieve the
#' structural requirements of the report (types, nullability, valid values).
#'
#' @keywords internal
#' @noRd
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


#' Handle NTLM/Negotiate Authentication
#'
#' @description
#' Manages the \code{userpwd} and \code{httpauth} options for \code{httr2}.
#' Integrates with \code{keyring} for secure local credential storage.
#'
#' @author [Brendan Bakos](mailto:brendan.bakos@vch.ca) provided this
#'   implementation.
#'
#' @keywords internal
#' @noRd
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
      # httpauth = 4L,
      httpauth = 31L, # CURLAUTH_ANY; will Kerberos, NTLM, and others
      userpwd  = sprintf('%s:%s', user, keyring::key_get(user))
    )

  } else {

    httr2::req_options(
      .req     = req,
      # httpauth = 4L,
      httpauth = 31L,
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

