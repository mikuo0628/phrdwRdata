#' Wrapper to explore cube names, dimensions, hierarchies, and levels of cube
#' connection.
#'
#' @description
#' Given `olapCnn` OLAP connection string, returns info on cubes under the OLAP
#' server. If no cube information is supplied, it will return all the
#' information of the highest level.
#'
#' @param olapCnn Required. An OLAP connection string.
#'   Can be created with [connect_to_phrdw()].
#'
#' @param cube Optional. Cube name.
#'
#' @param dimension Optional. Dimension name.
#'
#' @param hierarchy Optional. Hierarchy name.
#'
#' @param level Optional. Level name.
#'
#' @return Character vector of cube(s) information.
#'
#' @export
#'
explore <- function(
    olapCnn,
    cube      = NULL,
    dimension = NULL,
    hierarchy = NULL,
    level     = NULL
) {

  if (!is.OlapConnection(olapCnn)) {

    stop('olapCnn must be class of "OLAP_Conn".')

  }

  levels <-
    list(
      cube      = cube,
      dimension = dimension,
      hierarchy = hierarchy,
      level     = level
    )

  checks <-
    levels %>%
    purrr::imap(
      ~ {

        if (!(is.null(.x) || is.character(.x))) {

          stop(
            paste(
              stringr::str_to_title(.y),
              'must be either NULL or a string.'
            )
          )

        }

        !is.null(.x)

      }
    )

  dll_routines <-
    c(
      default   = 'olapRExploreCubes',
      cube      = 'olapRExploreDimensions',
      dimension = 'olapRExploreHierarchies',
      hierarchy = 'olapRExploreLevels',
      level     = 'olapRExploreMembers'
    )

  explore_levels <- purrr::discard(checks, isFALSE)
  last_level     <- names(explore_levels)[length(explore_levels)]

  # Default: cube, no layer option needed
  if (identical(unname(explore_levels), list())) {

    return(
      invisible_Call(
        dll_routines[1],
        olapCnn$cnn,
        PACKAGE = 'phrdwRdata'
      )
    )

  } else if (
    # Did user supply enough layer option values to dive that deep?
    (which(names(dll_routines) == last_level) - 1) !=
    length(explore_levels)
  ) {

    stop(
      paste0(
        '\n',
        'Please specify correct higher level options ',
        'before you can explore deeper.',
        '\n(i.e. you cannot explore dimension without a cube).\n',
        collapse = ' '
      )
    )

  } else {

    output <-
      try(
        do.call(
          what = invisible_Call,
          args =
            append(
              list(
                dll_routines[[last_level]],
                olapCnn$cnn,
                PACKAGE = 'phrdwRdata'
              ),
              unlist(levels)
            )
        ),
        silent = T
      )

  }

  if (inherits(output, 'try-error')) {

    if (stringr::str_detect(output[1], 'name is invalid')) {

      stop(stringr::str_extract(output[1], '(?<=:\\s).*'), call. = F)

    } else {

      stop(output[1])

    }

  } else {

    return(output)

  }

}


#' Wrapper to capture print output from calling C/C++ OLAP routines.
#'
#' @description
#' Simple function to capture `cat` into usable character vector.
#'
#' @param ...
#'
#' @return A character vector.
#'
#' @noRd
#'
invisible_Call <- function(...) { capture.output(invisible(.Call(...))) }
