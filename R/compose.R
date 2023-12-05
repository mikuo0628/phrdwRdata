compose <- function(mdx_qry) {

  if (is.Query(mdx_qry)) {

    if ("" %in% axis(mdx_qry, 1)) {

      stop("Columns must be set, and cannot contain blank entries.")

    }

    mdx <-
      paste0(
        "SELECT {",
        paste(axis(mdx_qry, 1), collapse = ', '),
        "} ON AXIS(0)"
      )

    if (length(mdx_qry$axes) > 1) {

      for (i in 2:length(mdx_qry$axes)) {

        axis <- axis(mdx_qry, i)

        if ("" %in% axis) stop("Axes cannot contain blank entires.")

        axisName <-
          paste0(
            "AXIS(",
            as.character(i - 1),
            ')'
          )

        mdx <-
          paste0(
            mdx,
            ", {",
            paste(axis, collapse = ', '),
            '} ON ',
            axisName
          )

      }

    }

    if ("" %in% cube(mdx_qry) || length(cube(mdx_qry)) > 1) {

      stop("A single Cube must be set.")

    }

    mdx <-
      paste0(
        mdx_qry,
        " WHERE {",
        paste(slicers(mdx_qry), collapse = ', ')
      )

    if (!("" %in% slicers(mdx_qry))) {

      mdx <-
        paste0(
          mdx, " WHERE {" , paste(slicers(mdx_qry), collapse = ', '), "}"
        )

    }

    return(mdx)

  } else {

    stop('Attempt to access "compose" on object that is not a correct MDX Query')

  }

}
