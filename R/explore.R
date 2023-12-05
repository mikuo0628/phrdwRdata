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

  checks <-
    list(
      cube      = cube,
      dimension = dimension,
      hierarchy = hierarchy,
      level     = level
    ) %>%
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

  if (
    (!checks$cube && (checks$dimension || checks$hierarchy || checks$level)) ||
    (!checks$dimension && (checks$hierarchy || checks$level)) ||
    (!checks$hierarchy && (checks$level))
  ) {

    stop(
      paste(
        'Need higher level options to be filled',
        'before you can explore deeper.',
        '\n(i.e. you cannot explore dimension without a cube).\n'
      )
    )

  }

  if (checks$cube) {

    if (checks$dimension) {

      if (checks$hierarchy) {

        if (checks$level) {

          return(
            .Call('olapRExploreMembers', olapCnn$cnn,
                  cube, dimension, hierarchy, level,
                  PACKAGE = 'olap')

          )

        }

        return(
          .Call('olapRExploreLevels', olapCnn$cnn,
                cube, dimension, hierarchy,
                PACKAGE = 'olap')
        )

      }

      return(
        .Call('olapRExploreHierarchies', olapCnn$cnn,
              cube, dimension,
              PACKAGE = 'olap')
      )

    }

    return(
      .Call('olapRExploreDimensions', olapCnn$cnn,
            cube, PACKAGE = 'olap')
    )

  } else {

    return(.Call('olapRExploreCubes', olapCnn$cnn, PACKAGE = 'olap'))

  }

}


