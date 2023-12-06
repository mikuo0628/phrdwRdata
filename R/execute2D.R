execute2D <- function(olapCnn, mdx_qry) {

  stopifnot(is.OlapConnection(olapCnn))
  stopifnot(is.Query(mdx_qry) || is.character(mdx_qry))

  if (is.Query(mdx_qry)) {

    if (mdx_qry$vldt) {

      # where is this fn??
      if (!validateQuery(olapCnn, mdx_qry)) stop("Invalid MDX Query detected.")

    }

    mdx <- compose(mdx_qry)

  } else { mdx <- mdx_qry }

  result <- .Call('olapRExecute2D', olapCnn$cnn, mdx,
                  PACKAGE = 'phrdwRdata')

  if (!is.data.frame(result)) {

    stop("Object received from olapRExecuteRecordset is not data frame.")

  }

  return(result)

}
