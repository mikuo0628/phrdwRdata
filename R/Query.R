#' Construct OLAP query.
#' `Query` constructs a "Query" object. Set functions are used to build and
#' modify the Query axes and cube name.
#'
#' @param validate
#'
#' @return
#' @export
#'
#' @examples
Query <- function(validate = F) {

  if (!is.logical(validate)) {

    stop('Argument "validate" must be `logical`.')

  }

  qr <- list(
    vldt = validate,
    cube = '',
    axes = list(c('')),
    slicers = c("")
  )

  class(qr) <- append(class(qr), 'MDX_Qry')

  return(qr)

}
