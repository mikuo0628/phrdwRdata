columns <- function(mdx_qry) {

  if (is.Query(mdx_qry)) return(axis(mdx_qry, 1))

  stop('Attempt to access "columns" on object that is not a correct MDX Query')

}


`columns<-` <- function(mdx_qry, value) {

  if (is.Query(mdx_qry)) {

    axis(mdx_qry, 1) <- value

    return(mdx_qry)

  }

  stop('Attempt to access "columns" on object that is not a correct MDX Query')

}
