rows <- function(mdx_qry) {

  if (is.Query(mdx_qry)) return(axis(mdx_qry, 2))

  stop('Attempt to access "rows" on object that is not a correct MDX Query')

}


`rows<-` <- function(mdx_qry, value) {

  if (is.Query(mdx_qry)) {

    axis(mdx_qry, 2) <- value

    return(mdx_qry)

  }

  stop('Attempt to access "rows" on object that is not a correct MDX Query')

}
