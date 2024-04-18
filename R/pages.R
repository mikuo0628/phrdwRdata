pages <- function(mdx_qry) {

  if (is.Query(mdx_qry)) return(axis(mdx_qry, 3))

  stop('Attempt to access "pages" on object that is not a correct MDX Query')

}


`pages<-` <- function(mdx_qry, value) {

  if (is.Query(mdx_qry)) {

    axis(mdx_qry, 3) <- value

    return(mdx_qry)

  }

  stop('Attempt to access "pages" on object that is not a correct MDX Query')

}
