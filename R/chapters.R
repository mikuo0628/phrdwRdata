chapters <- function(mdx_qry) {

  if (is.Query(mdx_qry)) return(axis(mdx_qry, 4))

  stop('Attempt to access "chapters" on object that is not a correct MDX Query')

}


`chapters<-` <- function(mdx_qry, value) {

  if (is.Query(mdx_qry)) {

    axis(mdx_qry, 4) <- value
    return(mdx_qry)

  }

  stop('Attempt to access "chapters" on object that is not a correct MDX Query')

}
