cube <- function(mdx_qry) {

  if (is.Query(mdx_qry)) return(mdx_qry$cube)

  stop('Attempt to access "cube" on object that is not a correct MDX Query')

}


`cube<-` <- function(mdx_qry, value) {

  if (is.Query(mdx_qry)) {

    mdx_qry$cube <- value

    return(mdx_qry)

  }

  stop('Attempt to access "cube" on object that is not a correct MDX Query')

}
