axis <- function(mdx_qry, i) {

  if (is.Query(mdx_qry)) return(mdx_qry$axes[[i]])

  stop('Attempt to access "axis" on object that is not a correct MDX Query')

}


`axis<-` <- function(mdx_qry, i, value) {

  if (is.Query(mdx_qry)) {

    while (i > length(mdx_qry$axes)) {

      mdx_qry$axes <- c(mdx_qry$axes, '')

    }

    mdx_qry$axes[[i]] <- value

    return(mdx_qry)

  }

  stop('Attempt to access "axis" on object that is not a correct MDX Query')

}
