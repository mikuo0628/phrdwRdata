slicers <- function(mdx_qry) {

  if (is.Query(mdx_qry)) return(mdx_qry$slicers)

  stop('Attempt to access "slicers" on object that is not a correct MDX Query')

}


`slicers<-` <- function(mdx_qry, value) {

  if (is.Query(mdx_qry)) {

    mdx_qry$slicers <- value

    return(mdx_qry)

  }

  stop('Attempt to access "slicers" on object that is not a correct MDX Query')

}
