
is.Query <- function(x) return(inherits(x, 'MDX_Qry'))

is.OlapConnection <- function(x) return(inherits(x, 'OLAP_Conn'))
