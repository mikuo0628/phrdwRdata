
is.Query <- function(x) return(inherits(phrdw_obj, 'MDX_Qry'))

is.OlapConnection <- function(x) return(inherits(phrdw_obj, 'OLAP_Conn'))
