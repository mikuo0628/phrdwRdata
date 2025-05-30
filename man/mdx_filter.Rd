% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/mdx_builder.R
\name{mdx_filter}
\alias{mdx_filter}
\title{MDX Builder: process filters, discrete or range, by date or other data types.}
\usage{
mdx_filter(discrete = NULL, range = NULL, ..., .as_lines = T)
}
\arguments{
\item{discrete}{A \code{data.frame} object with 3 columns: \code{dim}, \code{attr}, and
\code{memb}, for "dimension", "attribute", and "member". The attribute must
belong to dimension, and member must belong to the attribute hierarchy.
Each member to filter for should have its own row in this \code{data.frame}.}

\item{range}{A \code{data.frame} object with 3 columns: \code{dim}, \code{attr}, and
\code{memb}, for "dimension", "attribute", and "member".
The attribute must belong to dimension, and member must belong to the
attribute hierarchy.
Two rows must be provided here with two different member values as
the \code{from} and \code{to}.

If no bounds, use \code{NULL} or "null".}

\item{...}{Reserved for future development.}

\item{.as_lines}{Boolean value that if \code{TRUE} (default), returns a character
vector of properly spaced MDX filter clauses. This is needed as input to
\code{mdx_from} for formatting purposes. If \code{FALSE}, returns a single element
character vector, for printing purposes.}
}
\value{
Character vector.
}
\description{
MDX Builder: process filters, discrete or range, by date or other data types.
}
\examples{
\dontrun{
mdx_filter(
  discrete =
    tibble(
      dim = 'LIS - Test',
      attr = 'Test Code',
      memb = c('TPE1', 'RPR')
    ),
  range =
    tibble(
      dim = 'LIS - Date - Collection',
      attr = 'Date',
      memb = c('2019-01-01', '2019-02-02')
    ),
  .as_lines = T
)
}

}
\keyword{internal}
