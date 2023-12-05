.onLoad <- function(libname, pkgname) {

  # packageStartupMessage()

  dyn.load(
    system.file(
      paste0('libs', Sys.getenv('R_ARCH'), '/olap.dll'),
      package = 'phrdwRdata'
    )
  )

}
