.onLoad <- function(libname, pkgname) {

  # packageStartupMessage()

  dyn.load(
    system.file(
      paste0('libs', Sys.getenv('R_ARCH'), '/phrdwRdata.dll'),
      # 'libs/phrdwRdata.dll',
      package = 'phrdwRdata'
    )
  )

}
