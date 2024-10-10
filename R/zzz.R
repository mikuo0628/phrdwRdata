.onLoad <- function(libname, pkgname) {

  # packageStartupMessage()

  dyn.load(
    system.file(
      paste0('libs', Sys.getenv('R_ARCH'), '/phrdwRdata.dll'),
      # paste0('libs', '/x64', '/phrdwRdata.dll'),
      # paste0('libs', '/i386', '/phrdwRdata.dll'),
      # 'libs/phrdwRdata.dll',
      package = 'phrdwRdata'
    )
  )

}
