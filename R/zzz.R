.onLoad <- function(libname, pkgname) {

  # packageStartupMessage()

  dyn.load(
    system.file(
      file.path(
        'libs',
        ifelse(nchar(.Platform$r_arch) == 0, 'x64', .Platform$r_arch),
        paste0('phrdwRdata', '.dll')
      ),
      package = 'phrdwRdata'
    )
  )

}

