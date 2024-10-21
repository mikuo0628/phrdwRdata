.onLoad <- function(libname, pkgname) {

  # packageStartupMessage()

  dyn.load(
    system.file(
      file.path(
        'libs',
        .Platform$r_arch,
        paste0('phrdwRdata', .Platform$dynlib.ext)
      ),
      # file.path('libs', paste0('phrdwRdata', .Platform$dynlib.ext)),
      package = 'phrdwRdata'
    )
  )

}

