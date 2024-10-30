.onLoad <- function(libname, pkgname) {

  # packageStartupMessage()

  if (.Platform$OS.type == 'windows') {

    dyn.load(
      system.file(
        file.path(
          'libs',
          ifelse(nchar(.Platform$r_arch) == 0, 'x64', .Platform$r_arch),
          paste0('phrdwRdata', .Platform$dynlib.ext)
        ),
        package = 'phrdwRdata'
      )
    )

  }

}

.onAttach <- function(libname, pkgname) {

  if (.Platform$OS.type != 'windows') {

    packageStartupMessage(
      paste(
        'Please note: your OS is not Windows.',
        'Data cubes will not be accessible.',
        sep = '\n'
      )
    )

  }

}

