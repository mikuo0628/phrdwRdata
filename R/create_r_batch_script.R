#' Generates Powershell commands for batch script.
#'
#' @description
#' Helps users create the appropriate Powershell commands per user
#' specifications, such as Rscript.exe location, pandoc, standard log outputs,
#' etc.
#' This function will parameterize by setting respective variables for easy
#' manual editing.
#'
#' @param path_r_exe full executable path to Rscript.exe. If `NA`, will
#'   automatically be determined based on the version of R in use.
#' @param path_proj_dir folder path to project. If `NA`, will automatically
#'   invoke `getwd()`.
#' @param path_run_script user must supply the path to the script to execute.
#'   Can supply without project directory for convenience. The function will
#'   check file existence.
#' @param path_std_log full file path to a log, where standard outputs will be
#'   redirected to. If `NULL`, a project_name.log will be created in a
#'   "log" directory under project root. If no log should be produced, this can
#'   accept boolean value `False`.
#' @param .check_o_dirve boolean value. Defaults to `False`. If `True`, adds
#'   line to check if O drive is mapped, and will map if not.
#' @param .set_pandoc boolean value. Defaults to `False`. If `True`, adds
#'   line to set pandoc location using `rmarkdown::find_pandoc()`.
#' @param .load_renv boolean value. Defaults to `False`. If `True`, adds
#'   line to check if `renv` is installed, and will install if not. This will
#'   also explicitly load project with `renv::load()`.
#' @param .renv_run boolean value. Defaults to `False`, and uses `source()`
#'   to run script. If `True`, will use `renv::run()` to run script.
#' @param ... users can supply further values to be assigned to variables.
#'   The name of the parameter corresponds to the variable, and the value will
#'   be assigned per Powershell syntax.
#'
#' @returns
#' @export
#'
#' @examples
create_r_batch_script <- function(
    path_r_exe      = NA_character_,
    path_proj_dir   = NA_character_,
    path_run_script = NA_character_,
    path_std_log    = NULL,
    .check_o_dirve  = F,
    .set_pandoc     = F,
    .load_renv      = T,
    .renv_run       = F,
    ...
) {

  ps_cmds <- '@ECHO OFF'
  r_cmds  <- list()

  if (is.na(path_r_exe)) {

    path_r_exe <-
      R.home(component = 'bin') |>
      list.files('RScript', ignore.case = T, full.names = T) |>
      normalizePath() |>
      gsub(pattern = '\\\\', replacement = '/')

  }

  if (is.na(path_proj_dir)) path_proj_dir <- getwd()
  if (is.null(path_std_log)) {

    # TODO: clean this with PS var syntax
    path_std_log <-
      file.path(
        path_proj_dir, 'log',
        paste0(basename(path_proj_dir), '_run.log')
      )

    cat(sprintf('Default STD log output to:\n%s\n\n', path_std_log))

    if (!dir.exists(dirname(path_std_log))) {

      cat(
        sprintf(
          'STD log dir not available. One will be created here:\n%s\n\n',
          dirname(path_std_log)
        )
      )

      dir.create(path_std_log, recursive = T)

    }

  } else if (isFALSE(path_std_log)) { path_std_log <- NULL }

  if (!is.null(path_std_log)) {

    path_proj_dir <- gsub(path_proj_dir, '%DIR_PROJ%', path_std_log)

  }

  if (!file.exists(path_r_exe)) stop('Cannot find `Rscript.exe`')
  if (is.na(path_run_script)) stop('Must supply valid run script path')
  if (!file.exists(path_run_script)) stop('Cannot find run script')

  # clean path_run_script in case user included proj_dir
  path_run_script <-
    gsub(pattern = path_proj_dir, replacement = '', x = path_run_script)

  if (.check_o_dirve) {

    ps_cmds <-
      c(
        ps_cmds,
        paste(
          'IF EXIST O:\\',
          '(ECHO O Drive mapped)',
          'ELSE',
          '(NET USE O:',
          '\\\\phsabc.ehcnet.ca\\root)'
        )
      )

  }

  if (.set_pandoc) {

    r_cmds <-
      append(
        r_cmds,
        list(
          set_pandoc =
            sprintf(
              "Sys.setenv(RSTUDIO_PANDOC = '%s')\"",
              rmarkdown::find_pandoc()$dir
            )
        )
      )

  }

  if (.load_renv) {

    r_cmds <-
      append(
        r_cmds,
        list(
          try_renv = "try_find_renv <- try(find.package('renv'), silent = T)",
          install_renv = paste(
            "if (inherits(try_find_renv, 'try-error')) {",
            paste(
              "install.packages('renv',",
              "repos = c(CRAN = 'https://cloud.r-project.org'))"
            ),
            "}"
          ),
          load_renv = "renv::load('%DIR_PROJ%'); setwd(renv::project())"
        )
      )

  }

  assign_vars <-
    list(
      R_EXE      = path_r_exe,
      DIR_PROJ   = path_proj_dir,
      RUN_SCRIPT = path_run_script,
      LOG_FILE   = path_std_log
    ) %>%
    Filter(Negate(is.null), .)

  ps_cmds <-
    c(
      ps_cmds,
      unlist(
        lapply(
          X = names(assign_vars),
          FUN = function(name) {

            paste('SET', paste0('"', name, '=', assign_vars[[name]], '"'))

          }
        )
      )
    )

  ps_run_script <-
    paste(
      collapse = '/',
      paste0(
        '%',
        c(
          'DIR_PROJ',
          'RUN_SCRIPT'
        ),
        '%'
      )
    ) %>%
    paste0("'", ., "'") %>%
    {

      if (.renv_run) {

        paste0(
          'renv::run(',
          paste(
            sep = ', ',
            paste('script', ., sep = ' = '),
            paste('project', paste0("'%DIR_PROJ%'"), sep = ' = ')
          ),
          ')'
        )

      } else {

        paste0('source(', ., ')')

      }

    } %>%
    paste0('SET "run_script=', ., '"') %>%
    rlang::set_names('run_script')

  if (length(r_cmds) != 0) {

    ps_r_cmds <-
      c(
        rlang::set_names(
          unlist(
            lapply(
              X = names(r_cmds),
              FUN = function(name) {

                paste('SET', paste0('"', name, '=', r_cmds[[name]], '"'))

              }
            )
          ),
          names(r_cmds)
        ),
        ps_run_script
      )

  } else { ps_r_cmds <- ps_run_script }

  ps_final_execute <-
    paste0(
      'SET "final_execute=',
      paste(collapse = '; ', paste0('%', names(ps_r_cmds), '%')),
      '"'
    )

  ps_user_inputs <- rlang::list2(...)

  if (length(ps_user_inputs) != 0) {

    ps_user_inputs <-
      unlist(
        lapply(
          X = names(ps_user_inputs),
          FUN = function(name) {

            paste('SET', paste0('"', name, '=', ps_user_inputs[[name]], '"'))

          }
        )
      )

  }

  c(
    ps_cmds,
    ps_r_cmds,
    ps_user_inputs,
    ps_final_execute
  ) %>%
    paste(collapse = '\n') %>%
    paste0(
      '\n\n',
      '"%R_EXE%" -e "%final_execute%" > "%LOG_FILE%" 2>&1'
    )

}
