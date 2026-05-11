# Generates Powershell commands for batch script.

Helps users create the appropriate Powershell commands per user
specifications, such as Rscript.exe location, pandoc, standard log
outputs, etc. This function will parameterize by setting respective
variables for easy manual editing.

## Usage

``` r
create_r_batch_script(
  path_r_exe = NA_character_,
  path_proj_dir = NA_character_,
  path_run_script = NA_character_,
  path_std_log = NULL,
  .check_o_dirve = F,
  .set_pandoc = F,
  .load_renv = T,
  .renv_run = F,
  ...
)
```

## Arguments

- path_r_exe:

  full executable path to Rscript.exe. If `NA`, will automatically be
  determined based on the version of R in use.

- path_proj_dir:

  folder path to project. If `NA`, will automatically invoke
  [`getwd()`](https://rdrr.io/r/base/getwd.html).

- path_run_script:

  user must supply the path to the script to execute. Can supply without
  project directory for convenience. The function will check file
  existence.

- path_std_log:

  full file path to a log, where standard outputs will be redirected to.
  If `NULL`, a project_name.log will be created in a "log" directory
  under project root. If no log should be produced, this can accept
  boolean value `False`.

- .check_o_dirve:

  boolean value. Defaults to `False`. If `True`, adds line to check if O
  drive is mapped, and will map if not.

- .set_pandoc:

  boolean value. Defaults to `False`. If `True`, adds line to set pandoc
  location using
  [`rmarkdown::find_pandoc()`](https://pkgs.rstudio.com/rmarkdown/reference/find_pandoc.html).

- .load_renv:

  boolean value. Defaults to `False`. If `True`, adds line to check if
  `renv` is installed, and will install if not. This will also
  explicitly load project with
  [`renv::load()`](https://rstudio.github.io/renv/reference/load.html).

- .renv_run:

  boolean value. Defaults to `False`, and uses
  [`source()`](https://rdrr.io/r/base/source.html) to run script. If
  `True`, will use
  [`renv::run()`](https://rstudio.github.io/renv/reference/run.html) to
  run script.

- ...:

  users can supply further values to be assigned to variables. The name
  of the parameter corresponds to the variable, and the value will be
  assigned per Powershell syntax.
