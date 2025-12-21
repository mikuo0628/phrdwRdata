# Internal package environment
# See https://r-pkgs.org/data.html
# I am currently using this to set warnings for read_ssrs so 
# it warns only once per session.
the <- new.env(parent = emptyenv())
