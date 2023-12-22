####################################################################
# fn_disease_check_list - builds a list with checks for what diseases
# are being queried for.
#
# Functions allow for easier filtering of columns returned in datasets
#
# Author: Darren Frizzell
# Created: 2019-01-25
#
####################################################################



#
# Function returns a list with the diseases that are being queried for
#
vpd_disease_check_list <- function(parameter_list)
{

  #
  # Get the diseases that are being queried for.
  #
  disease = parameter_list$disease


  #
  # Build the disease specific checks. Used for including disease specific dimensions
  #
  disease_check_list = list(measles_check = any(disease %in% "Measles"),
                            mumps_check = any(disease %in% "Mumps"),
                            bordetella_check = any(disease %in% c("Pertussis","Holmesii","Parapertussis")),
                            group_b_strep_check = any(disease %in% "Streptococcal disease (group B)"),
                            igas_check = any(disease %in% "Streptococcal disease (invasive group A - iGAS)"),
                            ipd_check = any(disease %in% "Pneumococcal disease (invasive)"),
                            meningo_check = any(disease %in% c("Meningococcal disease (invasive)","Meningococcal disease (non-invasive)")),
                            rubella_check = any(disease %in% c("Rubella", "Rubella (congenital)")),
                            reportable_vpd_check = any(disease %in% c("Diphtheria","Haemophilus influenzae (invasive disease)","Tetanus")),
                            other_disease_check = any(!disease %in% c("Diphtheria",
                                                                 "Haemophilus influenzae (invasive disease)",
                                                                 "Tetanus",
                                                                 "Rubella",
                                                                 "Rubella (congenital)",
                                                                 "Meningococcal disease (invasive)",
                                                                 "Meningococcal disease (non-invasive)",
                                                                 "Pneumococcal disease (invasive)",
                                                                 "Streptococcal disease (invasive group A - iGAS)",
                                                                 "Streptococcal disease (group B)",
                                                                 "Pertussis","Holmesii","Parapertussis",
                                                                 "Mumps",
                                                                 "Measles"
                                                                 )
                                                      )
                            )



  return(disease_check_list)
}




