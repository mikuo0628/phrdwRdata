

#' Rename PHRDW Columns
#'
#' Function renames the columns for queries executed against a PHRDW Datamart
#' @param phrdw_dataset A data frame with columns from the PHRDW.
#' @return A data frame with renamed columns from a PHRDW query
#'
#' @export
#' @importFrom dplyr %>%
rename_phrdw_columns <- function(phrdw_dataset)
{
  # Get the column names from the phrdw dataset
  column_names <- names(phrdw_dataset)

  # split each column name based on a ].[ pattern
  splits = stringr::str_split(column_names, "\\].\\[")

	str_num = 1

	#
	# Loop through each column name
	#
	for(i in splits){


    str_token_1 = i[[1]]
		str_token_2 = i[[2]]

		# print(str_token_1)

		# remove the start [ on the first token
		str_token_1 = substr(str_token_1, 2, nchar(str_token_1))


    # Check if the column name is apart of the Measures group
		if(stringr::str_detect(str_token_1, "Measures")){
		  str_token_3 = ""
		  str_token_4 = ""
		  str_token_2 = substr(str_token_2, 1, nchar(str_token_2)-1)

		}else{
		  str_token_3 = i[[3]]
		  str_token_4 = i[[4]]
		  str_token_4 = substr(str_token_4, 1, nchar(str_token_4)-1)
		}


		# For Measures, use token 2 as the column name
		if(stringr::str_detect(str_token_1,"Measure")){
		  column_names[[str_num]] = str_token_2

		  #
		  # Rename column names in [LIS - Patient Location at Order] Dimension
		  #
		}else if( stringr::str_detect(str_token_1,"LIS - Patient Location at Order")){

		  column_names[[str_num]]=ifelse(  str_token_3 == "Hospital Code", "patient_hospital_code",
		  									ifelse(  str_token_3 == "Location Type", "patient_location_type",
													str_token_3))
		  #
		  # Rename column names in [LIS - Lab Location - Order Entry] Dimension
		  #
		}else if( stringr::str_detect(str_token_1,"Lab Location - Order Entry")){

		  column_names[[str_num]]=ifelse(  str_token_3 == "Lab Code", "order_entry_lab_code",
		                                   ifelse(  str_token_3 == "Hospital Code", "order_entry_hospital_code",
		                                            ifelse(  str_token_3 == "Lab Name", "order_entry_lab_name",
															ifelse(  str_token_3 == "Lab Location Code", "order_entry_lab_location_code",
		                                                     		ifelse(  str_token_3 == "Lab Location Name", "order_entry_lab_location_name",
		                                                                       ifelse(  str_token_3 == "Lab Location Description", "order_entry_lab_location_descr",
		                                                                                str_token_3))))))
		  #
		  # Rename column names in [LIS - Lab Location - Result] Dimension
		  #
		}	else if( stringr::str_detect(str_token_1,"Lab Location - Result")){
		  column_names[[str_num]]=ifelse(  str_token_3 == "Lab Code", "result_lab_code",
		                                   ifelse(  str_token_3 == "Hospital Code", "result_hospital_code",
		                                            ifelse(  str_token_3 == "Lab Name", "result_lab_name",
															ifelse(  str_token_3 == "Lab Location Code", "result_lab_location_code",
		                                                     			ifelse(  str_token_3 == "Lab Location Name", "result_lab_location_name",
		                                                                       ifelse(  str_token_3 == "Lab Location Description","result_lab_location_description",
		                                                                                str_token_3))))))

		# For Dates, use the first token. For date parts (ex. Year), use token 1 and 3.
		# Ensure Age or Based On dimensions are not accepted
		}else if(stringr::str_detect(str_token_1,"Date") && !(stringr::str_detect(str_token_1,"Age"))
		         && !(stringr::str_detect(str_token_1,"Based On")) && !(stringr::str_detect(str_token_3,"Date From"))){

		  # Perform an exact match on token 3 for string "Date" using ^ start anchor and $ end anchor
			if(stringr::str_detect(str_token_3, "^Date$")){

			  #
			  # This column is a date field. Assign the data type.
			  # It is harder to determine this once the column has been renamed.
			  #

			  # phrdw_dataset[,str_num] = as.Date(phrdw_dataset[,str_num])
			  phrdw_dataset[,str_num] = as.Date(phrdw_dataset[[str_num]])
			  column_names[[str_num]]=str_token_1
			  # print(paste("AFTER AS.DATE",str_token_1, str_token_2, str_token_3))

			}else{
			   column_names[[str_num]]=paste(str_token_1, str_token_3)
			}


		# Use either token 1 & 3 or token 1 & 4 for the LIS Copy To Provider. Multiple fields could all have Provider Name as the variable
		}else if(stringr::str_detect(str_token_1,"- Copy To")){

		  #print(paste("COPY TO ",str_token_1, str_token_2, str_token_3, str_token_4))

		  if(stringr::str_detect(str_token_4,"MEMBER")){
		    column_names[[str_num]]=paste(str_token_1, str_token_3)
		  }else{
		    column_names[[str_num]]=paste(str_token_1, str_token_4)
		  }




		# Use token 1 for the [LIS - Interval - Days Since ... dimensions. Multiple fields could all have Interval in Days as the variable
		}else if(stringr::str_detect(str_token_3,"Interval in Days")){

		  column_names[[str_num]]=paste(str_token_1)

		# Use token 1 & 3 for the [LIS - Interval - Days Since ... Interval Group 1 variables. Multiple fields could all have Interval Group 1 as the variable
		}else if(stringr::str_detect(str_token_3,"Interval Group 1")){

		  column_names[[str_num]]=paste(str_token_1,str_token_3)

		# Use token 1 for Flag dimensions. These dimensions have a Yes No field name.
		}else if(stringr::str_detect(str_token_1,"- Flag ")){

		  column_names[[str_num]]=paste(str_token_1)

		#
		# Use token 3 for normal fields that can be uniquely identified
		#
		}else if(stringr::str_detect(str_token_4,"MEMBER")){

			column_names[[str_num]]= str_token_3

		# For properties and any other column names, the 4th token will be used.
		}else{
			column_names[[str_num]]= str_token_4
		}

		str_num = str_num + 1
	}

	# Clean the column names to remove extra spaces and dashes. Convert all to lower case
	column_names = column_names %>%
	  stringr::str_replace_all(" - ", "_") %>%
	  stringr::str_replace_all(" ", "_") %>%
	  stringr::str_replace_all("-", "_") %>%
	  stringr::str_to_lower()

  names(phrdw_dataset) = column_names


	return(phrdw_dataset)
}
