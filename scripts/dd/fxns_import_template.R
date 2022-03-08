# Description: Functions for generating an import template from a PRISSMM REDCap data dictionary.
# Author: Haley Hunter-Zinck
# Date: 2021-10-28

# setup ----------------------------

library(glue)
library(dplyr)
library(synapser)
synLogin()

# input functions -------------------------

#' Function to wait indefinitely upon a certain condition.
#' 
#' @param cond Boolean value, usually from an evaluated condition
#' @param msg A string to print upon condition being TRUE
#' @return NA
waitifnot <- function(cond, msg) {
  if (!cond) {
    
    for (str in msg) {
      message(str)
    }
    message("Press control-C to exit and try again.")
    
    while(T) {}
  }
}

# synapse functions ----------------------

#' Check if a string is a Synapse ID that links to a valid entity.
#' 
#' @param synapse_id character string
#' @return TRUE if the string is a valid Synapse ID; FALSE otherwise
is_synapse_entity_id <- function(synapse_id) {
  
  res <- tryCatch({
    synGet(synapse_id, downloadFile = F)
  }, error = function(cond) {
    return(NULL)
  })
  
  return(as.logical(length(res)))
}

#' Download and load data stored in csv or other delimited format on Synapse
#' into an R data frame.
#' 
#' @param synapse_id Synapse ID
#' @version Version of the Synapse entity to download.  NA will load current
#' version
#' @param set Delimiter for file
#' @param na.strings Vector of strings to be read in as NA values
#' @param header TRUE if the file contains a header row; FALSE otherwise.
#' @param check_names TRUE if column names should be modified for compatibility 
#' with R upon reading; FALSE otherwise.
#' @return data frame
get_synapse_entity_data_in_csv <- function(synapse_id, 
                                           version = NA,
                                           sep = ",", 
                                           na.strings = c("NA"), 
                                           header = T,
                                           check_names = F) {
  
  if (is.na(version)) {
    entity <- synGet(synapse_id)
  } else {
    entity <- synGet(synapse_id, version = version)
  }
  
  data <- read.csv(entity$path, stringsAsFactors = F, 
                   na.strings = na.strings, sep = sep, check.names = check_names,
                   header = header)
  return(data)
}

#' Store a file on Synapse with options to define provenance.
#' 
#' @param path Path to the file on the local machine.
#' @param parent_id Synapse ID of the folder or project to which to load the file.
#' @param file_name Name of the Synapse entity once loaded
#' @param prov_name Provenance short description title
#' @param prov_desc Provenance long description
#' @param prov_used Vector of Synapse IDs of data used to create the current
#' file to be loaded.
#' @param prov_exec String representing URL to script used to create the file.
#' @return TRUE if successful, otherwise return error.
save_to_synapse <- function(path, 
                            parent_id, 
                            file_name = NA, 
                            prov_name = NA, 
                            prov_desc = NA, 
                            prov_used = NA, 
                            prov_exec = NA) {
  
  if (is.na(file_name)) {
    file_name = path
  } 
  file <- File(path = path, parentId = parent_id, name = file_name)
  
  if (!is.na(prov_name) || !is.na(prov_desc) || !is.na(prov_used) || !is.na(prov_exec)) {
    act <- Activity(name = prov_name,
                    description = prov_desc,
                    used = prov_used,
                    executed = prov_exec)
    file <- synStore(file, activity = act)
  } else {
    file <- synStore(file)
  }
  
  return(T)
}

# import template functions -------------------------

#' Create a RCC import template from the data dictionary.  
#' 
#' @param dd data frame representing REDCap data dictionary for BPC PRISSMM
#' @return vector representing the import template
create_import_template <- function(dd) {
  
  # record id column
  template <- dd$`Variable / Field Name`[1]
  
  # rcc columns
  template <- c(template, "redcap_repeat_instrument", "redcap_repeat_instance", "redcap_data_access_group")
  
  # iterate through variables in data dictionary order
  form_prev <- dd$`Form Name`[2]
  for (i in 2:length(dd$`Variable / Field Name`)) {
    
    form_curr <- dd$`Form Name`[i]
    var_name <- dd$`Variable / Field Name`[i]
    
    # add complete column
    if (form_curr != form_prev) {
      template <- c(template, paste0(form_prev, "_complete"))
    }
    
    # if check box
    if (dd$`Field Type`[i] == "checkbox") {
      choice_str <- unlist(dd %>% 
                             filter(`Variable / Field Name` == var_name) %>%
                             select(`Choices, Calculations, OR Slider Labels`))
      choice_code <- trim(unlist(lapply(strsplit(strsplit(choice_str, split = "\\|")[[1]], split = ", "), head, n = 1)))
      template <- c(template, paste0(var_name, "___", choice_code))
    } else {
      template <- c(template, var_name)
    }
    
    form_prev <- form_curr
  }
  
  # add last complete columns
  form_curr <- tail(dd$`Form Name`, 1)
  template <- c(template, paste0(form_prev, "_complete"))
  
  return(template)
}

# misc functions ----------------------------

#' Trim whitespace at the beginning or end of a string.
#' 
#' @param str character string
#' @return character string with leading or trailing whitespace removed
#' @param trim(" hello world ")
trim <- function(str) {
  front <- gsub(pattern = "^[[:space:]]+", replacement = "", x = str)
  back <- gsub(pattern = "[[:space:]]+$", replacement = "", x = front)
  
  return(back)
}
