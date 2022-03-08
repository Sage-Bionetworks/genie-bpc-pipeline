# Description: Functions for converting BPC REDCap Data Dictionary to non-PHI Data Dictionary.
# Author: Haley Hunter-Zinck
# Date: 2021-10-13

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

#' Gets the name associated with the file download of a Synapse entity.
#' 
#' @param synapse_id Synapse ID
#' @param keep_download TRUE if file downloaded from Synapse should be kept
#' after the download name has been captured; FALSE otherwise.
#' @return String representing download name
get_synapse_download_name <- function(synapse_id, keep_download = F) {
  
  file_path <- synGet(synapse_id, downloadFile = T)$path
  download_name <- tail(strsplit(file_path, split = "/")[[1]], n = 1)
  
  if (!keep_download) {
    file.remove(file_path)
  }
  return(download_name)
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

# dd functions -------------------------

#' Remove columns, rows, and entries from a REDCap Academic data 
#' dictionary to remove any potential PHI.  
#' 
#' @param dd Data frame representing data dictionary
#' @param is_prissmm TRUE if the data diciotnary is in PRISSMM format; FALSE otherwise
#' @return data frame representing non-PHI data dictionary 
remove_phi_from_dd <- function(dd, is_prissmm = T) {
  
  # remove columns, rows, and clear whole columns
  nonphi <- dd %>%
    filter(is.na(`Identifier?`) | `Identifier?` != "y") %>%
    filter(is.na(`Field Type`) | `Field Type` != 'descriptive')  %>%
    mutate(`mod Field Annotation` = NA) %>% 
    mutate(`mod Branching Logic (Show field only if...)` = NA) %>% 
    select(-c(`Field Annotation`, `Branching Logic (Show field only if...)`)) %>%
    rename(`Field Annotation` = `mod Field Annotation`) %>%
    rename(`Branching Logic (Show field only if...)` = `mod Branching Logic (Show field only if...)`)
  
  idx <- which(nonphi$`Field Type` == 'calc')
  nonphi$`Field Type`[idx] <- 'text'
  nonphi$`Choices, Calculations, OR Slider Labels`[idx] <- NA
  nonphi$`Text Validation Type OR Show Slider Number`[idx] <- "integer"
  
  if (is_prissmm) {
    
    nonphi <- nonphi %>%
      filter(!is.element(`Variable / Field Name`, c("cpt_qanotes", "rt_qanotes")))
    
    idx <- which(is.element(nonphi$`Variable / Field Name`, 
                            c("cur_curator", "qa_full_reviewer", "qa_full_reviewer_dual")))
    
    nonphi$`Field Type`[idx] <- 'text'
    nonphi$`Choices, Calculations, OR Slider Labels`[idx] <- NA
    nonphi$`Text Validation Type OR Show Slider Number`[idx] <- "integer"
    
    idx <- grep(x = nonphi$`Variable / Field Name`, pattern = "^drugs_drug_[0-9]$")
    nonphi$`Choices, Calculations, OR Slider Labels`[idx] <- paste0("49135, Investigational Drug | ", nonphi$`Choices, Calculations, OR Slider Labels`[idx])
  }
    
  return(nonphi)
}
