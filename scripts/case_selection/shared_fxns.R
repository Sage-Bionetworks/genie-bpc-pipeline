waitifnot <- function(cond, msg = "") {
  if (!cond) {
    
    for (str in msg) {
      message(str)
    }
    message("Press control-C to exit and try again.")
    
    while(T) {}
  }
}

get_sites_in_config <- function(config, phase, cohort){
  if (phase == '1_additional') {
    return(names(config$phase[[1]]$cohort[[cohort]]$site))
  }else{
    return(names(config$phase[[phase]]$cohort[[cohort]]$site))
  }
}

get_default_global <- function(config, key) {
  return(config$default$global[[key]])
}

get_default_site <- function(config, site, key) {
  return(config$default$site[[site]][[key]])
}

get_custom <- function(config, phase, cohort, site, key) {
  return(config$phase[[phase]]$cohort[[cohort]]$site[[site]][[key]])
}

get_production <- function(config, phase, cohort, site) {
  get_custom(config = config, phase = phase, cohort = cohort, site = site, key = "production")
}

get_adjusted <- function(config, phase, cohort, site) {
  get_custom(config = config, phase = phase, cohort = cohort, site = site, key = "adjusted")
}

get_pressure <- function(config, phase, cohort, site) {
  
  n_pressure_defualt <- config$default$site[[site]]$pressure
  n_pressure_specific <- config$phase[[phase]]$cohort[[cohort]]$site[[site]]$pressure
  n_pressure <- if (!is.null(n_pressure_specific)) n_pressure_specific else n_pressure_defualt
  
  return(n_pressure)
}

get_sdv_or_irr_value <- function(config, phase, cohort, site, key = c("sdv", "irr")) {
  
  n_pressure <- get_pressure(config, phase, cohort, site)
  n_prod <- get_production(config, phase, cohort, site)
  
  # site default
  val_site <- get_default_site(config, site, key)
  if (!is.null(val_site) && is.na(val_site)) {
    if (val_site < 1) {
      return(round(val_site * (n_prod)))
    }
    return(val_site)
  }
  
  # custom
  val_custom <- get_custom(config, phase, cohort, site, key)
  if (!is.null(val_custom) && !is.na(val_custom)) {
    if (val_custom < 1) {
      return(round(val_custom * (n_prod)))
    }
    return(val_custom)
  }
  
  # site default
  val_site <- get_default_site(config, site, key)
  if (!is.null(val_site) && !is.na(val_site)) {
    if (val_site < 1) {
      return(round(val_site * (n_prod)))
    }
    return(val_site)
  }
  
  # global default
  val_global <- get_default_global(config, key)
  if (!is.null(val_global) && !is.na(val_global)) {
    if (val_global < 1) {
      return(round(val_global * (n_prod)))
    }
    return(val_global)
  }
  return(NA)
}

get_sdv <- function(config, phase, cohort, site) {
  return(get_sdv_or_irr_value(config, phase, cohort, site, "sdv"))
}

get_irr <- function(config, phase, cohort, site) {
  return(get_sdv_or_irr_value(config, phase, cohort, site, "irr"))
}

now <- function(timeOnly = F, tz = "US/Pacific") {
  
  Sys.setenv(TZ=tz)
  
  if(timeOnly) {
    return(format(Sys.time(), "%H:%M:%S"))
  }
  
  return(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
}

get_synapse_folder_children <- function(synapse_id, 
                                        include_types=list("folder", "file", "table", "link", "entityview", "dockerrepo")) {
  
  ent <- as.list(synGetChildren(synapse_id, includeTypes = include_types))
  
  children <- c()
  
  if (length(ent) > 0) {
    for (i in 1:length(ent)) {
      children[ent[[i]]$name] <- ent[[i]]$id
    }
  }
  
  return(children)
}

#' Get a synapse ID by following a traditional file path from a root synapse folder entity.
#' 
#' @param synid_folder_root Synapse ID of the root folder
#' @param path Folder path starting in the first subfolder ending in desired folder and delimited with '/'
#' @return Synapse ID of the final subfolder in the path
#' @example get_folder_synid_from_path("syn12345", "first/second/final")
get_folder_synid_from_path <- function(synid_folder_root, path) {
  
  synid_folder_current <- synid_folder_root
  subfolders <- strsplit(path, split = "/")[[1]]
  
  for (i in 1:length(subfolders)) {
    synid_folder_children <- get_synapse_folder_children(synid_folder_current, 
                                                         include_types = list("folder"))
    
    if (!is.element(subfolders[i], names(synid_folder_children))) {
      return(NA)
    }
    
    synid_folder_current <- as.character(synid_folder_children[subfolders[i]])
  }
  
  return(synid_folder_current)
}

#' Get the Synapse ID of a named file entity given a file path and root Synapse ID. 
#' 
#'  @param synid_folder_root  Synapse ID of the root folder
#'  @param path Folder path starting in the first subfolder ending in desired 
#'  file and delimited with '/'
#'  @return Synapse ID of file entity
#'  @example get_file_synid_from_path("syn12345", "first/second/my_file.csv")
get_file_synid_from_path <- function(synid_folder_root, path) {
  
  path_part <- strsplit(path, split = "/")[[1]] 
  file_name <- tail(path_part, n = 1)
  path_abbrev <- paste0(path_part[1:(length(path_part) - 1)], collapse = "/")
  
  synid_folder_dest <- get_folder_synid_from_path(synid_folder_root, 
                                                  path_abbrev)
  
  synid_folder_children <- get_synapse_folder_children(synid_folder_dest, 
                                                       include_types = list("file"))
  
  if (!is.element(file_name, names(synid_folder_children))) {
    return(NA)
  }
  
  return(as.character(synid_folder_children[file_name]))
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


#' Extract and Combine Unique Sample IDs from a List
#'
#' This function takes a list of strings, splits each string by the ';' delimiter,
#' and combines all the resulting elements into a single list of unique sample IDs.
#'
#' @param sample_ids_list A list of strings where each string may contain delimited sample IDs.
#'
#' @return A character vector of unique sample IDs.
#'
#' @examples
#' \dontrun{
#' # Example list of sample IDs
#' sample_ids <- list("ID1;ID2;ID3", "ID4;ID5", "ID2;ID6")
#' all_sample_ids <- extract_sample_ids(sample_ids)
#' print(all_sample_ids)
#' }
#'
#' @export
extract_sample_ids <- function(sample_ids_list) {
  all_sample_ids <- list()
  for (sample_id_str in sample_ids_list) {
    split_ids <- strsplit(sample_id_str, ";")[[1]]
    all_sample_ids <- c(all_sample_ids, split_ids)
  }

  all_sample_ids <- unique(all_sample_ids)
  return(all_sample_ids)
}

#' Get Main GENIE clinical file using release version name
#'
#' @param release Release version name for a GENIE consortium release
#'                
#' @return A named list of Main GENIE clinical file Synapse ID.
get_main_genie_clinical_id <- function(release){
  query <- glue("SELECT id FROM syn17019650 WHERE name = '{release}'")
  release_folder_id <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  if (length(release_folder_id) == 0) {
  stop(glue("The release version is invalid."))
}
  release_files <- as.list(synGetChildren(release_folder_id, includeTypes = list("link")))
  for (release_file in release_files){
    if (release_file$name == "data_clinical.txt"){
      return(release_file$id)
    }
  }
  return(NULL)
}

#'  Mapping data for patient_characteristics
#' 
#' @param clinical A data frame of released clinical data for selected cases
#' @param existing_patients A data frame of available patient after case selection
#' @param ethnicity_mapping The NAACCR_ETHNICITY_MAPPING data frame
#' @param race_mapping The NAACCR_RACE_MAPPING data frame
#' @param sex_mapping The NAACCR_SEX_MAPPING data frame
#' @return A data frame with mapped code
remap_patient_characteristics <- function(clinical, existing_patients, ethnicity_mapping, race_mapping, sex_mapping){
  
  patient_df <- data.frame("record_id" = existing_patients)
  patient_df$redcap_repeat_instrument <- rep("")
  patient_df$redcap_repeat_instance <- rep("")
  
  patient_df$genie_patient_id <- patient_df$record_id
  patient_df$birth_year <- clinical$birth_year[match(patient_df$genie_patient_id, clinical$patient_id)]
  patient_df$naaccr_ethnicity_code <- clinical$ethnicity_detailed[match(patient_df$genie_patient_id, clinical$patient_id)]
  patient_df$naaccr_race_code_primary <- clinical$primary_race_detailed[match(patient_df$genie_patient_id, clinical$patient_id)]
  patient_df$naaccr_race_code_secondary <- clinical$secondary_race_detailed[match(patient_df$genie_patient_id, clinical$patient_id)]
  patient_df$naaccr_race_code_tertiary <- clinical$tertiary_race_detailed[match(patient_df$genie_patient_id, clinical$patient_id)]
  patient_df$naaccr_sex_code <- clinical$sex_detailed[match(patient_df$genie_patient_id, clinical$patient_id)]
  
  # mapping to code
  patient_df$naaccr_ethnicity_code <- ethnicity_mapping$CODE[match(patient_df$naaccr_ethnicity_code, ethnicity_mapping$DESCRIPTION)]
  patient_df$naaccr_race_code_primary <- race_mapping$CODE[match(patient_df$naaccr_race_code_primary, race_mapping$DESCRIPTION)]
  patient_df$naaccr_race_code_secondary <- race_mapping$CODE[match(patient_df$naaccr_race_code_secondary, race_mapping$DESCRIPTION)]
  patient_df$naaccr_race_code_tertiary <- race_mapping$CODE[match(patient_df$naaccr_race_code_tertiary, race_mapping$DESCRIPTION)]
  patient_df$naaccr_sex_code <- sex_mapping$CODE[match(patient_df$naaccr_sex_code,sex_mapping$DESCRIPTION)]

  return(patient_df)
}

#' Check for missing values in naaccr columns
#'
#' @param data The data frame to check against
#' @param columns The target columns
check_for_missing_values <- function(data, columns) {
  # filter out CHOP, PROV, JHU centers with known NAs
  data <- data[!grepl("CHOP|PROV|JHU", data$genie_patient_id), ]
  # Check for NA values or empty strings
  missingness_col <- c()
  for (col in columns) {
    if (any(is.na(data[[col]]) | data[[col]] == "" )){
      missingness_col <- c(col, missingness_col)
    }
  }
  if (length(missingness_col) > 0) {
    warning(paste0("Warning: Missing or empty values found in column(s): ", paste(missingness_col,collapse=", ")))
    }
}
