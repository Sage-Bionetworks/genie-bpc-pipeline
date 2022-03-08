# Description: Re-write of the qa_unmasked_drugs.Rmd report as an R script, adding
#   column for potentially investigative drugs with reference to the HemOnc ontology.
# Author: Haley Hunter-Zinck
# Date: August 23, 2021

# pre-setup -----------------------------------------

library(optparse)

#' Function to wait indefinitely upon a certain condition.
#' 
#' @param cond Boolean value, usually from an evaluated condition
#' @param msg A string to print upon condition being TRUE
#' @return NA
waitifnot <- function(cond, msg = "") {
  if (!cond) {
    
    for (str in msg) {
      message(str)
    }
    message("Press control-C to exit and try again.")
    
    while(T) {}
  }
}

# user input -----------------------------------------

option_list <- list( 
  make_option(c("-c", "--cohort"), type="character", 
              help="Cohort on which to run analysis"),
  make_option(c("-s", "--site"), type="character", 
              help="Site on which to run analysis"),
  make_option(c("-d", "--date"), type="character", 
              help="Upload date for folder labels"),
  make_option(c("-a", "--synapse_auth"), type = "character", default = "~/.synapseConfig", 
              help="Path to .synapseConfig file or Synapse PAT (default: '~/.synapseConfig')")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(!is.null(opt$cohort) && !is.null(opt$site) && !is.null(opt$date),
          msg = c("Usage: Rscript qa_unmasked_drugs_hemonc.R -h"))

# user arguments
cohort <- opt$cohort
site <- opt$site
date <- opt$date
auth <- opt$synapse_auth

# setup -----------------------------------------

tic <- as.double(Sys.time())

library(synapser)
library(glue)
library(yaml)

# synapse
config <- read_yaml("config.yaml")
synid_table_cur <- config$synapse$curation$id      
synid_table_drugs <- config$synapse$drugs$id  
synid_table_pt <- config$synapse$patient$id    
synid_table_rel <- config$synapse$relationship$id       
synid_table_con <- config$synapse$concept$id        
synid_table_map <- config$synapse$map$id     
synid_folder_qc <- config$synapse$qc$id  

# functions ---------------------------------------------

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

# get Synapse ID of the most recently reviewed drug report
get_synid_drug_signoff_file <- function(synid_root, cohort, site, date, map_cohort) {
  
  # get cohort subfolder synapse id
  synid_children_cohort <- get_synapse_folder_children(synid_root)
  synid_folder_cohort <- as.character(synid_children_cohort[map_cohort[[cohort]]])
  if (is.na(synid_folder_cohort)) {
    return(NA)
  }
  
  # get synapse ID of folder labeled "qa_drug_review_qa_manager_signoff"
  synid_children_signoff <- get_synapse_folder_children(synid_folder_cohort)
  synid_folder_signoff <- as.character(synid_children_signoff["qa_drug_review_qa_manager_signoff"])
  
  # get synapse ID of folder for most recent previous review
  synid_children_date <- get_synapse_folder_children(synid_folder_signoff)
  if (!length(synid_children_date)) {
    return(NA)
  }
  
  synid_children_date_reports <- synid_children_date[grep("Reports", names(synid_children_date))]
  synid_children_date_prev <- synid_children_date_reports[which(names(synid_children_date_reports) < date)]
  synid_folder_date <- as.character(tail(sort(synid_children_date_prev), 1))
  
  # get synpase ID of file by approximate file name match
  synid_children_report <- get_synapse_folder_children(synid_folder_date)
  idx = grep(pattern = glue("unmasked_drugs_review_{site}_{cohort}"), 
             x = names(synid_children_report))
  synid_file_report <- as.character(synid_children_report[idx])
  
  if (length(synid_file_report)) {
    return(synid_file_report)
  }
  
  return(NA)
}

get_latest_curation_date <- function(synapse_id, cohort) {
  query <- glue("SELECT MAX(curation_dt) FROM {synapse_id} WHERE cohort = '{cohort}'")
  dt <- as.character(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))[1])
  return(dt)
}

trim <- function(str) {
  front <- gsub(pattern = "^[[:space:]]+", replacement = "", x = str)
  back <- gsub(pattern = "[[:space:]]+$", replacement = "", x = front)
  
  return(back)
}

shorten_drug_names <- function(full_drug_name) {
  return(trim(unlist(lapply(strsplit(x = full_drug_name, split = "(", fixed = T), head, n = 1))))
}

pivot_drugs <- function(synapse_id, cohort, site, drug_numbers = c(1:5)) {
  
  # read in drug data 
  raw_drugs <- as.data.frame(synTableQuery(glue("SELECT * FROM {synid_table_drugs} 
                                                WHERE cohort = '{cohort}'
                                                  AND redcap_data_access_group = '{site}'")))
  
  # one row per drug
  column_labels <- c("record_id", "regimen_number", 
                     "drugs_ct_yn", "drugs_drug", "drugs_other", "days_on_drug", 
                     "drug_number", "drug_name")
  row_drugs <- c()
  for (drug_number in drug_numbers) {
    
    column_drug <- glue("drugs_drug_{drug_number}")
    column_other <- glue("drugs_drug_oth_{drug_number}")
    column_start <- glue("drugs_startdt_int_{drug_number}")
    column_end <- glue("drugs_enddt_int_{drug_number}")
    column_regimen <- "redcap_repeat_instance"
    
    # map names
    drug_name_mod <- raw_drugs[,column_other]
    idx_na_other <- which(is.na(raw_drugs[,column_other]))
    drug_name_mod[idx_na_other] <- shorten_drug_names(raw_drugs[idx_na_other, column_drug])
    days_on_drug <- as.double(raw_drugs[,column_end]) - as.double(raw_drugs[,column_start]) + 1

    reg_drugs <- as.matrix(cbind(raw_drugs[,c("record_id", 
                                              column_regimen,
                                              "drugs_ct_yn",
                                              column_drug, 
                                              column_other)],
                                 days_on_drug,
                                 drug_number, drug_name_mod))
    
    idx <- which(is.na(reg_drugs[,column_drug]))
    if (length(idx) > 0) {
      row_drugs <- rbind(row_drugs, reg_drugs[-idx,])
    } else {
      row_drugs <- rbind(row_drugs, reg_drugs)
    }
  }
  
  # format
  colnames(row_drugs) <- column_labels
  
  return(row_drugs)
}

sort_and_collapse <- function(x, delim = ", ") {
  return(paste0(sort(x), collapse = delim))
}

get_drugs_by_regimen <- function(df_drug) {
  res <- aggregate(drug_name ~  record_id + regimen_number + drugs_ct_yn, 
                   data = df_drug, 
                   FUN = sort_and_collapse)
  colnames(res)[which(colnames(res) == "drug_name")] = "drugs_in_regimen"
  return(res)
}

consolidate_fda_status <- function(statuses) {
  
  if (length(which(statuses == "unapproved"))) {
    return("unapproved")
  } 
  
  if (length(which(statuses == "unknown"))) {
    return("unknown")
  } 
  
  if (length(which(statuses == "masked"))) {
    return("masked")
  }
  
  return("approved")
}

get_fda_status_by_regimen <- function(df_drug) {
  res <- aggregate(fda_status ~  record_id + regimen_number + drugs_ct_yn, 
                   data = df_drug, 
                   FUN = consolidate_fda_status)
  
  return(res)
}

get_redacted_patients <- function(synapse_id, cohort, site) {
  query <- glue("SELECT record_id 
                FROM {synapse_id} 
                WHERE redacted = 'Yes'
                  AND cohort = '{cohort}' 
                  AND redcap_data_access_group = '{site}'")
  res <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  return(as.character(unlist(res)))
}

get_previously_reviewed_regimens <- function(synapse_id, site,
                                             values_not_reviewed = c(NA, "pending")) {
  
  data <- read.csv(synGet(synapse_id)$path, stringsAsFactors = F, na.strings = "", check.names = F)
  idx_id <- which(is.element(colnames(data), c("record_id", "Record ID")))
  idx_reg <- which(is.element(colnames(data), c("Regimen Number", "regimen_number")))
  idx_rev <- which(is.element(colnames(data), c("QA manager sign off", "qa_manager_signoff")))
  idx_site <- grep(pattern = site, x = data[,idx_id])
  idx_reviewed <- which(!is.element(tolower(data[,idx_rev]), values_not_reviewed))
  
  res <- data[intersect(idx_site,idx_reviewed),c(idx_id, idx_reg, idx_rev)]
  colnames(res) <- c("record_id", "regimen_number", "previous_signoff")
  return(res)
}

regularize_drug_names <- function(drug_names_raw) {
  # lower case, remove white space, replace punctuation with white space
  mod <- tolower(drug_names_raw)
  mod <- gsub(pattern = "[[:space:]]", replacement = "", x = mod)
  mod <- gsub(pattern = "[[:punct:]]", replacement = " ", x = mod)
  return(mod)
}

#' Get the FDA approval status of a BPC drug according to the HemOnc ontology.
#' 
#' @param bpc_drug_name Single string representing a BPC drug name
#' @param date index date for which to compare date of FDA approval
#' @param synid_table_hem_map Synapse ID of table containing BPC to HemOnc drug name mappings
#' @param synid_table_hem_rel Synapse ID of table containing HemOnc ontology relationships
#' @param synid_table_hem_rel Synapse ID of table containing HemOnc concepts
#' @return vector representing FDA approval status of each drug according to the user
#' supplied reference date.  Potential values are "approved", "unapproved", "unknown".
#' @example 
#' get_fda_approval_status("bpc_drug_1)
get_fda_approval_status_single <- function(bpc_drug_name_raw, date = Sys.Date(),
                                           synid_table_hem_map, 
                                           synid_table_hem_rel,
                                           synid_table_hem_con) {
  
  if (bpc_drug_name_raw == "Investigational Drug") {
    return("masked")
  }
  
  # get BPC to hemonc drug name mapping
  query <- glue("SELECT HemOnc_code, BPC FROM {synid_table_hem_map} WHERE HemOnc_code NOT IN ('NA', 'Other')")
  map <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  map[,"BPC"] <- regularize_drug_names(map[,"BPC"])
  bpc_drug_name <- regularize_drug_names(bpc_drug_name_raw)
  
  # get hemonc code, if mapping found
  drug_concept_id <- ""
  idx <- which(map[,"BPC"] == bpc_drug_name)
  if(length(idx) == 0) {
    return("unknown")
  }
  drug_concept_id <- map[idx,"HemOnc_code"]
  
  fda_year_concept_id <- as.double(unlist(as.data.frame(synTableQuery(glue("SELECT concept_code_2 
                                        FROM {synid_table_hem_rel}
                                        WHERE concept_code_1 = {drug_concept_id}
                                          AND relationship_id = 'Was FDA approved yr'"), 
                                                                  includeRowIdAndRowVersion = F))))
  
  # if no FDA approval recorded, unapproved
  fda_year_concept_id <- fda_year_concept_id[which(!is.na(fda_year_concept_id))]
  if (!length(fda_year_concept_id)) {
    return("unapproved")
  }
  
  fda_year_concept_id_list <- paste0("'", paste0(fda_year_concept_id, collapse = "','"), "'")
  fda_year <- as.character(unlist(as.data.frame(synTableQuery(glue("SELECT MAX(concept_name)
                                      FROM {synid_table_hem_con}
                                       WHERE concept_code IN ({fda_year_concept_id_list})"), 
                                                       includeRowIdAndRowVersion = F))))
  
  # if approval year is before or equal to user supplied date's year, approved
  date_year <- format(as.Date(date), "%Y")
  if (fda_year <= date_year) {
    return("approved")
  }
  
  # if approval year is after user supplied date's year, unapproved
  if(fda_year > date_year) {
    return("unapproved")
  }
}

#' Get the FDA approval status of a BPC drug according to the HemOnc ontology
#' for multiple drug names.
#' 
#' @param bpc_drug_names Vector of string representing BPC drug names
#' @param date index date for which to compare date of FDA approval
#' @param synid_table_hem_map Synapse ID of table containing BPC to HemOnc drug name mappings
#' @param synid_table_hem_rel Synapse ID of table containing HemOnc ontology relationships
#' @param synid_table_hem_rel Synapse ID of table containing HemOnc concepts
#' @return vector representing FDA approval status of each drug according to the user
#' supplied reference date. Potential values are "approved", "unapproved", "unknown".
get_fda_approval_status <- function(bpc_drug_names, date = Sys.Date(),
                                    synid_table_hem_map, 
                                    synid_table_hem_rel,
                                    synid_table_hem_con,
                                    debug = F) {
  
  fda_status <- c()
  
  for (bpc_drug_name in bpc_drug_names) {
    
    if(debug) {
      print(bpc_drug_name)
    }
    
    fda_status[bpc_drug_name] <- get_fda_approval_status_single(bpc_drug_name = bpc_drug_name, 
                                   date = date,
                                   synid_table_hem_map = synid_table_hem_map, 
                                   synid_table_hem_rel = synid_table_hem_rel,
                                   synid_table_hem_con = synid_table_hem_con)
    
  }
  
  return(fda_status)
}

#' Extract personal access token from .synapseConfig
#' located at a custom path. 
#' 
#' @param path Path to .synapseConfig
#' @return personal acccess token
get_auth_token <- function(path) {
  
  lines <- scan(path, what = "character", sep = "\t", quiet = T)
  line <- grep(pattern = "^authtoken = ", x = lines, value = T)
  
  token <- strsplit(line, split = ' ')[[1]][3]
  return(token)
}

#' Override of synapser::synLogin() function to accept 
#' custom path to .synapseConfig file or personal authentication
#' token.  If no arguments are supplied, performs standard synLogin().
#' 
#' @param auth full path to .synapseConfig file or authentication token
#' @param silent verbosity control on login
#' @return TRUE for successful login; F otherwise
#' Override of synapser::synLogin() function to accept 
#' custom path to .synapseConfig file or personal authentication
#' token.  If no arguments are supplied, performs standard synLogin().
#' 
#' @param auth full path to .synapseConfig file or authentication token
#' @param silent verbosity control on login
#' @return TRUE for successful login; F otherwise
synLogin <- function(auth = NA, silent = T) {
  
  # default synLogin behavior
  if (is.na(auth)) {
    syn <- synapser::synLogin(silent = silent)
    return(T)
  }
  
  token = auth
  
  # extract token from .synapseConfig
  if (grepl(x = auth, pattern = "\\.synapseConfig$")) {
    token = get_auth_token(auth)
    
    if (is.na(token)) {
      return(F)
    }
  }
  
  # login
  syn <- tryCatch({
    synapser::synLogin(authToken = token, silent = silent)
  }, error = function(cond) {
    return(F)
  })
  
  if (is.null(syn)) {
    return(T)
  }
  return(syn)
}

# synapse login -------------------

login_status <- synLogin(auth = auth)

# query and compute -------------------------------------------------

# get latest curation date
date_curation <- get_latest_curation_date(synapse_id = synid_table_cur, cohort = cohort)

# patient data
record_id_redacted <- get_redacted_patients(synapse_id = synid_table_pt, cohort = cohort, site = site)

# extract formatted drug data
drug_ind <- pivot_drugs(synapse_id = synid_table_drugs, cohort = cohort, site = site)
drug_reg_name <- get_drugs_by_regimen(data.frame(drug_ind))

# fda approval status according to hemonc
fda_status <- get_fda_approval_status(bpc_drug_names = unique(drug_ind[,"drugs_drug"]), 
                                      date = date_curation, 
                                      synid_table_hem_map = synid_table_map, 
                                      synid_table_hem_rel = synid_table_rel,
                                      synid_table_hem_con = synid_table_con)
drug_ind_fda_status <- as.character(fda_status[drug_ind[,"drugs_drug"]])
drug_reg_fda <- get_fda_status_by_regimen(data.frame(drug_ind, fda_status =  drug_ind_fda_status))

# consolidate all regimen information
drug_reg <- data.frame(drug_reg_name, fda_status = drug_reg_fda[,"fda_status"])

# previously reviewed
regimen_reviewed <- drug_reg[,c("record_id","regimen_number")]
synid_drug_signoff <- get_synid_drug_signoff_file(synid_root = synid_folder_qc, 
                                             cohort = cohort, 
                                             site = site, 
                                             date = date,
                                             map_cohort = config$map$cohort_name)
if(!is.na(synid_drug_signoff)) {
  regimen_reviewed <- get_previously_reviewed_regimens(synapse_id = synid_drug_signoff, 
                                      site = site)
}


# keys
key_ind <- apply(drug_ind[,c("record_id","regimen_number")], 1, paste0, collapse = "_")
key_regimen <- apply(drug_reg[,c("record_id","regimen_number")], 1, paste0, collapse = "_")
key_reviewed <- apply(regimen_reviewed[,c("record_id","regimen_number")], 1, paste0, collapse = "_")

# get flags for regimens -----------------------------------------------

# flag 1: investigational drugs with duration == 1 day
flag_01 <- rep("ok", nrow(drug_reg))
days <- as.double(unlist(drug_ind[,"days_on_drug"]))
idx_flag_01_ind <- which((is.na(days) | days != 1) & drug_ind[,"drug_name"] == "Investigational Drug")
if(length(idx_flag_01_ind)) {
  flag_01[match(key_ind[idx_flag_01_ind], key_regimen)] <- "review"
}

# flag 2: already reviewed notation
flag_02 <- rep("", nrow(drug_reg))
if(!is.na(synid_drug_signoff)) {
  idx_match <- match(key_regimen, key_reviewed)
  flag_02[which(!is.na(idx_match))] <- regimen_reviewed[idx_match[which(!is.na(idx_match))], "previous_signoff"]
}

# flag 3: >89 patients
flag_03 <- is.element(as.character(unlist(drug_reg["record_id"])), record_id_redacted)

# flag 4: investigational drug has other filled in
flag_04 <- rep("ok", nrow(drug_reg))
idx_flag_04_ind <- which(drug_ind[,"drugs_drug"] == "Investigational Drug" 
                         & !is.na(drug_ind[,"drugs_other"]))
if(length(idx_flag_04_ind)) {
  flag_04[match(key_ind[idx_flag_04_ind], key_regimen)] <- "review"
}

# consolidate flags
flags <- cbind(flag_01, flag_04, flag_03, flag_02)
colnames(flags) <- c("investigational_duration","investigational_other","age_greater_89","previous_signoff")

# export -------------------------------------------------

# write locally
local_csv <- glue("unmasked_drugs_review_{site}_{cohort}_{date}.csv")
to_write <- data.frame(cohort, drug_reg, flags, "qa_manager_signoff" = flags[,"previous_signoff"])
write.csv(to_write, file = local_csv, row.names = F)

# close out ---------------------------------------------

toc <- as.double(Sys.time())
print(glue("Output written locally to file '{local_csv}'."))
print(glue("Runtime: {round(toc - tic)} s"))
