# Description: Create table for phase 1 BPC case selection counts as a reference.
# Author: Haley Hunter-Zinck
# Date: 2021-09-22

# setup ----------------------

tic = as.double(Sys.time())

library(optparse)
library(glue)
library(dplyr)
library(yaml)
library(synapser)

workdir <- "."
if (!file.exists("config.yaml")) {
  workdir <- "/usr/local/src/myscripts"
}
config <- read_yaml(glue("{workdir}/config.yaml"))
source(glue("{workdir}/shared_fxns.R"))

# user input --------------------------

option_list <- list( 
  make_option(c("-s", "--save_synapse"), action="store_true", default = FALSE, 
              help="Save updated counts on Synapse"),
  make_option(c("-c", "--comment"), type = "character",
              help="Comment for new table snapshot version"),
  make_option(c("-a", "--synapse_auth"), type = "character", default = "~/.synapseConfig", 
              help="Path to .synapseConfig file or Synapse PAT (default: '~/.synapseConfig')")
)
opt <- parse_args(OptionParser(option_list=option_list))

save_synapse <- opt$save_synapse
comment <- opt$comment
auth <- opt$synapse_auth

# functions ----------------------------

get_current_production_record_count <- function(synid_table_patient, cohort, site = NA) {
  
  if (is.na(site)) {
    query = glue("SELECT record_id FROM {synid_table_patient} WHERE cohort = '{cohort}'")
  } else {
    query = glue("SELECT record_id FROM {synid_table_patient} WHERE cohort = '{cohort}' AND redcap_data_access_group = '{site}'")
  }
  record_ids <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  
  return(nrow(record_ids))
}

#' Create a Synapse table snapshot version with comment.
#' 
#' @param table_id Synapse ID of a table entity
#' @param comment Message to annotate the new table version
#' @return snapshot version number
#' @example 
#' create_synapse_table_snapshot("syn12345", comment = "my new snapshot")
create_synapse_table_snapshot <- function(table_id, comment) {
  res <- synRestPOST(glue("/entity/{table_id}/table/snapshot"), 
                     body = glue("{'snapshotComment':'{{comment}}'}", 
                                 .open = "{{", 
                                 .close = "}}"))
  
  return(res$snapshotVersionNumber)
}

#' Clear all rows from a Synapse table.
#' 
#' @param table_id Synapse ID of a table
#' @return Number of rows deleted
clear_synapse_table <- function(table_id) {
  
  res <- as.data.frame(synTableQuery(glue("SELECT * FROM {table_id}")))
  tbl <- Table(schema = synGet(table_id), values = res)
  synDelete(tbl)
  
  return(nrow(res))
}

#' Update rows of a Synapse table with new data.
#' 
#' @param table_id Synapse ID of a table
#' @param data Data frame of new data
#' @return Number of rows added
update_synapse_table <- function(table_id, data) {
  
  entity <- synGet(table_id)
  project_id <- entity$properties$parentId
  table_name <- entity$properties$name
  table_object <- synBuildTable(table_name, project_id, data)
  synStore(table_object)
  
  return(nrow(data))
}

#' Clear all data from a table, replace with new data, and 
#' create a new snapshot version.
#' 
#' @param table_id Synapse ID of the table
#' @param data Data frame of new data
#' @param comment Comment string to include with the new snapshot version.
#' @return New snapshot version number
create_new_table_version <- function(table_id, data, comment = "") {
  n_rm <- clear_synapse_table(table_id)
  n_add <- update_synapse_table(table_id, data)
  n_version <- create_synapse_table_snapshot(table_id, comment)
  return(n_version)
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

# main ----------------------------

labels <- c("cohort", "site", "phase", "current_cases", "target_cases", "adjusted_cases", "pressure", "sdv", "irr")
final <- matrix(NA, nrow = 0, ncol = length(labels), dimnames = list(c(), labels))

# gather production counts
for (phase in names(config$phase)) {
  for (cohort in names(config$phase[[phase]]$cohort)) {
    for (site in names(config$phase[[phase]]$cohort[[cohort]]$site)) {
      
      n_current <- get_current_production_record_count(synid_table_patient = config$synapse$bpc_patient$id,
                                                       cohort = cohort, 
                                                       site = site)
      n_target <- get_production(config, phase, cohort, site)
      n_adjust <- get_adjusted(config, phase, cohort, site)
      n_pressure <- get_pressure(config, phase, cohort, site)
      n_sdv <- get_sdv(config, phase, cohort, site)
      n_irr <- get_irr(config, phase, cohort, site)
      
      if (phase == "2") {
        n_current = 0
      }
      
      final <- rbind(final, c(cohort, site, phase, n_current, n_target, n_adjust, n_pressure, n_sdv, n_irr))
    }
  }
}

# sort
final <- as.data.frame(final) %>% 
  arrange(phase, cohort, site)

# save ------------------------

if (save_synapse) {
  n_version <- create_new_table_version(table_id = config$synapse$case_selection$id, 
                           data = final, 
                           comment = comment)
} else {
  write.csv(final, row.names = F, file = "case_selection_counts.csv")
}

# close out ----------------------------

if (save_synapse) {
  print(glue("Table saved to Synapse as 'Case Selection Counts' ({config$synapse$bpc_internal$id}), version {n_version}"))
} else {
  print(glue("Table saved locally to 'case_selection_counts.csv'"))
}
toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
