# Description: Create a Synapse table with GENIE BPC
#   case selection criteria.
# Author: Haley Hunter-Zinck
# Date: 2022-05-25

# setup ----------------------------

tic = as.double(Sys.time())

library(optparse)
library(glue)
library(dplyr)
library(yaml)
library(synapser)

# user input ----------------------------

option_list <- list( 
  make_option(c("-s", "--save_synapse"), action="store_true", default = FALSE, 
              help="Save updated counts on Synapse (default: FALSE)"),
  make_option(c("-c", "--comment"), type = "character", default = "update to case selection criteria",
              help="Comment for new table snapshot version (default: 'update to case selection criteria')"),
  make_option(c("-v", "--verbose"), action="store_true", default = FALSE, 
              help="Output script messages to the user (default: FALSE)"),
  make_option(c("-a", "--auth"), 
              type = "character",
              default = NA,
              help="Synapse personal access token or path to .synapseConfig (default: normal synapse login behavior)")
)
opt <- parse_args(OptionParser(option_list=option_list))

save_synapse <- opt$save_synapse
comment <- opt$comment
verbose <- opt$verbose
auth <- opt$auth

# parameters
file_config <- "config.yaml"
file_output <- "bpc_case_selection_criteria.csv"

# functions ----------------------------

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
synLogin <- function(auth = NA, silent = T) {
  
  secret <- Sys.getenv("SCHEDULED_JOB_SECRETS")
  if (secret != "") {
    # Synapse token stored as secret in json string
    syn = synapser::synLogin(silent = T, authToken = fromJSON(secret)$SYNAPSE_AUTH_TOKEN)
  } else if (auth == "~/.synapseConfig" || is.na(auth)) {
    # default Synapse behavior
    syn <- synapser::synLogin(silent = silent)
  } else {
    
    # in case pat passed directly
    token <- auth
    
    # extract token from custom path to .synapseConfig
    if (grepl(x = auth, pattern = "\\.synapseConfig$")) {
      token = get_auth_token(auth)
      
      if (is.na(token)) {
        return(F)
      }
    }
    
    # login with token
    syn <- tryCatch({
      synapser::synLogin(authToken = token, silent = silent)
    }, error = function(cond) {
      return(F)
    })
  }
  
  # NULL returned indicates successful login
  if (is.null(syn)) {
    return(T)
  }
  return(F)
}

#' Get phases represented in the case selection configuration file.
#' 
#' @param config configuration file object read from yaml
#' @return vector of strings
get_phases <- function(config) {
  phases <- grep(x = names(config$phase), pattern = "[0-9]+_additional$", invert = T, value = T) 
  return(sort(phases))
}

#' Get cohorts represented in the case selection configuration file
#' for a particular phase.
#' 
#' @param config configuration file object read from yaml
#' @param phase string representing phase code
#' @return vector of strings
get_cohorts <- function(config, phase) {
  cohorts <- names(config$phase[[phase]]$cohort)
  return(sort(cohorts))
}

#' Get sites represented in the case selection configuration file
#' for a particular phase and cohort.
#' 
#' @param config configuration file object read from yaml
#' @param phase string representing phase code
#' @param cohort string representing cohort code
#' @return vector of strings
get_sites <- function(config, phase, cohort) {
  sites <- names(config$phase[[phase]]$cohort[[cohort]]$site)
  return(sort(sites))
}

#' Get minimum sequencing date represented in the case selection configuration file.
#' 
#' @param config configuration file object read from yaml
#' @param phase string representing phase code
#' @param cohort string representing cohort code
#' @param site string representing site code
#' @return string representing date like '%b-%Y'
get_min_seq_date <- function(config, phase, cohort, site) {
  if (!is.null(config$phase[[phase]]$cohort[[cohort]]$site[[site]]$date)) {
    return(config$phase[[phase]]$cohort[[cohort]]$site[[site]]$date$seq_min)
  }
  
  return(config$phase[[phase]]$cohort[[cohort]]$date$seq_min)
}

#' Get maximum sequencing date represented in the case selection configuration file.
#' 
#' @param config configuration file object read from yaml
#' @param phase string representing phase code
#' @param cohort string representing cohort code
#' @param site string representing site code
#' @return string representing date like '%b-%Y'
get_max_seq_date <- function(config, phase, cohort, site) {
  if (!is.null(config$phase[[phase]]$cohort[[cohort]]$site[[site]]$date)) {
    return(config$phase[[phase]]$cohort[[cohort]]$site[[site]]$date$seq_max)
  }
  
  return(config$phase[[phase]]$cohort[[cohort]]$date$seq_max)
}

#' Get allowed oncotree codes represented in the case selection configuration file
#' for a particular phase and cohort.
#' 
#' @param config configuration file object read from yaml
#' @param phase string representing phase code
#' @param cohort string representing cohort code
#' @return vector of strings
get_oncotree_codes <- function(config, phase, cohort, delim = ';') {
  oncotree_vec <- config$phase[[phase]]$cohort[[cohort]]$oncotree$allowed_codes
  
  oncotree_str <- paste0(oncotree_vec, collapse = delim)
  return(oncotree_str)
}

#' Get target count represented in the case selection configuration file.
#' 
#' @param config configuration file object read from yaml
#' @param phase string representing phase code
#' @param cohort string representing cohort code
#' @param site string representing site code
#' @return integer
get_target_count <- function(config, phase, cohort, site) {
  target <- config$phase[[phase]]$cohort[[cohort]]$site[[site]]$production
  return(as.integer(target))
}

#' Create a Synapse table snapshot version with comment.
#' 
#' @param table_id Synapse ID of a table entity
#' @param comment Message to annotate the new table version
#' @return snapshot version number
#' @example 
#' create_synapse_table_snapshot("syn12345", comment = "my new snapshot")
snapshot_synapse_table <- function(table_id, comment) {
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
create_synapse_table_version <- function(table_id, data, comment = "", append = T) {
  
  if (!append) {
    n_rm <- clear_synapse_table(table_id)
  }
  n_add <- update_synapse_table(table_id, data)
  n_version <- snapshot_synapse_table(table_id, comment)
  return(n_version)
}

# synapse login --------------------

status <- synLogin(auth = auth)

# read ----------------------------

# configuration file
config <- read_yaml(file_config)

# main ----------------------------

# storage
labels <- c("phase", "cohort", "site", "min_seq_date", "max_seq_date", "oncotree", "target")
tab <- matrix(NA, nrow = 0, ncol = length(labels), dimnames = list(c(), labels))

if (verbose) {
  print(glue("Gathering case selection criteria from configuration file..."))
}

phases <- get_phases(config)
for (phase in phases) {
  cohorts <- get_cohorts(config, phase)
  for (cohort in cohorts) {
    sites <- get_sites(config, phase, cohort)
    for (site in sites) {
      min_seq_date <- get_min_seq_date(config, phase, cohort, site)
      max_seq_date <- get_max_seq_date(config, phase, cohort, site) 
      oncotree <- get_oncotree_codes(config, phase, cohort)
      target <- get_target_count(config, phase, cohort, site)
      
      row <- c(phase, cohort, site, min_seq_date, max_seq_date, oncotree, target)
      tab <- rbind(tab, row)
    }
  }
}

if (save_synapse) {
  
  if (verbose) {
    print(glue("Update case selection criteria table ({synid_table_output})..."))
  }
  n_version <- create_synapse_table_version(table_id = config$synapse$selection_criteria$id, 
                                            data = as.data.frame(tab), 
                                            comment = comment, 
                                            append = F)
  if (verbose) {
    print(glue("Updated to version {n_version}..."))
  }
} else {
  if (verbose) {
    print(glue("Writing case selection criteria to local file ({file_output})..."))
  }
  write.csv(tab, file = file_output, row.names = F)
}

# close out ----------------------------

toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
