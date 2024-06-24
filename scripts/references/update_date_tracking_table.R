# Description: Update the upload tracking table with new dates for a cohort.
# Author: Haley Hunter-Zinck
# Date: 2021-11-15

# pre-setup  ---------------------------

library(optparse)

waitifnot <- function(cond, msg) {
  if (!cond) {

    for (str in msg) {
      message(str)
    }
    message("Press control-C to exit and try again.")

    while(T) {}
  }
}

# user input ----------------------------

option_list <- list( 
  make_option(c("-c", "--cohort"), type = "character",
              help="BPC cohort"),
  make_option(c("-d", "--date"), type = "character",
              help="New current date for cohort"),
  make_option(c("-s", "--save_comment"), type = "character", 
              help="Save table snapshot to Synapse with supplied comment"),
  make_option(c("-a", "--synapse_auth"), type = "character", default = NA,
              help="Path to .synapseConfig file or Synapse PAT (default: normal synapse login behavior)")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(!is.null(opt$cohort) && !is.null(opt$date),
          msg = "Rscript template.R -h")

cohort <- opt$cohort
date <- opt$date
save_comment <- opt$save_comment
auth <- opt$synapse_auth

# setup ----------------------------

tic = as.double(Sys.time())

library(glue)
library(dplyr)
library(synapser)

# synapse
synid_table_tracking <- "syn25837005"

# functions ----------------------------

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

# read ----------------------------

query <- glue("SELECT * FROM {synid_table_tracking}")
tbl_old <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))

# main ----------------------------

tbl_new <- tbl_old

if (cohort %in% tbl_old$cohort){
  idx <- which(tbl_old$cohort == cohort)
  tbl_new[idx, "previous_date"] <- tbl_old[idx, "current_date"]
  tbl_new[idx, "current_date"] <- date
}else{
  tbl_new <- tbl_new %>% add_row(cohort=cohort, current_date=date)
}

# save ---------------------------------

file_outfile <- "upload_tracking.csv"
write.csv(tbl_new, file = file_outfile, row.names = F)

if (!is.null(save_comment)) {
  n_version <- create_new_table_version(table_id = synid_table_tracking, 
                           data = tbl_new, 
                           comment = save_comment)
  file.remove(file_outfile)
}

# close out ----------------------------

if (!is.null(save_comment)) {
  print(glue("New table version posted to '{synGet(synid_table_tracking)$properties$name}' ({synid_table_tracking}.{n_version})"))
} else {
  print(glue("New table version written locally to '{file_outfile}'"))
}

toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
