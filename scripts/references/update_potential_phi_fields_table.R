# Description: Check the Scope of Release for additional variables in need of
#   redaction that are not in the Sage internal reference table.
# Author: Haley Hunter-Zinck
# Date: 2022-03-24

# pre-setup  ---------------------------

library(optparse)

# user input ----------------------------

option_list <- list( 
  make_option(c("-f", "--synid_file_sor"), type = "character",
              help="Synapse ID of Scope of Release file (default: syn22294851)", default = "syn22294851"),
  make_option(c("-t", "--synid_table_red"), type = "character",
              help="Synapse ID of table listing variables to redact (default: syn23281483)", default = "syn23281483"),
  make_option(c("-a", "--auth"), type = "character",
              help="path to .synapseConfig or Synapse PAT (default: normal synapse login behavior)", default = NA)
)
opt <- parse_args(OptionParser(option_list=option_list))

# synapse
synid_file_sor <- opt$synid_file_sor
synid_table_red <- opt$synid_table_red
auth <- opt$auth

# setup ----------------------------

tic = as.double(Sys.time())

library(glue)
library(dplyr)
library(synapser)

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

#' Read contents of an Excel Spreadsheet stored on Synapse.
#' 
#' @param synapse_id Synapse ID of the spreadsheet
#' @param version Version of the file
#' @param sheet Number of the sheet of the spreadsheet
#' @param check.names Whether R should modify names if non-conforming
#' @return Matrix of data
#' @example 
#' get_synapse_entity_data_in_xlsx(synapse_id = "syn123345", sheet = 2)
get_synapse_entity_data_in_xlsx <- function(synapse_id, 
                                            version = NA,
                                            sheet = 1,
                                            check.names = F) {
  library(openxlsx)
  
  if (is.na(version)) {
    entity <- synGet(synapse_id)
  } else {
    entity <- synGet(synapse_id, version = version)
  }
  
  data <- read.xlsx(entity$path, check.names = check.names, sheet = sheet)
  
  return(data)
}

#' Remove leading and trailing whitespace from a string.
#' @param str String
#' @return String without leading or trailing whitespace
trim <- function(str) {
  front <- gsub(pattern = "^[[:space:]]+", replacement = "", x = str)
  back <- gsub(pattern = "[[:space:]]+$", replacement = "", x = front)
  
  return(back)
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

#' Gather list of variable and type corresponding to time interval suffixes in
#' the scope of release.
#' 
#' @param synid_file_sor Synapse ID of scope of release
#' @return DataFrame with two columns corresponding to the variable name and type.
get_sor_inf_to_redact <- function(synid_file_sor) {
  sor <- get_synapse_entity_data_in_xlsx(synid_file_sor, sheet = 2)
  
  # ends in _int, _days, _mos, _yrs
  inf_sor <- sor %>% 
    filter(grepl(pattern = "_int$", x = VARNAME) | 
             grepl(pattern = "_int_[1-5]$", x = VARNAME) |
             grepl(pattern = "_days$", x = VARNAME) | 
             grepl(pattern = "_mos$", x = VARNAME) | 
             grepl(pattern = "_yrs$", x = VARNAME) |
             grepl(pattern = "_age$", x = VARNAME) |
             grepl(pattern = "^age_", x = VARNAME) |
             grepl(pattern = "_age_$", x = VARNAME)) %>%
    mutate(VARNAME_TRIM = trim(VARNAME)) %>%
    select(VARNAME_TRIM, TYPE) %>%
    distinct() %>%
    rename(VARNAME = VARNAME_TRIM)
  
  return(inf_sor)
}

#' Get list of currently redacted variables.
#' 
#' @param synid_table_red Synapse ID of potential PHI fields.
#' @return Vector of variable names
get_var_currently_redacted <- function(synid_table_red) {
  der <- as.data.frame(synTableQuery(glue("SELECT * FROM {synid_table_red}")))
  var_red <- trim(unlist(der %>%
                           select(variable) %>%
                           distinct()))
  
  return(var_red)
}

#' Get matrix of variables and associated metadata for update to potential PHI
#' fields table.
#' 
#' @param var_add Vector of variable names to add
#' @param inf_sor DataFrame with two columns corresponding to the variable name and type.
#' @return Matrix with table update
get_red_table_update <-function(var_add, inf_sor) {
  
  mat_add = c()
  
  if (length(var_add)) {
    
    labels <- c("type","variable", "unit")
    
    # get info for update
    mat_add <- matrix(NA, nrow = length(var_add), ncol = length(labels), dimnames = list(c(), labels))
    mat_add[,"variable"] <- var_add
    mat_add[,"type"] <- tolower(inf_sor[match(var_add, inf_sor[,"VARNAME"]), "TYPE"])
    mat_add[grepl(pattern = "_int_[1-5]$", x = mat_add[,"variable"]),"unit"] <- "day"
    mat_add[grepl(pattern = "_int$", x = mat_add[,"variable"]),"unit"] <- "day"
    mat_add[grepl(pattern = "_days$", x = mat_add[,"variable"]),"unit"] <- "day"
    mat_add[grepl(pattern = "_mos$", x = mat_add[,"variable"]),"unit"] <- "month"
    mat_add[grepl(pattern = "_yrs$", x = mat_add[,"variable"]),"unit"] <- "year"
    mat_add[grepl(pattern = "_age_", x = mat_add[,"variable"]),"unit"] <- "year"
    mat_add[grepl(pattern = "_age$", x = mat_add[,"variable"]),"unit"] <- "year"
    mat_add[grepl(pattern = "^age_", x = mat_add[,"variable"]),"unit"] <- "year"
  } 
  
  return(mat_add)
}

#' Update Synapse table and take snapshot.
#' 
#' @param synid_table Synapse ID of table to update
#' @param df_update Data frame containing data with which to update the table.
#' @param comment Comment for table snapshot
#' @return Integer corresponding to new snapshot version or NA if table is not updated.
update_red_table <- function(synid_table, df_update, comment) {
  
  n_version = NA
  
  if (nrow(df_update)) {
    tbl <- synStore(Table(synid_table, df_update))
    n_version <- snapshot_synapse_table(table_id = synid_table, comment = comment)
  }
  
  return(n_version)
}

# Synpase login --------------------

status <- synLogin()

# main ----------------------------

# parameters
comment <- glue("Update according to SOR {synid_file_sor}.{synGet(synid_file_sor, downloadFile = F)$properties$versionNumber}")

# sor
inf_sor <- get_sor_inf_to_redact(synid_file_sor)
var_sor <- unlist(inf_sor$VARNAME)

# redaction table
var_red <- get_var_currently_redacted(synid_table_red)

# missing variables
var_add <- setdiff(var_sor, var_red)

# update table with missing variables
tbl_update <- get_red_table_update(var_add = var_add, 
                                   inf_sor = inf_sor)
n_version <- update_red_table(synid_table = synid_table_red, 
                              df_update = data.frame(tbl_update), 
                              comment = comment)

# close out ----------------------------

if (is.na(n_version)) {
  print(glue("No missing potential PHI variables detected.  Redaction table '{synGet(synid_table_red)$properties$name}' ({synid_table_red}) unchanged. "))
} else {
  print(glue("Detected {length(var_add)} missing potential PHI variables.  Redaction table '{synGet(synid_table_red)$properties$name}' ({synid_table_red}) updated to version {n_version}."))
}

toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
