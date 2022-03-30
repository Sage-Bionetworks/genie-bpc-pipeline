# Description: Update the cbio mapping Synapse Table using the CSV file provided by cBioPortal.  
#   Create a table snapshot with a user-supplied comment or an automatically generated comment
#   referencing the modified date and version of the CSV file entity.  
# Usage: Rscript update_cbio_mapping.R -h
# Authors: Xindi Guo, Haley Hunter-Zinck

# setup -----------------------------

tic = as.double(Sys.time())

# set up
library(glue)
library(dplyr)
library(optparse)
library(synapser)

# cli ---------------------

option_list <- list(
  make_option(c("-s", "--save_to_synapse"), 
              action="store_true", 
              default = FALSE,
              help="Save mapping to Synapse table and delete local output file"),
  make_option(c("-c", "--comment"), 
              type = "character", 
              default = NA,
              help="Comment for table snapshot if saving to synapse (optional)"),
  make_option(c("-a", "--auth"), 
              type = "character",
              default = NA,
              help="Synapse personal access token or path to .synapseConfig (default: normal synapse login behavior)"),
  make_option(c("-v", "--verbose"), 
              action="store_true", 
              default = FALSE,
              help="Print script progress to the user")
)
opt <- parse_args(OptionParser(option_list=option_list))
save_to_synapse <- opt$save_to_synapse
comment <- opt$comment
auth <- opt$auth
verbose <- opt$verbose

if (verbose) {
  print(glue("Parameters: "))
  print(glue("- save on synapse:\t{save_to_synapse}"))
  print(glue("- comment:\t\t'{if (is.na(comment)) 'auto-generated' else comment}'"))
  print(glue("- verbose:\t\t{verbose}"))
}

# parameters ------------------------

file_id <- "syn25585554"
tbl_id <- "syn25712693"
cohorts <- c("NSCLC","CRC","BrCa","PANC","Prostate","BLADDER")
outfile <- "cbio_mapping_table_update.csv"

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

#' Return current time as a string.
#' 
#' @param timeOnly If TRUE, return only time; otherwise return date and time
#' @param tz Time Zone
#' @return Time stamp as string
#' @example 
#' now(timeOnly = T)
now <- function(timeOnly = F, tz = "US/Pacific") {
  
  Sys.setenv(TZ=tz)
  
  if(timeOnly) {
    return(format(Sys.time(), "%H:%M:%S"))
  }
  
  return(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
}

# synapse login -------------------------

status <- synLogin(auth = auth)

# read ----------------------------------

if (verbose) {
  print(glue("{now(timeOnly = T)}: reading mapping file CSV..."))
}

# download the file and get the column names
entity <- synGet(file_id)
mapping_file <- read.csv(entity$path)
columns <- as.list(synGetColumns(tbl_id))
col_names <- sapply(columns, function(x){x$name})

# mapping -------------------------------

# comment for table snapshot
if (is.na(comment)) {
  utc_mod <- as.POSIXct(entity$properties$modifiedOn)
  pt_mod <- as.POSIXct(utc_mod, tz="America/Los_Angeles", usetz=TRUE)
  comment <- glue("mapping file update from {format(pt_mod, format='%Y-%m-%d')} PT ({file_id}.{entity$properties$versionNumber})")
}

if (verbose) {
  print(glue("{now(timeOnly = T)}: formatting mapping information..."))
}

# make adjustment to match the table schema
mapping_file <- mapping_file %>%
  dplyr::rename(BrCa=BRCA, Prostate=PROSTATE, inclusion_criteria=inclusion.criteria) %>%
  mutate_at(all_of(cohorts),list(~recode(.,"Y"=TRUE,"N"=FALSE, "TBD"=FALSE))) %>%
  mutate(data_type=tolower(data_type)) %>%
  mutate(data_type=recode(data_type,"tumor_registry"="curated"))

# write ------------------------------------

# wipe the Synapse Table
if (save_to_synapse) {
  
  if (verbose) {
    print(glue("{now(timeOnly = T)}: updating Synapse table '{synGet(tbl_id)$properties$name}' ({tbl_id}) with snapshot..."))
  }
  
  query_for_deletion <- synTableQuery(sprintf("select * from %s", tbl_id))
  deleted <- synDelete(query_for_deletion)
  
  # update with mapping table
  synStore(Table(tbl_id, mapping_file))
  res <- synRestPOST(glue("/entity/{tbl_id}/table/snapshot"), 
                     body = glue("{'snapshotComment':'{{comment}}'}", 
                                 .open = "{{", 
                                 .close = "}}"))
  
  if (verbose) {
    print(glue("{now(timeOnly = T)}: updated table {synGet(tbl_id)$properties$name} ({tbl_id}) to version {res$snapshotVersionNumber} with comment '{comment}'"))
  }
} else {
  write.csv(mapping_file, row.names = F, file = outfile)
  
  if (verbose) {
    print(glue("{now(timeOnly = T)}: table update written to '{getwd()}/{outfile}'"))
  }
}

# close out ---------------------------------

toc <- as.double(Sys.time())

if (verbose) {
  print(glue("Runtime: {round(toc - tic)} s"))
}
