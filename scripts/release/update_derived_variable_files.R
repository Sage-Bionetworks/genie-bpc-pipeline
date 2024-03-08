# Create Release Files for BPC
# Usage: Rscript update_derived_variable_files.R -v

# set up ------------------------

# start timer
tic = as.double(Sys.time())

# set up
library(glue)
library(dplyr)
library(optparse)
library(synapser)

# cli -------------------

# input
option_list <- list( 
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
auth <- opt$auth
verbose <- opt$verbose

if (verbose) {
  print(glue("Parameters: "))
  print(glue("- verbose:\t\t{verbose}"))
}

# parameters -----------------------

# defined variables
syn_id_rdata <- "syn22299362"
syn_id_folder <- "syn22296812"
from <- c('NSCLC2', 'CRC2')
to <- c('NSCLC', 'CRC')

# functions -----------------------

# edit cohort in the data
edit_cohorts <- function(data, from, to) {
  result <- data %>% mutate(cohort=recode(cohort, !!! setNames(to, from)))
  return(result)
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

# store files to Synapse
store_derived_file <- function(var, 
                       filename, 
                       dataset,
                       syn_id_output_folder,
                       activity = NULL) {
  write.csv(var, filename, row.names = F, quote = T, na = "")
  
  synStore(File(filename, parent=syn_id_output_folder, 
           annotations=list(data_type='derived', dataset=dataset)),
           activity = activity)
  
  file.remove(filename)
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

# synapse login ---------------

status <- synLogin(auth = auth)

# read --------------------

# load Rdata of derived variable
if (verbose) {
  print(glue("{now(timeOnly = T)}: loading derived variable file data from '{synGet(syn_id_rdata,downloadFile=FALSE)$properties$name}' ({syn_id_rdata})..."))
}

obj_rdata <- synGet(syn_id_rdata)
env_rdata <- mget(load(obj_rdata$path, envir=(NE. <- new.env())), envir=NE.)

# main -----------------

if (verbose) {
  print(glue("{now(timeOnly = T)}: updating derived variables..."))
}

# Updating data
updated_df_list <- list()
for (df_name in names(env_rdata)) {
  if (verbose) {
    print(glue("{now(timeOnly = T)}: updating {df_name}..."))
  }
  dat <- env_rdata[[df_name]]
  result <- edit_cohorts(dat, from, to)
  updated_df_list[[df_name]] <- result
}

# Store derived variable files
if (verbose) {
  print(glue("{now(timeOnly = T)}: storing derived variable files to '{synGet(syn_id_folder)$properties$name}' ({syn_id_folder})..."))
}

# provenance
act <- Activity(name = "BPC derived files update",
                description = glue("GENIE BPC derived variable files update"),
                used = c(syn_id_rdata),
                executed = "https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/release/update_derived_variable_files.R")
# store
list2env(updated_df_list, .GlobalEnv)

store_derived_file(ca_dx_derived_redacted, "ca_dx_derived.csv", 'Cancer-level dataset', syn_id_folder, activity = act)
store_derived_file(ca_dx_derived_index_redacted, "ca_dx_derived_index.csv",'Cancer-level index dataset', syn_id_folder, activity = act)
store_derived_file(ca_dx_derived_non_index_redacted, "ca_dx_derived_non_index.csv",'Cancer-level non-index dataset', syn_id_folder, activity = act)
store_derived_file(pt_derived_redacted, "pt_derived.csv",'Patient-level dataset', syn_id_folder, activity = act)
store_derived_file(ca_drugs_derived_redacted, "ca_drugs_derived.csv",'Regimen-Cancer level dataset', syn_id_folder, activity = act)
store_derived_file(prissmm_image_derived_redacted, "prissmm_image_derived.csv",'Imaging-level dataset', syn_id_folder, activity = act)
store_derived_file(prissmm_path_derived_redacted, "prissmm_path_derived.csv",'Pathology-report level dataset', syn_id_folder, activity = act)
store_derived_file(prissmm_md_derived_redacted, "prissmm_md_derived.csv",'Med Onc Note level dataset', syn_id_folder, activity = act)
store_derived_file(prissmm_tm_derived_redacted, "prissmm_tm_derived.csv",'PRISSMM Tumor Marker level dataset', syn_id_folder, activity = act)
store_derived_file(cpt_derived_redacted, "cpt_derived.csv",'Cancer panel test level dataset', syn_id_folder, activity = act)
store_derived_file(ca_radtx_derived_redacted, "ca_radtx_derived.csv", "Cancer-Directed Radiation Therapy dataset", syn_id_folder, activity = act)

# Store updated rdata
filename_rdata <- obj_rdata$files[0]
save(list = names(updated_df_list), file = filename_rdata)
synStore(File(path=filename_rdata,
              parent=syn_id_folder,
              name=obj_rdata$properties$name), 
         activity = act)
file.remove(filename_rdata)

# close out --------------------

toc = as.double(Sys.time())
if (verbose) {
  print(glue("Runtime: {round(toc - tic)} s"))
}
