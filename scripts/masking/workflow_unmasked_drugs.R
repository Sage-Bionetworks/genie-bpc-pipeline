# Description: Creates sign off sheets by cohort and site with flags to mark issues with drug masking 
#   and generates parallel reports to highlight issues.  
# Author: Haley Hunter-Zinck
# Date: September 2, 2021
# Inputs: 
#  cohort: Cohort on which to run drug masking report
#  date: date of latest upload (for file labeling only)

# load libraries and parameters -----------------------------

library(optparse)
library(glue)
library(rmarkdown)
library(yaml)
library(synapser)

workdir <- "."
if (!file.exists("config.yaml")) {
  workdir <- "/usr/local/src/myscripts"
}

config <- read_yaml(glue("{workdir}/config.yaml"))

# user input ----------------------

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

option_list <- list( 
  make_option(c("-c", "--cohort"), type="character", 
              help="Cohort on which run analysis"),
  make_option(c("-d", "--date"), type="character", 
              help="Upload date for folder labels"),
  make_option(c("-s", "--save_synapse"), action="store_true", default=FALSE,
              help="Save output to Synapse"),
  make_option(c("-a", "--synapse_auth"), type = "character", default = "~/.synapseConfig", 
              help="Path to .synapseConfig file or Synapse PAT (default: '~/.synapseConfig')")
)

# user arguments
opt <- parse_args(OptionParser(option_list=option_list))
cohort <- opt$cohort
date <- opt$date
save_synapse <- opt$save_synapse
auth <- opt$synapse_auth

# flags
flag_run_analysis <- T
flag_generate_report <- save_synapse

# check user input -----------------

# print paremters
print(glue("Parameters:"))
print(glue("  run_analysis = {flag_run_analysis}"))
print(glue("  render_report = {flag_generate_report}"))
print(glue("  save_synapse = {save_synapse}"))
print(glue("  cohort = {cohort}"))
print(glue("  date = {date}"))

# check that analysis or report or both are flagged
waitifnot(flag_run_analysis || flag_generate_report,
          msg = c("Error: --run_analysis or --render_report flag must be activated.",
                  "Usage: Rscript workflow_unmasked_drugs -h"))

# check that cohort and date are entered
waitifnot(!is.null(cohort),
          msg = c("Error: cohort value (-c COHORT) must be entered",
                  "Usage: Rscript workflow_unmasked_drugs -h"))
waitifnot(!is.null(date),
          msg = c("Error: date value (-d DATE) must be entered",
                  "Usage: Rscript workflow_unmasked_drugs -h"))

# check for valid cohort
cohort_str <- paste0("'", paste0(names(config$cohort), collapse = "', '"), "'")
waitifnot(is.element(cohort, names(config$cohort)),
          msg = c(glue("Error: cohort '{cohort}' is invalid.  Valid values: {cohort_str}"),
                  "Usage: Rscript workflow_unmasked_drugs -h"))

# check for valid date (before present date)
waitifnot(as.Date(date) <= as.Date(Sys.time()),
          msg = c(glue("Error: date ('{date}') cannot be in the future.  Please adjust.  "),
                  "Usage: Rscript workflow_unmasked_drugs -h"))

# setup ----------------------------

tic = as.double(Sys.time())

# parameters

sites <- config$cohort[[cohort]]$sites
debug <- config$misc$debug

# functions ----------------------------

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

create_synapse_folder <- function(name, parentId) {
  
  # check if folder already exists
  children <- get_synapse_folder_children(parentId)
  if(is.element(name, names(children))) {
    return(as.character(children[name]))
  }
  
  concreteType <- "org.sagebionetworks.repo.model.Folder"
  uri <- "/entity"
  payload <- paste0("{", glue("'name':'{name}', 'parentId':'{parentId}', 'concreteType':'{concreteType}'"), "}")
  ent <- synRestPOST(uri = uri, body = payload)
  return(ent$id)
}

get_synid_drug_report_folder <- function(cohort, date, synid_folder_qc, map_cohort) {
  
  # get cohort subfolder synapse id
  synid_children_cohort <- get_synapse_folder_children(synid_folder_qc)
  synid_folder_cohort <- as.character(synid_children_cohort[map_cohort[[cohort]]])
  
  # get synapse ID of folder containing reports
  synid_children_review <- get_synapse_folder_children(synid_folder_cohort)
  synid_folder_review <- as.character(synid_children_review["qa_drug_review"])
  
  # get synapse ID of folder for most recent previous review
  synid_folder_date <- create_synapse_folder(name = glue("{date} Reports"), parentId = synid_folder_review)
  
  return(synid_folder_date)
}

create_synid_drug_signoff_folder <- function(cohort, date, synid_folder_qc, map_cohort) {
  
  # get cohort subfolder synapse id
  synid_children_cohort <- get_synapse_folder_children(synid_folder_qc)
  synid_folder_cohort <- as.character(synid_children_cohort[map_cohort[[cohort]]])
  
  # get synapse ID of folder containing report signoffs
  synid_children_review <- get_synapse_folder_children(synid_folder_cohort)
  synid_folder_review <- as.character(synid_children_review["qa_drug_review_qa_manager_signoff"])
  
  # get synapse ID of folder for most recent previous review
  synid_folder_date <- create_synapse_folder(name = glue("{date} Reports"), parentId = synid_folder_review)
  
  return(synid_folder_date)
}


get_synid_report <- function(cohort, site, date, synid_folder_qc, map_cohort) {
  
  # get cohort subfolder synapse id
  synid_children_cohort <- get_synapse_folder_children(synid_folder_qc)
  synid_folder_cohort <- as.character(synid_children_cohort[map_cohort[[cohort]]])
  
  # get synapse ID of folder labeled "qa_drug_review_qa_manager_signoff"
  synid_children_review <- get_synapse_folder_children(synid_folder_cohort)
  synid_folder_review <- as.character(synid_children_review["qa_drug_review"])
  
  # get synapse ID of folder for most recent previous review
  synid_children_date <- get_synapse_folder_children(synid_folder_review)
  synid_folder_date <- as.character(synid_children_date[glue("{date} Reports")])
  
  synid_children_report <- get_synapse_folder_children(synid_folder_date)
  synid_file_report <- as.character(synid_children_report[glue("unmasked_drugs_review_{site}_{cohort}_{date}.csv")])
  
  return(synid_file_report)
}

now <- function(timeOnly = F, tz = "US/Pacific") {
  
  Sys.setenv(TZ=tz)
  
  if(timeOnly) {
    return(format(Sys.time(), "%H:%M:%S"))
  }
  
  return(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
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

# create spreadsheets and reports for each site
for (site in sites) {
  
  if (debug) {
    print(glue("{now(timeOnly = T)}: starting drug masking report analysis for site '{site}'..."))
    print(glue("  {now(timeOnly = T)}: analyzing drug regimens..."))
  }
  
  # generate report data and save to Synapse
  if (flag_run_analysis) {
    
    system(glue("Rscript {workdir}/qa_unmasked_drugs_hemonc.R --args -c {cohort} -s {site} -d {date} -a {auth}"))
    
    if (save_synapse) {
      
      synid_folder_output <- get_synid_drug_report_folder(cohort = cohort,
                                                          date = date,
                                                          synid_folder_qc = config$synapse$qc$id,
                                                          map_cohort = config$map$cohort_name)
      synid_folder_signoff <- create_synid_drug_signoff_folder(cohort = cohort,
                                       date = date,
                                       synid_folder_qc = config$synapse$qc$id,
                                       map_cohort = config$map$cohort_name)
      file_local <- glue("unmasked_drugs_review_{site}_{cohort}_{date}.csv")
      save_to_synapse(path =  file_local, 
                      parent_id = synid_folder_output, 
                      prov_name = "drug masking report", 
                      prov_desc = "flags potentially investigational drugs for review", 
                      prov_used = c(config$synapse$drugs$id, 
                                    config$synapse$relationship$id, 
                                    config$synapse$concept$id, 
                                    config$synapse$map$id, 
                                    config$synapse$patient$id,
                                    config$synapse$curation$id), 
                      prov_exec = "https://github.com/Sage-Bionetworks/Genie_processing/blob/main/bpc/masking/qa_unmasked_drugs_hemonc.R")
      
      # clean up locally
      file.remove(file_local)
    }
  }
  
  # generate report from report data and save to synapse
  if (flag_generate_report) {
    
    if (debug) {
      print(glue("  {now(timeOnly = T)}: generating drug regimen report..."))
    }
    
    file_local <- glue("unmasked_drugs_review_{site}_{cohort}_{date}.html")
    rmarkdown::render(glue("{workdir}/qa_unmasked_drugs_hemonc.Rmd", 
                      params = list(cohort = cohort, site = site, date = date, auth = auth), 
                      output_file = file_local)
    
    if (save_synapse) {
      
      if (debug) {
        print(glue("  {now(timeOnly = T)}: saving report to Synapse..."))
      }
      
      # save and clean up
      synid_folder_output <- get_synid_drug_report_folder(cohort = cohort, 
                                                          date = date,
                                                          synid_folder_qc = config$synapse$qc$id,
                                                          map_cohort = config$map$cohort_name)
      prov_used <- get_synid_report(cohort = cohort, 
                                    site = site, 
                                    date = date, 
                                    synid_folder_qc = config$synapse$qc$id, 
                                    map_cohort = config$map$cohort_name)
      save_to_synapse(path = file_local, 
                      parent_id = synid_folder_output,
                      prov_name = "drug masking report", 
                      prov_desc = glue("drug masking report for the {cohort} cohort from site {site} uploaded on {date}"), 
                      prov_used = prov_used, 
                      prov_exec = "https://github.com/Sage-Bionetworks/Genie_processing/blob/main/bpc/masking/qa_unmasked_drugs_hemonc.Rmd")
      file.remove(file_local)
    }
  }
}

# close out ----------------------------

if (flag_run_analysis && save_synapse) {
  synid_folder_res <- get_synid_drug_report_folder(cohort = cohort,
                                                   date = date,
                                                   synid_folder_qc = config$synapse$qc$id,
                                                   map_cohort = config$map$cohort_name)
  name_folder_res <- synGet(synid_folder_res)$properties$name
  print(glue("!!!REMINDER!!!: notify QA managers that masking reports are available in folder '{name_folder_res}' ({synid_folder_res})"))
}

toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
