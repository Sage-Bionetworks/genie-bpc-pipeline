# Description: Workflow for conducting BPC case selection and constructing both 
#   output files representing eligibility criteria, ID lists, and a reports.
# Author: Haley Hunter-Zinck
# Date: 2021-09-22

# pre-setup --------------------------------

library(optparse)
library(yaml)
library(glue)
source("shared_fxns.R")

# parameters
config <- read_yaml("config.yaml")

# user input ----------------------------

option_list <- list( 
  make_option(c("-p", "--phase"), type = "character",
              help="BPC phase"),
  make_option(c("-c", "--cohort"), type = "character",
              help="BPC cohort"),
  make_option(c("-s", "--site"), type = "character",
              help="BPC site"),
  make_option(c("-mp", "--main_patient"), default = 'syn9734568', 
              help="Main GENIE file of all patients with patient info. Defaults to the lastest version of the Main Genie Release."),
  make_option(c("-ms", "--main_sample"), default = 'syn9734573', 
              help="Main GENIE file of all samples with sample info. Defaults to the lastest version of the Main Genie Release."),
  make_option(c("--production"), action="store_true", default = FALSE,
              help="Save output to production folder")
)
opt <- parse_args(OptionParser(option_list=option_list))

if (is.null(opt$phase) || is.null(opt$cohort) || is.null(opt$site)) {
  stop("Usage: Rscript workflow_case_selection.R -h")
}
phase <- opt$phase
cohort <- opt$cohort
site <- opt$site
main_patient <- opt$main_patient
main_sample <- opt$main_sample
is_production <- opt$production

# check user input -----------------

if (!is.element(phase, names(config$phase))) {
  stop(glue("Error: {phase} is not a valid phase. Valid values: {phase_str}\nUsage: workflow_case_selection.R -h"))
}

cohort_in_config <- names(config$phase[[phase]]$cohort)
cohort_str <- paste0(cohort_in_config, collapse = ", ")
if (!is.element(cohort, cohort_in_config)) {
  stop(glue("Error: {cohort} is not a valid cohort. Valid values: {cohort_str}\nUsage: workflow_case_selection.R -h"))
}

sites_in_config <- get_sites_in_config(config, phase, cohort)
site_str <- paste0(sites_in_config, collapse = ", ")
if (!is.element(site, sites_in_config)) {
  stop(glue("Error: {site} is not a valid site. Valid values: {site_str}\nUsage: workflow_case_selection.R -h"))
}

# setup ----------------------------

tic = as.double(Sys.time())

library(dplyr)
library(synapser)
synLogin()

# file names
file_report <- tolower(glue("{cohort}_{site}_phase{phase}_case_selection.html"))
file_matrix <- tolower(glue("{cohort}_{site}_phase{phase}_eligibility_matrix.csv"))
file_selection <- tolower(glue("{cohort}_{site}_phase{phase}_case_selection.csv"))
file_add <- tolower(glue("{cohort}_{site}_phase{phase}_samples.csv"))

# synapse
synid_folder_output <- get_folder_synid_from_path(synid_folder_root = config$synapse$ids$id, 
                                                  path = glue("{cohort}/{site}"))

# provenance exec
prov_exec_selection <- "https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/case_selection/perform_case_selection.R"
prov_exec_add <- prov_exec_selection
prov_exec_report <- "https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/case_selection/perform_case_selection.Rmd"

# provancne used
prov_used_selection <- c(main_patient, main_sample, config$synapse$bpc_patient$id)
prov_used_add <- c(prov_used_selection, config$synapse$bpc_sample$id)
prov_used_report <- ""

# additional parameters
flag_additional <- grepl(pattern = "addition", x = phase)

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

# case selection ----------------------------

# construct eligibility matrices + case lists
exit_status <- system(glue("Rscript perform_case_selection.R -p {phase} -c {cohort} -s {site} -mp {main_patient} -ms {main_sample}"))

# Check if the command failed
if (exit_status != 0) {
  stop(glue("The command Rscript perform_case_selection.R failed with exit status {exit_status}. Script will terminate."))
}

if (!flag_additional) {
  # render eligibility report
  rmarkdown::render("perform_case_selection.Rmd", 
                    output_file = file_report,
                    params = list(phase = phase, cohort = cohort, site = site, main_patient = main_patient, main_sample = main_sample))
}

# load to synapse --------------------
 # This is the staging folder to upload files to
if (!is_production) {
  synid_folder_output <- "syn62149812"
}
# store case selection files 

if (file.exists(file_selection)) {
  save_to_synapse(path = file_matrix,
                  parent_id = synid_folder_output,
                  prov_name = "Eligibility matrix",
                  prov_desc = "Reports eligibility criteria and values for all possible patients",
                  prov_used = prov_used_selection,
                  prov_exec = prov_exec_selection)
  save_to_synapse(path = file_selection,
                  parent_id = synid_folder_output,
                  prov_name = "Eligible cohort",
                  prov_desc = "Cohort of eligible patient IDs",
                  prov_used = prov_used_selection,
                  prov_exec = prov_exec_selection)

  prov_used_report <- get_file_synid_from_path(synid_folder_root = config$synapse$ids$id,
                                                path = glue("{cohort}/{site}/{file_matrix}"))
  save_to_synapse(path = file_report,
                  parent_id = synid_folder_output,
                  prov_name = "Summary of eligibility",
                  prov_desc = "Summary of steps and information for selection eligible patients",
                  prov_used = prov_used_report,
                  prov_exec = prov_exec_report)
  
  # local clean-up
  if (is_production) {
    file.remove(file_matrix)
    file.remove(file_selection)
    file.remove(file_report)
  }

} else if (file.exists(file_add)) {
  save_to_synapse(path = file_add,
                  parent_id = synid_folder_output,
                  prov_name = "Summary of eligibility",
                  prov_desc = "Summary of steps and information for selection eligible patients",
                  prov_used = prov_used_add,
                  prov_exec = prov_exec_add)

  # local clean-up
  if (is_production) {
    file.remove(file_add)
  }
}


# close out ----------------------------
print(glue("Output saved to Synapse ({synid_folder_output})"))

toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
