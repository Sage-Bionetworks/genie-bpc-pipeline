# Create Release Files for BPC
# Usage: Rscript create_release_files.R -c [cohort]

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
  make_option(c("-c", "--cohort"), 
              type = "character",
              help="BPC cohort. i.e. NSCLC, CRC, BrCa, and etc."),
  make_option(c("-s", "--save_to_synapse"), 
              action="store_true", 
              default = FALSE,
              help="Save files to Synapse and delete local copies"),
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
selected_cohort <- opt$cohort
save_to_synapse <- opt$save_to_synapse
auth <- opt$auth
verbose <- opt$verbose

if (verbose) {
  print(glue("Parameters: "))
  print(glue("- cohort:\t\t{selected_cohort}"))
  print(glue("- save on synapse:\t{save_to_synapse}"))
  print(glue("- verbose:\t\t{verbose}"))
}

# parameters -----------------------

# Variation of cohort names 
str_cohort <- selected_cohort
if(str_cohort == "PANC"){
  str_cohort <- "Pancreas"
} else if(str_cohort == "BLADDER"){
  str_cohort <- "Bladder"
}

# defined variables
syn_id_rdata <- "syn22299362"
syn_id_sor <- "syn22294851"
syn_id_release_info <- "syn27628075"

# functions -----------------------

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

filter_for_release <- function(dataset, selected_dataset, selected_cohort, sor_df_filtered)
{
  # get the list of released columns
  release_cols <- sor_df_filtered %>%
    filter(dataset==selected_dataset) %>%
    pull(variable)
  
  release_cols <- intersect(release_cols, colnames(dataset))
  
  # filter the data by cohort and release columns
  release_dat <- dataset %>%
    filter(cohort==selected_cohort) %>%
    select(all_of(release_cols))
  
  return(release_dat)
}

store_synapse <- function(var, 
                          filename, 
                          syn_id_release_folder, 
                          save_to_synapse = F,
                          activity = NULL) {
  write.csv(var, filename, row.names = F, quote = T, na = "")

  if (save_to_synapse) {
    synStore(File(filename, syn_id_release_folder), activity = activity)
    file.remove(filename)
  }
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

if (verbose) {
  print(glue("{now(timeOnly = T)}: reading release information from '{synGet(syn_id_release_info)$properties$name}' ({syn_id_release_info})..."))
}

# download release info
release_info <- synTableQuery(glue("SELECT cohort, release_version, clinical_file_folder FROM {syn_id_release_info} WHERE current is true"))$asDataFrame()
syn_id_release_folder <- release_info[release_info$cohort==selected_cohort,]$clinical_file_folder
release_version <- release_info[release_info$cohort==selected_cohort,]$release_version
if(release_version == "1.1"){
  release_version <- "1"
}

if (verbose) {
  print(glue("{now(timeOnly = T)}: loading derived variable file data from '{synGet(syn_id_rdata)$properties$name}' ({syn_id_rdata})..."))
}

# load Rdata of derived variable
load(synGet(syn_id_rdata)$path)

if (verbose) {
  print(glue("{now(timeOnly = T)}: reading scope of release from '{synGet(syn_id_sor)$properties$name}' ({syn_id_sor})..."))
}

# read scope of release
sor_df <- readxl::read_excel(synGet(syn_id_sor)$path, sheet = "Data Dictionary")

# main -----------------

if (verbose) {
  print(glue("{now(timeOnly = T)}: formatting scope of release information..."))
}

# use the scope of release to determine which variables to release
clinical_column <- colnames(sor_df)[grepl(glue("Shared for ",str_cohort,".+",release_version,".+"),colnames(sor_df))]
cols_to_use <- c('VARNAME','Dataset',clinical_column)
cols_to_use_name <- c('variable','dataset','sor')
sor_df_filtered <- sor_df[,cols_to_use]
colnames(sor_df_filtered) <- cols_to_use_name
yes_values <- c("yes","index cancer only","non-index cancer only")
sor_df_filtered <- sor_df_filtered %>%
  mutate(sor = tolower(sor)) %>%
  mutate_at(all_of("sor"),list(~recode(.,"always"="yes"))) %>%
  filter(sor %in% yes_values)

if (verbose) {
  print(glue("{now(timeOnly = T)}: filtering datasets according to scope of release information..."))
}

# generate release dataset
ca_dx_derived_index_release <- filter_for_release(ca_dx_derived_index_redacted,
                                                  selected_dataset = "Cancer diagnosis dataset",
                                                  selected_cohort,
                                                  sor_df_filtered = sor_df_filtered)
index_cols <- intersect(sor_df_filtered$variable[sor_df_filtered$sor %in% c("yes","index cancer only")],
                        colnames(ca_dx_derived_index_release))
ca_dx_derived_index_release <- ca_dx_derived_index_release[index_cols]

ca_dx_derived_non_index_release <- filter_for_release(ca_dx_derived_non_index_redacted,
                                                      selected_dataset = "Cancer diagnosis dataset",
                                                      selected_cohort,
                                                      sor_df_filtered = sor_df_filtered)
non_index_cols <- intersect(sor_df_filtered$variable[sor_df_filtered$sor %in% c("yes","non-index cancer only")],
                            colnames(ca_dx_derived_non_index_release))
ca_dx_derived_non_index_release <- ca_dx_derived_non_index_release[non_index_cols]
pt_derived_release <- filter_for_release(pt_derived_redacted,
                                         'Patient-level dataset',
                                         selected_cohort,
                                         sor_df_filtered = sor_df_filtered)
ca_drugs_derived_release <- filter_for_release(ca_drugs_derived_redacted, 
                                               'Cancer-directed regimen dataset',
                                               selected_cohort,
                                               sor_df_filtered = sor_df_filtered)
prissmm_image_derived_release <- filter_for_release(prissmm_image_derived_redacted,
                                                    'PRISSMM Imaging level dataset',
                                                    selected_cohort,
                                                    sor_df_filtered = sor_df_filtered)
prissmm_path_derived_release <- filter_for_release(prissmm_path_derived_redacted,
                                                   'PRISSMM Pathology level dataset',
                                                   selected_cohort,
                                                   sor_df_filtered = sor_df_filtered)
prissmm_md_derived_release <- filter_for_release(prissmm_md_derived_redacted, 
                                                 'PRISSMM Medical Oncologist Assessment level dataset',
                                                 selected_cohort,
                                                 sor_df_filtered = sor_df_filtered)
cpt_derived_release <- filter_for_release(cpt_derived_redacted, 
                                          'Cancer panel test level dataset',
                                          selected_cohort,
                                          sor_df_filtered = sor_df_filtered)
if('PRISSMM Tumor Marker level dataset' %in% unique(sor_df_filtered$dataset)){
  prissmm_tm_derived_release <- filter_for_release(prissmm_tm_derived_redacted, 
                                                   'PRISSMM Tumor Marker level dataset',
                                                   selected_cohort,
                                                   sor_df_filtered = sor_df_filtered)
}
if('Cancer-Directed Radiation Therapy dataset' %in% unique(sor_df_filtered$dataset)){
  ca_radtx_derived_release <- filter_for_release(ca_radtx_derived_redacted, 
                                                 'Cancer-Directed Radiation Therapy dataset',
                                                 selected_cohort,
                                                 sor_df_filtered = sor_df_filtered)
}

# save to synapse ---------------------

if (save_to_synapse && verbose) {
  print(glue("{now(timeOnly = T)}: wiping release folder '{synGet(syn_id_release_folder)$properties$name}' ({syn_id_release_folder})..."))
}

# Wipe the folder
if (save_to_synapse) {
  current_files <- synGetChildren(syn_id_release_folder)$asList()
  remove_files <- sapply(current_files, function(x){
    synDelete(x[['id']])
  })
}

if (verbose) {
  if (save_to_synapse) {
    print(glue("{now(timeOnly = T)}: storing clinical files in release folder '{synGet(syn_id_release_folder)$properties$name}' ({syn_id_release_folder})..."))
  } else {
    print(glue("{now(timeOnly = T)}: storing clinical files locally in current working directory '{getwd()}'..."))
  }
}

# provenance
act <- Activity(name = "BPC clinical files",
                description = glue("GENIE BPC clinical file generation for the {selected_cohort} cohort"),
                used = c(syn_id_rdata, syn_id_release_info, syn_id_sor),
                executed = "https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/release/create_release_files.R")

# Write and store files to Synapse
store_synapse(ca_dx_derived_index_release, "cancer_level_dataset_index.csv",syn_id_release_folder, save_to_synapse = save_to_synapse, activity = act)
store_synapse(ca_dx_derived_non_index_release, "cancer_level_dataset_non_index.csv",syn_id_release_folder, save_to_synapse = save_to_synapse, activity = act)
store_synapse(pt_derived_release, "patient_level_dataset.csv",syn_id_release_folder, save_to_synapse = save_to_synapse, activity = act)
store_synapse(ca_drugs_derived_release, "regimen_cancer_level_dataset.csv",syn_id_release_folder, save_to_synapse = save_to_synapse, activity = act)
store_synapse(prissmm_image_derived_release, "imaging_level_dataset.csv",syn_id_release_folder, save_to_synapse = save_to_synapse, activity = act)
store_synapse(prissmm_path_derived_release, "pathology_report_level_dataset.csv",syn_id_release_folder, save_to_synapse = save_to_synapse, activity = act)
store_synapse(prissmm_md_derived_release, "med_onc_note_level_dataset.csv",syn_id_release_folder, save_to_synapse = save_to_synapse, activity = act)
store_synapse(cpt_derived_release, "cancer_panel_test_level_dataset.csv",syn_id_release_folder, save_to_synapse = save_to_synapse, activity = act)
if('PRISSMM Tumor Marker level dataset' %in% unique(sor_df_filtered$dataset)){
  store_synapse(prissmm_tm_derived_release, "tm_level_dataset.csv",syn_id_release_folder, save_to_synapse = save_to_synapse, activity = act)
}
if('Cancer-Directed Radiation Therapy dataset' %in% unique(sor_df_filtered$dataset)){
  store_synapse(ca_radtx_derived_release, "ca_radtx_dataset.csv",syn_id_release_folder, save_to_synapse = save_to_synapse, activity = act)
}

# close out --------------------

toc = as.double(Sys.time())
if (verbose) {
  print(glue("Runtime: {round(toc - tic)} s"))
}
