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
  make_option(c("-s", "--staging"), 
              action="store_true", 
              default = FALSE,
              help="Save files to staging folder and delete local copies"),
  make_option(c("-r", "--release"), 
              action="store_true", 
              default = FALSE,
              help="Copy release files from staging to release folder"),
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
staging <- opt$staging
release <- opt$release
auth <- opt$auth
verbose <- opt$verbose

if (verbose) {
  print(glue("Parameters: "))
  print(glue("- cohort:\t\t{selected_cohort}"))
  print(glue("- staging:\t\t{staging}"))
  print(glue("- release:\t\t{release}"))
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
#syn_id_rdata_version <- 58
syn_id_sor <- "syn22294851"
syn_id_release_info <- "syn27628075"
syn_id_retraction <- "syn52915299"

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

filter_for_release <- function(dataset, selected_dataset, selected_cohort, 
                               sor_df_filtered, retracted_patient)
{
  # get the list of released columns
  release_cols <- sor_df_filtered %>%
    filter(dataset==selected_dataset) %>%
    pull(variable)
  
  release_cols <- intersect(release_cols, colnames(dataset))
  
  # filter the data by cohort and release columns
  release_dat <- dataset %>%
    filter(cohort_internal==selected_cohort) %>%
    filter(!record_id %in% retracted_patient) %>%
    select(all_of(release_cols))
  
  return(release_dat)
}

store_file <- function(var, 
                       filename, 
                       syn_id_output_folder, 
                       save_to_synapse = F,
                       activity = NULL) {
  write.csv(var, filename, row.names = F, quote = T, na = "")

  if (save_to_synapse) {
    synStore(File(filename, syn_id_output_folder), activity = activity)
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
release_info <- synTableQuery(glue("SELECT cohort, release_version, release_type, staging_folder, clinical_file_folder, sor_column FROM {syn_id_release_info} WHERE current is true"))$asDataFrame()
syn_id_staging_folder <- release_info[release_info$cohort==selected_cohort,]$staging_folder
syn_id_release_folder <- release_info[release_info$cohort==selected_cohort,]$clinical_file_folder
clinical_column <- release_info[release_info$cohort==selected_cohort,]$sor_column

# load Rdata of derived variable
if (verbose) {
  print(glue("{now(timeOnly = T)}: loading derived variable file data from '{synGet(syn_id_rdata)$properties$name}' ({syn_id_rdata})..."))
}

load(synGet(syn_id_rdata)$path)

# read scope of release
if (verbose) {
  print(glue("{now(timeOnly = T)}: reading scope of release from '{synGet(syn_id_sor)$properties$name}' ({syn_id_sor})..."))
}

sor_df <- readxl::read_excel(synGet(syn_id_sor)$path, sheet = "Data Dictionary")

# check that column name exists in the SOR
if (!is.element(clinical_column, colnames(sor_df))) {
  message(glue("Column '{clinical_column}' does not exist in the SOR.  Check the column name in the release info table ({syn_id_release_info}) and the SOR ({syn_id_sor}). Quitting..."))
  stop()
}

if (verbose) {
  release_version <- release_info[release_info$cohort==selected_cohort,]$release_version
  release_type <- release_info[release_info$cohort==selected_cohort,]$release_type
  print(glue("{now(timeOnly = T)}: extracting release status for {selected_cohort} {release_version}-{release_type} from SOR column '{clinical_column}'..."))
}

# get the list of retracted patient
if (verbose) {
  print(glue("{now(timeOnly = T)}: loading retracted patients ({syn_id_retraction}) for the release..."))
}
retracted_table <- synTableQuery(glue("SELECT patient_id FROM {syn_id_retraction} WHERE cohort like '%{selected_cohort}%'"))$asDataFrame()
retracted_pt_list <- retracted_table$patient_id

# main -----------------

if (verbose) {
  print(glue("{now(timeOnly = T)}: formatting scope of release information..."))
}

# use the scope of release to determine which variables to release
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
                                                  sor_df_filtered = sor_df_filtered,
                                                  retracted_patient = retracted_pt_list)
index_cols <- intersect(sor_df_filtered$variable[sor_df_filtered$sor %in% c("yes","index cancer only")],
                        colnames(ca_dx_derived_index_release))
ca_dx_derived_index_release <- ca_dx_derived_index_release[index_cols]

ca_dx_derived_non_index_release <- filter_for_release(ca_dx_derived_non_index_redacted,
                                                      selected_dataset = "Cancer diagnosis dataset",
                                                      selected_cohort,
                                                      sor_df_filtered = sor_df_filtered,
                                                      retracted_patient = retracted_pt_list)
non_index_cols <- intersect(sor_df_filtered$variable[sor_df_filtered$sor %in% c("yes","non-index cancer only")],
                            colnames(ca_dx_derived_non_index_release))
ca_dx_derived_non_index_release <- ca_dx_derived_non_index_release[non_index_cols]
pt_derived_release <- filter_for_release(pt_derived_redacted,
                                         'Patient-level dataset',
                                         selected_cohort,
                                         sor_df_filtered = sor_df_filtered,
                                         retracted_patient = retracted_pt_list)
ca_drugs_derived_release <- filter_for_release(ca_drugs_derived_redacted, 
                                               'Cancer-directed regimen dataset',
                                               selected_cohort,
                                               sor_df_filtered = sor_df_filtered,
                                               retracted_patient = retracted_pt_list)
prissmm_image_derived_release <- filter_for_release(prissmm_image_derived_redacted,
                                                    'PRISSMM Imaging level dataset',
                                                    selected_cohort,
                                                    sor_df_filtered = sor_df_filtered,
                                                    retracted_patient = retracted_pt_list)
prissmm_path_derived_release <- filter_for_release(prissmm_path_derived_redacted,
                                                   'PRISSMM Pathology level dataset',
                                                   selected_cohort,
                                                   sor_df_filtered = sor_df_filtered,
                                                   retracted_patient = retracted_pt_list)
prissmm_md_derived_release <- filter_for_release(prissmm_md_derived_redacted, 
                                                 'PRISSMM Medical Oncologist Assessment level dataset',
                                                 selected_cohort,
                                                 sor_df_filtered = sor_df_filtered,
                                                 retracted_patient = retracted_pt_list)
cpt_derived_release <- filter_for_release(cpt_derived_redacted, 
                                          'Cancer panel test level dataset',
                                          selected_cohort,
                                          sor_df_filtered = sor_df_filtered,
                                          retracted_patient = retracted_pt_list)
if('PRISSMM Tumor Marker level dataset' %in% unique(sor_df_filtered$dataset)){
  prissmm_tm_derived_release <- filter_for_release(prissmm_tm_derived_redacted, 
                                                   'PRISSMM Tumor Marker level dataset',
                                                   selected_cohort,
                                                   sor_df_filtered = sor_df_filtered,
                                                   retracted_patient = retracted_pt_list)
}
if('Cancer-Directed Radiation Therapy dataset' %in% unique(sor_df_filtered$dataset)){
  ca_radtx_derived_release <- filter_for_release(ca_radtx_derived_redacted, 
                                                 'Cancer-Directed Radiation Therapy dataset',
                                                 selected_cohort,
                                                 sor_df_filtered = sor_df_filtered,
                                                 retracted_patient = retracted_pt_list)
}

# save to synapse ---------------------

if (release & verbose) {
  print(glue("{now(timeOnly = T)}: wiping release folder '{synGet(syn_id_release_folder)$properties$name}' ({syn_id_release_folder})..."))
}

# Wipe the folder
if (release) {
  current_files <- synGetChildren(syn_id_release_folder)$asList()
  remove_files <- sapply(current_files, function(x){
    synDelete(x[['id']])
  })
}

# Get the clinical folder in Staging
current_files <- synGetChildren(syn_id_staging_folder)$asList()
current_files_tbl <- do.call(rbind.data.frame, current_files)
if ("clinical_data" %in% current_files_tbl$name) {
  syn_id_staging_clinical <- current_files_tbl$id[which(current_files_tbl$name=="clinical_data")]
} else {
  syn_id_staging_clinical <- synStore(Folder(name="clinical_data", parent=syn_id_staging_folder))$properties$id
}

if (verbose) {
  if (release) {
    print(glue("{now(timeOnly = T)}: copying clinical files from staging to release folder '{synGet(syn_id_release_folder)$properties$name}' ({syn_id_release_folder})..."))
  } else if (staging) {
    print(glue("{now(timeOnly = T)}: storing clinical files to staging folder '{synGet(syn_id_staging_clinical)$properties$name}' ({syn_id_staging_clinical})..."))
  } else {
    print(glue("{now(timeOnly = T)}: storing clinical files locally in current working directory '{getwd()}'..."))
  }
}

if(release){
  staging_files <- synGetChildren(syn_id_staging_clinical)$asList()
  # Check if there is files in the staging folder
  if (length(staging_files) == 0) {
    message(glue("No files was found in the staging folder ({syn_id_staging_clinical}). Quitting..."))
    stop()
  }
  copy_files <- sapply(staging_files, function(x){
    synapserutils::copy(x[['id']], syn_id_release_folder, setProvenance='existing')
  })
} else {
  # provenance
  act <- Activity(name = "BPC clinical files",
                  description = glue("GENIE BPC clinical file generation for the {selected_cohort} cohort"),
                  used = c(syn_id_rdata, syn_id_release_info, syn_id_sor, syn_id_retraction),
                  executed = "https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/release/create_release_files.R")

  # Write and store files to local or staging
  store_file(ca_dx_derived_index_release, "cancer_level_dataset_index.csv",syn_id_staging_clinical, save_to_synapse = staging, activity = act)
  store_file(ca_dx_derived_non_index_release, "cancer_level_dataset_non_index.csv",syn_id_staging_clinical, save_to_synapse = staging, activity = act)
  store_file(pt_derived_release, "patient_level_dataset.csv",syn_id_staging_clinical, save_to_synapse = staging, activity = act)
  store_file(ca_drugs_derived_release, "regimen_cancer_level_dataset.csv",syn_id_staging_clinical, save_to_synapse = staging, activity = act)
  store_file(prissmm_image_derived_release, "imaging_level_dataset.csv",syn_id_staging_clinical, save_to_synapse = staging, activity = act)
  store_file(prissmm_path_derived_release, "pathology_report_level_dataset.csv",syn_id_staging_clinical, save_to_synapse = staging, activity = act)
  store_file(prissmm_md_derived_release, "med_onc_note_level_dataset.csv",syn_id_staging_clinical, save_to_synapse = staging, activity = act)
  store_file(cpt_derived_release, "cancer_panel_test_level_dataset.csv",syn_id_staging_clinical, save_to_synapse = staging, activity = act)
  if('PRISSMM Tumor Marker level dataset' %in% unique(sor_df_filtered$dataset)){
    store_file(prissmm_tm_derived_release, "tm_level_dataset.csv",syn_id_staging_clinical, save_to_synapse = staging, activity = act)
  }
  if('Cancer-Directed Radiation Therapy dataset' %in% unique(sor_df_filtered$dataset)){
    store_file(ca_radtx_derived_release, "ca_radtx_dataset.csv",syn_id_staging_clinical, save_to_synapse = staging, activity = act)
  }
}
# close out --------------------

toc = as.double(Sys.time())
if (verbose) {
  print(glue("Runtime: {round(toc - tic)} s"))
}
