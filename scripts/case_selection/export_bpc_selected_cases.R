#! /usr/bin/env Rscript
# Authors: Xindi Guo, Kristen Dang
# September 17, 2019

# setup --------------------------

library(glue)
library(sqldf)
library(synapser)
library(optparse)
library(plyr)
source("shared_fxns.R")

synLogin()

options(stringsAsFactors = FALSE)

# user input --------------------------

option_list <- list( 
  make_option(c("-i", "--input"), 
              type = "character",
              help="Synapse ID of the input file that has the BPC selected cases"),
  make_option(c("-o", "--output"), 
              type = "character",
              default = "syn20798271",
              help="Synapse ID of the BPC output folder. Default: syn20798271"),
  make_option(c("-p", "--phase"), 
              type = "character",
              help="BPC phase. i.e. pilot, phase 1, phase 1 additional"),
  make_option(c("-c", "--cohort"), 
              type = "character",
              help="BPC cohort. i.e. NSCLC, CRC, BrCa, and etc."),
  make_option(c("-s", "--site"), type = "character",
              help="BPC site. i.e. DFCI, MSK, UHN, VICC, and etc.")
)
opt <- parse_args(OptionParser(option_list=option_list))

if (is.null(opt$input) || is.null(opt$phase) || is.null(opt$cohort) || is.null(opt$site)) {
  stop("Usage: Rscript export_bpc_selected_cases.R -h")
}

in_file <- opt$input
out_folder <- opt$output
phase <- opt$phase
cohort <- opt$cohort
site <- opt$site

# check user input -----------------

# TODO: how to get the options
# file_view_id <- "syn21557543"
# file_view_schema <- synGetColumns(file_view_id)$asList()

phase_option <- c("phase 1","phase 1 additional","phase 2")
cohort_option <- c("NSCLC","CRC","BrCa","PANC","Prostate","BLADDER", "MELANOMA", "RENAL", "OVARIAN", "ESOPHAGO")
site_option <- c("DFCI","MSK","UHN","VICC","UCSF","PROV","VHIO","JHU","WAKE")

# Check if phase is valid
phase_str <- paste0(phase_option, collapse = ", ")
if (!is.element(phase, phase_option)) {
  stop(glue("Error: {phase} is not a valid phase. Valid values: {phase_str}\nUsage: export_bpc_selected_cases.R -h"))
}

# Check if cohort is valid
cohort_str <- paste0(cohort_option, collapse = ", ")
if (!is.element(cohort, cohort_option)) {
  stop(glue("Error: {cohort} is not a valid cohort. Valid values: {cohort_str}\nUsage: export_bpc_selected_cases.R -h"))
}

# Check if site is valid
site_str <- paste0(site_option, collapse = ", ")
if (!is.element(site, site_option)) {
  stop(glue("Error: {site} is not a valid site. Valid values: {site_str}\nUsage: export_bpc_selected_cases.R -h"))
}

# setup ----------------------------
print("get clinical data")
# clinical data
# HACK: always use the most recent consortium release at the time of execution of this code
clinical_sample_id <- "syn9734573"
clinical_patient_id <- "syn9734568"

# mapping tables
sex_mapping <- synTableQuery("SELECT * FROM syn7434222")$asDataFrame()
race_mapping <- synTableQuery("SELECT * FROM syn7434236")$asDataFrame()
ethnicity_mapping <- synTableQuery("SELECT * FROM syn7434242")$asDataFrame()
# sample_type_mapping <- synTableQuery("SELECT * FROM syn7434273")$asDataFrame()

# output setup
phase_no_space <- sub(" ","_",sub(" ","",phase))
output_entity_name <- glue("{site}_{cohort}_{phase_no_space}_genie_export.csv")
output_file_name <- glue("{site}_{cohort}_{phase_no_space}_genie_export_{Sys.Date()}.csv")

# download input file and get selected cases/samples
selected_info <- read.csv(synGet(in_file)$path)
selected_cases <- selected_info$PATIENT_ID
# selected_samples <- unlist(strsplit(paste0(selected_info$SAMPLE_IDS,collapse=";"),";"))

# create the data file ----------------------------

# Create query for selected cases
temp <- toString(unique(selected_cases))
temp <- sapply(strsplit(temp, '[, ]+'), function(x) toString(shQuote(x)))

# download clinical data
# sample clinical data
# Make sure to include all patients that have at least one sample that meet the eligibility criteria
clinical_sample <- read.delim(synGet(clinical_sample_id, downloadFile = TRUE, followLink = TRUE)$path, skip = 4, header = TRUE)
clinical_sample <- sqldf(paste("SELECT * FROM clinical_sample where PATIENT_ID in (",temp,")",sep = ""))

# patient clinical data
clinical_patient <- read.delim(synGet(clinical_patient_id, downloadFile = TRUE, followLink = TRUE)$path, skip = 4, header = TRUE)

# combined clinical data
sql <- "select * from clinical_sample left join clinical_patient on clinical_sample.PATIENT_ID = clinical_patient.PATIENT_ID"
clinical <- sqldf(sql)

# change the columns to lower case
colnames(clinical) <- tolower(colnames(clinical))
print("get all samples for selected patients")
# Get all samples for those patients
# samples_per_patient <- sapply(selected_cases, function(x){as.character(clinical$sample_id[clinical$patient_id %in% x])})
missing_patients <- selected_cases[!selected_cases %in% clinical$patient_id]
print("Missing patients from consortium release:")
print(missing_patients)

samples_per_patient <- clinical$sample_id[clinical$patient_id %in% selected_cases]

print("map data for each instrument")
# mapping data for each instrument
# instrument - patient_characteristics
patient_output <- data.frame("record_id" = selected_cases)
patient_output$redcap_repeat_instrument <- rep("")
patient_output$redcap_repeat_instance <- rep("")

patient_output$genie_patient_id <- patient_output$record_id
patient_output$birth_year <- clinical$birth_year[match(patient_output$genie_patient_id, clinical$patient_id)]
patient_output$naaccr_ethnicity_code <- clinical$ethnicity[match(patient_output$genie_patient_id, clinical$patient_id)]
patient_output$naaccr_race_code_primary <- clinical$primary_race[match(patient_output$genie_patient_id, clinical$patient_id)]
patient_output$naaccr_race_code_secondary <- clinical$secondary_race[match(patient_output$genie_patient_id, clinical$patient_id)]
patient_output$naaccr_race_code_tertiary <- clinical$tertiary_race[match(patient_output$genie_patient_id, clinical$patient_id)]
patient_output$naaccr_sex_code <- clinical$sex[match(patient_output$genie_patient_id, clinical$patient_id)]

# mapping to code
patient_output$naaccr_ethnicity_code <- ethnicity_mapping$CODE[match(patient_output$naaccr_ethnicity_code, ethnicity_mapping$CBIO_LABEL)]
patient_output$naaccr_race_code_primary <- race_mapping$CODE[match(patient_output$naaccr_race_code_primary, race_mapping$CBIO_LABEL)]
patient_output$naaccr_race_code_secondary <- race_mapping$CODE[match(patient_output$naaccr_race_code_secondary, race_mapping$CBIO_LABEL)]
patient_output$naaccr_race_code_tertiary <- race_mapping$CODE[match(patient_output$naaccr_race_code_tertiary, race_mapping$CBIO_LABEL)]
patient_output$naaccr_sex_code <- sex_mapping$CODE[match(patient_output$naaccr_sex_code,sex_mapping$CBIO_LABEL)]
print("recode")
# recode
# cannotReleaseHIPAA = NA
patient_output$birth_year[which(patient_output$birth_year == "cannotReleaseHIPAA")] <- NA
# -1 Not collected = 9 Unknown
patient_output$naaccr_ethnicity_code[which(patient_output$naaccr_ethnicity_code == -1)] <- 9
# -1 Not collected = 99 Unknown
patient_output$naaccr_race_code_primary[which(patient_output$naaccr_race_code_primary == -1)] <- 99
# -1 Not collected = 88 according to NAACCR
patient_output$naaccr_race_code_secondary[which(patient_output$naaccr_race_code_secondary == -1)] <- 88
patient_output$naaccr_race_code_tertiary[which(patient_output$naaccr_race_code_tertiary == -1)] <- 88

print("instrument cancer panel test")
# instrument - cancer_panel_test
sample_info_list <- lapply(samples_per_patient,function(x){
  sample_list = list()
  for(i in 1:length(x)){
    temp_df = data.frame("record_id" = clinical$patient_id[clinical$sample_id == x[i]])
    temp_df$redcap_repeat_instrument = "cancer_panel_test"
    temp_df$redcap_repeat_instance = i
    temp_df$redcap_data_access_group = clinical$center[clinical$sample_id == x[i]]
    
    temp_df$cpt_genie_sample_id = x[i]
    temp_df$cpt_oncotree_code = clinical$oncotree_code[clinical$sample_id == x[i]]
    temp_df$cpt_sample_type = clinical$sample_type_detailed[clinical$sample_id == x[i]]
    temp_df$cpt_seq_assay_id = clinical$seq_assay_id[clinical$sample_id == x[i]]
    temp_df$cpt_seq_date = clinical$seq_year[clinical$sample_id == x[i]]
    temp_df$age_at_seq_report = clinical$age_at_seq_report_days[clinical$sample_id == x[i]]
    sample_list[[i]] <- temp_df
  }
  combined_df = rbind.fill(sample_list)
  return(combined_df)
})

sample_info_df <- rbind.fill(sample_info_list)
patient_output <- rbind.fill(patient_output,sample_info_df)

print("validate output")
n_unique_patients_export = length(unique(sample_info_df$record_id))
n_unique_samples_export = length(unique(sample_info_df$cpt_genie_sample_id))
n_unique_selected_patients = length(unique(selected_cases))
n_unique_selected_samples = length(unique(samples_per_patient))
n_missing_patients = length(missing_patients)

print(paste("export file N unique patients", n_unique_patients_export))
print(paste("export file N unique samples", n_unique_samples_export))
print(paste("N Unique selected patients", n_unique_selected_patients))
print(paste("N Unique selected samples", n_unique_selected_samples))

if (n_unique_samples_export != n_unique_selected_samples){
  stop("Number of unique samples in export file does not match number of selected samples")
}
if (n_unique_patients_export != n_unique_selected_patients - n_missing_patients){
  stop("Number of unique patients in export file does not match number of selected patients")
}
if (!all(patient_output$record_id %in% selected_cases)){
  stop("Some patients in export file are not in selected patients")
}
# There is expected NA, because the export file is technically two csvs concatenated together
if (!all(unique(na.omit(patient_output$cpt_genie_sample_id)) %in% samples_per_patient)){
  stop("Some samples in export file are not in selected samples")
}

print("output and upload")
# output and upload ----------------------------
write.csv(patient_output,file = output_file_name,quote = TRUE,row.names = FALSE,na = "")
act <- Activity(name = 'export main GENIE data', 
                description='Export selected BPC patient data from main GENIE database',
                used = c(clinical_sample_id, clinical_patient_id, in_file),
                executed = 'https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/case_selection/export_bpc_selected_cases.R')
syn_file <- File(output_file_name, 
                 parent=out_folder,
                 name=output_entity_name,
                 annotations=list(phase=sub("phase ", "", phase),cohort=cohort,site=site))
syn_file <- synStore(syn_file)
synSetProvenance(syn_file,act)
file.remove(output_file_name)
