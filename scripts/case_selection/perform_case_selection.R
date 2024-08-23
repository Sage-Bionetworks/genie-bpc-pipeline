# Description: Perform BPC case selection by first constructing an eligibility matrix
#   and then returning all records that fit the eligibility criteria.  Also, sample
#   the requested number of SDV and IRR cases.  
# Author: Haley Hunter-Zinck, Xindi Guo
# Date: 2021-09-22

# pre-setup --------------------------

library(optparse)
library(yaml)
library(glue)
source("shared_fxns.R")

# read in all global parameters
waitifnot(cond = file.exists("config.yaml"), msg = glue("File 'config.yaml' does not exist.  Is '{getwd()}' the correct working directory?"))
config <- read_yaml("config.yaml")

# user input --------------------------

option_list <- list( 
  make_option(c("-p", "--phase"), type = "character",
              help="BPC phase"),
  make_option(c("-c", "--cohort"), type = "character",
              help="BPC cohort"),
  make_option(c("-s", "--site"), type = "character",
              help="BPC site")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(!is.null(opt$phase) && !is.null(opt$cohort) && !is.null(opt$site),
          msg = "Usage: Rscript workflow_case_selection.R -h")

phase <- opt$phase
cohort <- opt$cohort
site <- opt$site

# check user input -----------------

phase_str <- paste0(names(config$phase), collapse = ", ")
waitifnot(is.element(phase, names(config$phase)),
          msg = c(glue("Error: phase {phase} is not valid.  Valid values: {phase_str}"),
                  "Usage: Rscript perform_case_selection.R -h"))

cohort_in_config <- names(config$phase[[phase]]$cohort)
cohort_str <- paste0(cohort_in_config, collapse = ", ")
waitifnot(is.element(cohort, cohort_in_config),
          msg = c(glue("Error: cohort {cohort} is not valid for phase {phase}.  Valid values: {cohort_str}"),
                  "Usage: Rscript perform_case_selection.R -h"))

sites_in_config <- get_sites_in_config(config, phase, cohort)
site_str <- paste0(sites_in_config, collapse = ", ")
waitifnot(is.element(site, sites_in_config),
          msg = c(glue("Error: site {site} is not valid for phase {phase} and cohort {cohort}.  Valid values: {site_str}"),
                  "Usage: Rscript perform_case_selection.R -h"))

# additional parameters
flag_additional <- grepl(pattern = "addition", x = phase)

if (!flag_additional){
  if(get_production(config, phase, cohort, site) == 0) {
    stop(glue("Production target is 0 for phase {phase} {site} {cohort}.  Please revise eligibility criteria."))
  }
}  

# setup ----------------------------

tic = as.double(Sys.time())

library(RCurl)
library(jsonlite)
library(dplyr)
library(lubridate)
library(synapser)
synLogin()

# set random seed
default_site_seed <- config$default$site[[site]]$seed
cohort_site_seed <- config$phase[[phase]]$cohort[[cohort]]$site[[site]]$seed
site_seed <- if (!is.null(cohort_site_seed)) cohort_site_seed else default_site_seed
set.seed(site_seed)

# output files
file_matrix <- tolower(glue("{cohort}_{site}_phase{phase}_eligibility_matrix.csv"))
file_selection <- tolower(glue("{cohort}_{site}_phase{phase}_case_selection.csv"))
file_add <- tolower(glue("{cohort}_{site}_phase{phase}_samples.csv"))

# misc parameters
debug <- config$misc$debug

# functions ----------------------------

is_double_value <- function(x) {
  res <- tryCatch({
    as.double(x)
  }, error = function(cond){
    return(NA)
  }, warning = function(cond){
    return(NA)
  }, finally = {
  })
  
  if (is.na(res)) {
    return(F)
  }
  return(T)
}

is_double <- function(x) {
  return(apply(as.matrix(x), 1, is_double_value))
}

get_count_from_percentage <- function(total, perc, min_count = 1) {
  return(max(min_count, round(total*perc/100)))
}

get_seq_dates <- function(config, phase, cohort, site) {
  
  seq_dates <- c()
  
  if (!is.null(config$phase[[phase]]$cohort[[cohort]]$site[[site]]$date)) {
    seq_dates <- config$phase[[phase]]$cohort[[cohort]]$site[[site]]$date
  } else {
    seq_dates <- config$phase[[phase]]$cohort[[cohort]]$date
  }
  
  return(seq_dates)
}

get_patient_ids_in_release <- function(synid_file_release) {
  
  data <- get_synapse_entity_data_in_csv(synid_file_release)
  return(unlist(data$record_id))
}

get_patient_ids_bpc_removed <- function(synid_table_patient_removal, cohort) {
  query <- glue("SELECT record_id FROM {synid_table_patient_removal} WHERE {cohort} = 'true'")
  res <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  return(res)
}

get_sample_ids_bpc_removed <- function(synid_table_sample_removal, cohort) {
  query <- glue("SELECT SAMPLE_ID FROM {synid_table_sample_removal} WHERE {cohort} = 'true'")
  res <- as.character(unlist(as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))))
  return(res)
}

#' Hack to deal with consolidating site codes in main GENIE>
#' 
#' @param site main GENIE site code
#' @return site codes corresponding to site code in current release.
get_site_list <- function(site) {
  if (site == "PROV") {
    return(c("PROV", "SCI"))
  }
  
  return(site)
}

#' Create data matrix with all necessary information to determine 
#' eligibility for BPC cohort case selection. 
#' 
#' @param patient Data from the data_clinical_patient.txt file from a GENIE
#'                consortium release
#' @param sample Data from the data_clinical_sample.txt file from a GENIE
#'                consortium release
#' @return Matrix of data elements for all samples in the consortium data files.
#' @example 
#'   get_eligibility_data(patient, sample)
get_eligibility_data <- function(synid_table_patient, synid_table_sample, site) {
  
  # read table data
  patient_data <- as.data.frame(synTableQuery(query = glue("SELECT PATIENT_ID, 
                                                                  CENTER,
                                                                  YEAR_DEATH,
                                                                  INT_CONTACT
                                                                  FROM {synid_table_patient}"),
                                             includeRowIdAndRowVersion = F)) 
  sample_data <- as.data.frame(synTableQuery(query = glue("SELECT PATIENT_ID, 
                                                                  SAMPLE_ID,
                                                                  ONCOTREE_CODE,
                                                                  SEQ_DATE,
                                                                  SEQ_YEAR,
                                                                  AGE_AT_SEQ_REPORT
                                                                  FROM {synid_table_sample}"),
                                              includeRowIdAndRowVersion = F)) 
  
  sites <- get_site_list(site)
  
  # merge and filter
  data <- patient_data %>% 
    inner_join(sample_data, by = "PATIENT_ID") %>%  
    filter(is.element(CENTER, sites)) %>%
    select(PATIENT_ID, 
           SAMPLE_ID, 
           ONCOTREE_CODE, 
           SEQ_DATE, 
           AGE_AT_SEQ_REPORT,
           SEQ_YEAR,
           YEAR_DEATH,
           INT_CONTACT)
  
  return(data)
}

#' Create a matrix of both data and flags for exclusion criteria for all
#' samples in GENIE for eligibility for BPC cohort.  
#' 
#' @param data matrix of all necessary data elements for each sample in order
#'             to determine eligibility.
#' @param allowed_codes character vector eligible OncoTree codes
#' @param seq_min earliest eligible sequencing date (format: %b-%Y)
#' @param seq_min latest eligible sequencing date (format: %b-%Y)
#' @return Matrix of data elements used to determine eligibility and flags indicating 
#'   inclusion or exclusion for a given eligibility criteria check.
#' @example
#' create_eligibility_matrix(data = get_eligibility_data(patient, sample))
create_eligibility_matrix <- function(data, 
                                      allowed_codes, 
                                      seq_min, 
                                      seq_max,
                                      exclude_patient_id = c(),
                                      exclude_sample_id = c()) {
  
  mat <- data %>% 
    
    # valid oncotree code
    mutate(FLAG_ALLOWED_CODE = is.element(ONCOTREE_CODE, allowed_codes)) %>%   
    
    # >=18 years old at sequencing
    mutate(FLAG_ADULT = AGE_AT_SEQ_REPORT != '<6570') %>%            
    
    # sequenced within specified time range
    mutate(FLAG_SEQ_DATE = my(SEQ_DATE) >= my(seq_min) & my(SEQ_DATE) <= my(seq_max)) %>%
    
    # patient was alive at sequencing
    mutate(SEQ_ALIVE_YR = !is_double(YEAR_DEATH) | YEAR_DEATH >= SEQ_YEAR)  %>% 

    mutate(SEQ_ALIVE_INT = !is_double(INT_CONTACT) | INT_CONTACT >= AGE_AT_SEQ_REPORT) %>%
    
    # patient not explicitly excluded
    mutate(FLAG_NOT_EXCLUDED = !is.element(PATIENT_ID, exclude_patient_id) & !is.element(SAMPLE_ID, exclude_sample_id))  %>% 

    select(PATIENT_ID, 
           SAMPLE_ID, 
           ONCOTREE_CODE, 
           AGE_AT_SEQ_REPORT,
           INT_CONTACT,
           SEQ_DATE,
           SEQ_YEAR,
           YEAR_DEATH,
           SEQ_ALIVE_INT,
           FLAG_ALLOWED_CODE, 
           FLAG_ADULT, 
           FLAG_SEQ_DATE, 
           SEQ_ALIVE_YR,
           FLAG_NOT_EXCLUDED)         
  
  return(mat)
}

#' Get patient ID, and group relevant sample IDs, of the eligible cohort.  
#' 
#' @param x Eligibility matrix.
#' @param randomize if TRUE, cohort should be randomly shuffled; 
#'                  if FALSE, return order as is
#' @return Matrix of patient and sample ID pairs.  
#' @example
#' 
get_eligible_cohort <- function(x, randomize = T) {
  
  mod <- x
  
  col_flags <- grep(pattern = "^FLAG_", x = colnames(x), value = T, invert = F)
  mod$flag_eligible <- apply(x[,col_flags], 1, all)
  
  # determine eligible samples (all flags TRUE)
  eligible <- as.data.frame(mod %>%
    filter(flag_eligible) %>% 
    group_by(PATIENT_ID) %>%
    summarize(SAMPLE_IDS = paste0(SAMPLE_ID, collapse = ";"))%>%
    select(PATIENT_ID, SAMPLE_IDS))
  
  if (nrow(eligible) == 0) {
    stop(glue("Number of eligible samples for phase {phase} {site} {cohort} is 0.  Please revise eligibility criteria."))
  }

  # randomize cohort
  final <- list()
  if (randomize) {
    final <- eligible %>%
        sample_n(size = nrow(eligible)) %>%
        mutate(order = c(1:nrow(eligible))) %>%
        select(order, PATIENT_ID, SAMPLE_IDS)
  } else {
    final <- eligible %>%
        mutate(order = c(1:nrow(eligible))) %>%
        select(order, PATIENT_ID, SAMPLE_IDS) 
  }
  
  return(final)
}

create_selection_matrix <- function(eligible_cohort, n_prod, n_pressure, n_sdv, n_irr) {
  
  n_eligible <- nrow(eligible_cohort)
  
  if (n_eligible < n_prod) {
    stop(glue("not enough eligible patients for production target ({n_eligible} < {n_prod}) for phase {phase} {site} {cohort}.  Please revise eligibility criteria."))
  }
  
  # randomly disperse additional SDV cases among non-pressure
  col_sdv <- rep("", n_eligible)
  set.seed(site_seed)
  idx_sdv <- sample((n_pressure+1):n_prod, n_sdv)
  col_sdv[1:n_pressure] <- "sdv"
  col_sdv[idx_sdv] <- "sdv"
  
  # randomly disperse addition IRR cases among non-pressure and non-sdv
  col_irr <- rep("", n_eligible)
  set.seed(site_seed)
  idx_irr <- sample(setdiff((n_pressure+1):n_prod, idx_sdv), n_irr)
  col_irr[idx_irr] <- "irr"
  
  categorized_cohort <- eligible_cohort %>%
    mutate(pressure = c(rep("pressure", n_pressure), rep("", n_eligible - n_pressure))) %>%
    mutate(sdv = col_sdv) %>%
    mutate(irr = col_irr) %>%
    mutate(category = c(rep("production", n_prod), rep("extra", n_eligible - n_prod))) %>%
    select(order, PATIENT_ID, SAMPLE_IDS, pressure, sdv, irr, category)
  return(categorized_cohort)
}

# main ----------------------------

if (debug) {
  print(glue("{now(timeOnly = T)}: querying data to determine eligibility..."))
}

eligibility_data <- get_eligibility_data(synid_table_patient = config$synapse$main_patient$id, 
                                         synid_table_sample = config$synapse$main_sample$id, 
                                         site = site)

if (debug) {
  print(glue("{now(timeOnly = T)}: calculating eligibility criteria..."))
}

exclude_patient_id <- c()
exclude_sample_id <- c()
seq_dates <- get_seq_dates(config, phase, cohort, site)

flag_prev_release <- (config$release$cohort[[cohort]]$patient_level_dataset != "NA")
if (phase == 2 && flag_prev_release) {
  exclude_patient_id <- get_patient_ids_in_release(synid_file_release = config$release$cohort[[cohort]]$patient_level_dataset)
  exclude_patient_id <- append(exclude_patient_id, 
                               get_patient_ids_bpc_removed(synid_table_patient_removal = config$synapse$bpc_removal_patient$id, 
                                                           cohort = cohort))
  exclude_sample_id <- get_sample_ids_bpc_removed(synid_table_sample_removal = config$synapse$bpc_removal_sample$id, 
                                                  cohort = cohort)
}

eligibility_matrix <- create_eligibility_matrix(data = eligibility_data, 
                                                allowed_codes = config$phase[[phase]]$cohort[[cohort]]$oncotree$allowed_codes, 
                                                seq_min = seq_dates$seq_min, 
                                                seq_max = seq_dates$seq_max,
                                                exclude_patient_id = exclude_patient_id,
                                                exclude_sample_id = exclude_sample_id)

if (debug) {
  print(glue("{now(timeOnly = T)}: extracting eligible patient IDs..."))
}

eligible_cohort <- get_eligible_cohort(x = eligibility_matrix, randomize = T) 

if (debug) {
  print(glue("{now(timeOnly = T)}: conducting case selection..."))
}

# assign case selection categories
if (flag_additional) {
  
  query <- glue("SELECT record_id AS PATIENT_ID FROM {config$synapse$bpc_patient$id} WHERE cohort = '{cohort}' AND redcap_data_access_group = '{site}'")
  bpc_pat_ids <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
  
  added_sam <- eligible_cohort %>% 
    filter(is.element(PATIENT_ID, unlist(bpc_pat_ids))) %>%
    select(PATIENT_ID, SAMPLE_IDS)
  
  added_sam$ALREADY_IN_BPC <- rep(F, nrow(added_sam))
  if (nrow(added_sam)) {
    for (i in 1:nrow(added_sam)) {
      ids_sam <- added_sam[i, "SAMPLE_IDS"]
      str_ids_sam <- paste0("'", paste0(unlist(strsplit(ids_sam, split = ";")), collapse = "','"), "'")
      query <- glue("SELECT cpt_genie_sample_id FROM {config$synapse$bpc_sample$id} WHERE cpt_genie_sample_id IN ({str_ids_sam})")
      res <- as.data.frame(synTableQuery(query, includeRowIdAndRowVersion = F))
      if (nrow(res)) {
        added_sam$ALREADY_IN_BPC[i] <- T
      }
    }
  } 
} else {
  case_selection <- create_selection_matrix(eligible_cohort = eligible_cohort,
                                            n_prod = get_production(config, phase, cohort, site), 
                                            n_pressure = get_pressure(config, phase, cohort, site), 
                                            n_sdv = get_sdv(config, phase, cohort, site), 
                                            n_irr = get_irr(config, phase, cohort, site))
}

# write locally -----------------------

if (debug) {
  print(glue("{now(timeOnly = T)}: writing eligibility matrix and case selection to file..."))
}


n_unique_patients_eligible_matrix = length(unique(eligibility_matrix$PATIENT_ID))
n_unique_samples_eligible_matrix = length(unique(eligibility_matrix$SAMPLE_ID))
n_unique_selected_patients = length(unique(eligible_cohort$PATIENT_ID))
n_unique_selected_samples = length(unique(eligible_cohort$SAMPLE_ID))

if (debug) {
  print("validation")
  print(paste("export file N unique patients", n_unique_patients_eligible_matrix))
  print(paste("export file N unique samples", n_unique_samples_eligible_matrix))
  print(paste("N Unique selected patients", n_unique_selected_patients))
  print(paste("N Unique selected samples", n_unique_selected_samples))
}
if (n_unique_samples_eligible_matrix != n_unique_selected_samples){
  stop("Number of unique samples in eligibility matrix file does not match number of selected samples")
}
if (n_unique_patients_eligible_matrix != n_unique_selected_patients){
  stop("Number of unique patients in eligibility matrix file does not match number of selected patients")
}
if (!all(eligibility_matrix$PATIENT_ID %in% eligibility_matrix$PATIENT_ID)){
  stop("Some patients in eligibility matrix file are not in selected patients")
}
# There is expected NA, because the export file is technically two csvs concatenated together
if (!all(eligibility_matrix$SAMPLE_ID %in% eligibility_matrix$SAMPLE_ID)){
  stop("Some samples in eligibility matrix file are not in selected samples")
}


if (flag_additional) {
  write.csv(added_sam, file = file_add, row.names = F)
} else {
  write.csv(eligibility_matrix, file = file_matrix, row.names = F)
  write.csv(case_selection, file = file_selection, row.names = F)
}

# close out ----------------------------

if (debug && flag_additional) {
  print(glue("Summary:"))
  print(glue("  Phase: {phase}"))
  print(glue("  Cohort: {cohort}"))
  print(glue("  Site: {site}"))
  print(glue("  Total number of additional samples: {nrow(added_sam)}"))
  print(glue("Outfile: {file_add}"))
} else {
  print(glue("Summary:"))
  print(glue("  Phase: {phase}"))
  print(glue("  Cohort: {cohort}"))
  print(glue("  Site: {site}"))
  print(glue("  Total number of samples: {nrow(eligibility_data)}"))
  print(glue("  Number of eligible patients: {nrow(eligible_cohort)}"))
  print(glue("  Number of target cases: {get_production(config, phase, cohort, site)}"))
  print(glue("  Number of pressure cases: {get_pressure(config, phase, cohort, site)}"))
  print(glue("  Number of SDV cases (excluding pressure): {get_sdv(config, phase, cohort, site)}"))
  print(glue("  Number of IRR cases: {get_irr(config, phase, cohort, site)}"))
  print(glue("Outfiles: {file_matrix}, {file_selection}"))
}

toc = as.double(Sys.time())

print(glue("Runtime: {round(toc - tic)} s"))
