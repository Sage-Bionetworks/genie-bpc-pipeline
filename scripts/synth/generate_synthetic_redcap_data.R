# Description: Generate a synthetic REDCap academic export from a REDCap data dictionary.
# Author: Haley Hunter-Zinck
# Date: 2021-11-09

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
  make_option(c("-d", "--synid_file_dd"), type = "character",
              help="Synapse ID of REDCap data dictionary"),
  make_option(c("-c", "--cohort"), type = "character", default = "synth_cohort",
              help="Name of the cohort to generate (default: synth_cohort)"),
  make_option(c("-n", "--n_patient"), type = "integer", default = 10, 
              help="Number of synthetic patients to generate (default: 10)"),
  make_option(c("-s", "--site"), type = "character", default = "synth_site",
              help="Name of the site associated with the synthetic dataset (default: synth_site)"),
  make_option(c("-p", "--record_prefix"), type = "character", default = "patient",
              help="Prefix to each synthetic record_id (default: patient)"),
  make_option(c("-r", "--non_repeating_forms"), type = "character", 
              default = "curation_completion;curation_initiation_eligibility;patient_characteristics;quality_assurance",
              help="Name of the site associated with the synthetic dataset (default: curation_completion;curation_initiation_eligibility;patient_characteristics;quality_assurance)"),
  make_option(c("-u", "--upload"), type = "character",
              help="Upload to synapse ID folder (default: NULL)"),
  make_option(c("-v", "--verbose"), action = "store_true", default = F,
              help="Output messaging to user on script progress (default: FALSE)")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(!is.null(opt$synid_file_dd),
          msg = "Rscript template.R -h")

synid_file_dd <- opt$synid_file_dd
cohort <- opt$cohort
n_patient <- opt$n_patient
site <- opt$site
record_prefix <- opt$record_prefix
nr_forms <- strsplit(opt$non_repeating_forms, split = ";")[[1]]
synid_folder_upload <- opt$upload
verbose <- opt$verbose

# setup ----------------------------

tic = as.double(Sys.time())

library(glue)
library(dplyr)
library(synapser)
synLogin()

# functions ----------------------------

now <- function(timeOnly = F, tz = "US/Pacific") {
  
  Sys.setenv(TZ=tz)
  
  if(timeOnly) {
    return(format(Sys.time(), "%H:%M:%S"))
  }
  
  return(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
}

#' Remove leading and trailing whitespace from a string.
#' @param str String
#' @return String without leading or trailing whitespace
trim <- function(str) {
  front <- gsub(pattern = "^[[:space:]]+", replacement = "", x = str)
  back <- gsub(pattern = "[[:space:]]+$", replacement = "", x = front)
  
  return(back)
}

#' Download and load data stored in csv or other delimited format on Synapse
#' into an R data frame.
#' 
#' @param synapse_id Synapse ID
#' @version Version of the Synapse entity to download.  NA will load current
#' version
#' @param set Delimiter for file
#' @param na.strings Vector of strings to be read in as NA values
#' @param header TRUE if the file contains a header row; FALSE otherwise.
#' @param check_names TRUE if column names should be modified for compatibility 
#' with R upon reading; FALSE otherwise.
#' @return data frame
get_synapse_entity_data_in_csv <- function(synapse_id, 
                                           version = NA,
                                           sep = ",", 
                                           na.strings = c("NA"), 
                                           header = T,
                                           check_names = F) {
  
  if (is.na(version)) {
    entity <- synGet(synapse_id)
  } else {
    entity <- synGet(synapse_id, version = version)
  }
  
  data <- read.csv(entity$path, stringsAsFactors = F, 
                   na.strings = na.strings, sep = sep, check.names = check_names,
                   header = header)
  return(data)
}

#' Create a RCC import template from the data dictionary.  
#' 
#' @param dd data frame representing REDCap data dictionary for BPC PRISSMM
#' @return vector representing the import template
create_import_template <- function(dd) {
  
  # record id column
  template <- dd$`Variable / Field Name`[1]
  
  # rcc columns
  template <- c(template, "redcap_repeat_instrument", "redcap_repeat_instance", "redcap_data_access_group")
  
  # iterate through variables in data dictionary order
  form_prev <- dd$`Form Name`[2]
  for (i in 2:length(dd$`Variable / Field Name`)) {
    
    form_curr <- dd$`Form Name`[i]
    var_name <- dd$`Variable / Field Name`[i]
    
    # add complete column
    if (form_curr != form_prev) {
      template <- c(template, paste0(form_prev, "_complete"))
    }
    
    # if check box
    if (dd$`Field Type`[i] == "checkbox") {
      choice_str <- unlist(dd %>% 
                             filter(`Variable / Field Name` == var_name) %>%
                             select(`Choices, Calculations, OR Slider Labels`))
      choice_code <- trim(unlist(lapply(strsplit(strsplit(choice_str, split = "\\|")[[1]], split = ", "), head, n = 1)))
      template <- c(template, paste0(var_name, "___", choice_code))
    } else {
      template <- c(template, var_name)
    }
    
    form_prev <- form_curr
  }
  
  # add last complete columns
  form_curr <- tail(dd$`Form Name`, 1)
  template <- c(template, paste0(form_prev, "_complete"))
  
  return(template)
}

#' Parse the REDCap choices values that maps codes to text labels.
#' 
#' @param choices REDCap formatted string of code and choice mappings
#' @param delim_item Delimeter for separating different code-label mapping pairs
#' @param delim_code Delimeter for separating a code and label in a pairing
#' @return Character vector with values as codes and names as labels
parse_choices <- function(choices, delim_item = "\\|", delim_code = ", ") {
  parsed <- strsplit(strsplit(choices, split = delim_item)[[1]], split = delim_code)
  codes <- unlist(lapply(parsed, head, n = 1))
  values <- unlist(lapply(parsed, tail, n = 1))
  return(setNames(codes, values))
}

#' Parse out the REDCap code choices from a variable's choices string.
#' 
#' @param choices REDCap formatted string of code and choice mappings
#' @param delim_item Delimeter for separating different code-label mapping pairs
#' @param delim_code Delimeter for separating a code and label in a pairing
#' @return Character vector with values as codes and names as labels
get_codes <- function(choices, delim_item = "\\|", delim_code = ", ") {
  parsed <- strsplit(strsplit(choices, split = delim_item)[[1]], split = delim_code)
  codes <- trim(unlist(lapply(parsed, head, n = 1)))
  return(codes)
}

#' Generate a synthetic value for a REDCap data variable.
#' 
#' @param dd data frame representing the REDCap data dictionary
#' @param var_name name of the variable 
#' @return character value or vector if checkbox
generate_synthetic_redcap_value <- function(dd, var_name, patient_id, instance_no, lambda = 10) {
  
  var_type <- unlist(dd %>% filter(`Variable / Field Name` == var_name) %>% select(`Field Type`))
  var_val <- unlist(dd %>% filter(`Variable / Field Name` == var_name) %>% select(`Text Validation Type OR Show Slider Number`))
  var_choices <- unlist(dd %>% filter(`Variable / Field Name` == var_name) %>% select(`Choices, Calculations, OR Slider Labels`))
  
  if (grepl(pattern = "sample_id", x = var_name)) {
    return(glue("{patient_id}-0{instance_no}"))
  }
  
  if (var_type == "text") {
    
    if (is.na(var_val) || is.element(var_val, c('integer', 'number'))) {
      return(rpois(1, lambda = lambda))
    }
    
    if (var_val == 'datetime_seconds_mdy') {
      return("5000-01-01 00:00:00")
    }
    
    if (var_val == 'alpha_only') {
      mod <- gsub(pattern = "[[:digit:]]", replacement = 'x', x = var_name)
      mod <- gsub(pattern = "[[:punct:]]", replacement = 'x', x = mod)
      
      return(mod)
    }
    
    if (var_val == 'date_mdy') {
      return("5000-01-01")
    }
    
    return(var_name)
  }
  
  if (is.element(var_type, c("dropdown", "radio"))) {
    codes <- get_codes(var_choices)
    return(sample(codes, 1))
  }
  
  if (var_type == "yesno") {
    return(sample(c(0,1), 1))
  }
  
  if (var_type == "checkbox") {
    codes <- get_codes(var_choices)
    var_names <- paste0(var_name, "___", codes)
    return(setNames(sample(c(0,1), length(var_names), replace = T), var_names))
  }
  
  return(NULL)
}

p_remove_prefix <- function(x, delim = "\\.") {
  splt <- strsplit(x, split = delim)[[1]]
  
  if (length(splt) == 1) {
    return(x)
  }
  
  return(splt[2])
}

remove_prefix <- function(x, delim = "\\.") {
  return(unlist(lapply(x, p_remove_prefix, delim = delim)))
}

generate_synthetic_redcap_instance <- function(dd, form_name, patient_id, instance_no) {
  
  var_name <- unlist(dd %>% filter(`Form Name` == form_name) %>% select(`Variable / Field Name`))
  instance <- lapply(as.list(var_name), generate_synthetic_redcap_value, 
                     dd = dd, 
                     patient_id = patient_id, 
                     instance_no = instance_no)
  names(instance) <- var_name
  instance <- unlist(instance)
  names(instance) <- remove_prefix(names(instance))
  
  instance["record_id"] = patient_id
  instance["redcap_repeat_instance"] <- instance_no
  instance["redcap_repeat_instrument"] <- form_name

  return(instance)
}

generate_synthetic_redcap_instrument <- function(dd, form_name, patient_id, 
                                                 n_instance = NA, lambda = 1,
                                                 nr_forms = NULL,
                                                 verbose = F) {
  
  if (verbose) {
    print(glue("{now()} | creating instrument '{form_name}' for synthetic patient '{patient_id}'..."))
  }
  
  instrument <- c()
  
  # determine number of instances of instrument to simulate
  if (is.na(n_instance)) {
    if (!is.null(nr_forms) && is.element(form_name, nr_forms)) {
      n_instance <- 1
    } else {
      n_instance <- rpois(n = 1, lambda = lambda) + 1
    }
  } 
  
  for (i in 1:n_instance) {
    
    if (verbose) {
      print(glue("{now()} | creating instance {i} of {n_instance} for instrument '{form_name}' for synthetic patient '{patient_id}'..."))
    }
    
    instrument <- rbind(instrument, generate_synthetic_redcap_instance(dd = dd, 
                                                                       form_name = form_name,
                                                                       patient_id = patient_id,
                                                                       instance_no = i))
  }
  
  instrument <- as.data.frame(instrument)
  instrument$record_id <- rep(patient_id, n_instance)
  if (!is.element(form_name, nr_forms)) {
    instrument$redcap_repeat_instrument <- rep(form_name, n_instance)
  } else {
    instrument$redcap_repeat_instrument <- rep(NA, n_instance)
  }
  
  instrument$redcap_repeat_instance <- c(1:n_instance)
  
  return(instrument)
}

generate_synthetic_redcap_patient <- function(dd, patient_id, 
                                              nr_forms = NULL,
                                              verbose = F) {
  
  if (verbose) {
    print(glue("{now()} | creating synthetic patient '{patient_id}'..."))
  }
  
  patient <- list()
  u_form <- unique(dd$`Form Name`)
  for (form_name in u_form) {
    
    patient[[form_name]] <- generate_synthetic_redcap_instrument(dd = dd, 
                                         form_name = form_name, 
                                         patient_id = patient_id, 
                                         n_instance = NA,
                                         nr_forms = nr_forms,
                                         verbose = verbose) 
  }
  return(patient)
}

generate_synthetic_redcap_dataset <- function(dd, n_patient = 100, site = "synth_site",
                                              record_prefix = "patient",
                                              nr_forms = NULL,
                                              verbose = verbose) {
  
  if (verbose) {
    print(glue("{now()} | initiating creation of REDCap synthetic dataset..."))
  }
  
  template <- create_import_template(dd)
  formatted <- matrix(NA, nrow = 0, ncol = length(template), dimnames = list(c(), template))
  
  n_digit <- max(nchar(as.character(seq_len(n_patient))))+1
  patient_ids <- paste0(record_prefix, "-", site, "-", sprintf(glue("%0{n_digit}d"), 1:n_patient))
  dataset <- sapply(patient_ids, generate_synthetic_redcap_patient, dd = dd, 
                    nr_forms = nr_forms, verbose = verbose)
  
  for (j in 1:ncol(dataset)) {
    for (i in 1:nrow(dataset)) {
      
      new_data <- as.matrix(dataset[i,j][[1]])
      
      for (k in 1:nrow(new_data)) {
        formatted <- rbind(formatted, rep(NA, length(template)))
      }
      
      idx <- (nrow(formatted) - nrow(new_data) + 1):nrow(formatted)
      formatted[idx, colnames(new_data)] <- new_data
    }
  }
  
  formatted[,"redcap_data_access_group"] = site
  
  return(formatted)
}

#' Store a file on Synapse with options to define provenance.
#' 
#' @param path Path to the file on the local machine.
#' @param parent_id Synapse ID of the folder or project to which to load the file.
#' @param file_name Name of the Synapse entity once loaded
#' @param prov_name Provenance short description title
#' @param prov_desc Provenance long description
#' @param prov_used Vector of Synapse IDs of data used to create the current
#' file to be loaded.
#' @param prov_exec String representing URL to script used to create the file.
#' @return TRUE if successful, otherwise return error.
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

# main ----------------------------

dd <- get_synapse_entity_data_in_csv(synid_file_dd, na.strings = c(""))
synth <- generate_synthetic_redcap_dataset(dd = dd, 
                                           n_patient = n_patient, 
                                           site = site,
                                           record_prefix = record_prefix,
                                           nr_forms = nr_forms,
                                           verbose = verbose)

# replace cohort code
cohort_str <- dd %>% 
  filter(`Variable / Field Name` == "cur_cohort") %>% 
  select(`Choices, Calculations, OR Slider Labels`)
map_cohort <- matrix(unlist(strsplit(strsplit(x = unlist(cohort_str), split = '|', fixed = T)[[1]], split = ', ')), 
                     ncol = 2, byrow = T, dimnames = list(c(), c("code","value")))
cohort_code <- map_cohort[grep(map_cohort[,"value"], pattern = cohort)]
synth[which(!is.na(synth[,"cur_cohort"])), "cur_cohort"] <- cohort_code

# write ----------------------------

file_output <- glue("{site}_{gsub(pattern = '[ /]', replacement = '-', tolower(cohort))}_synthetic_data.csv")
write.csv(synth, file = file_output, row.names = F, na = "")

if (is.null(synid_folder_upload)) {
  print(glue("File written locally to '{file_output}'"))
} else {
  save_to_synapse(path = file_output, 
                  parent_id = synid_folder_upload, 
                  prov_name = "synthetic redcap dataset", 
                  prov_desc = "synthetically generated dataset sampled from variables defined in a REDCap data dictionary", 
                  prov_used = synid_file_dd, 
                  prov_exec = "https://github.com/Sage-Bionetworks/Genie_processing/blob/synth-bpc/bpc/synth/generate_synthetic_redcap_data.R")
  file.remove(file_output)
  
  print(glue("File stored on Synapse in folder '{synid_folder_upload}'"))
}

# close out ----------------------------

toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
