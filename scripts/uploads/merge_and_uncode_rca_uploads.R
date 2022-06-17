# Description: Merge and uncode REDCap uploads for GENIE BPC
#   into REDCap academic format to replace RCC functionality.
# Author: Haley Hunter-Zinck
# Date: September 13, 2021

# pre-setup ----------------------------

library(optparse)
library(yaml)
library(glue)

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

workdir <- "."
if (!file.exists("config.yaml")) {
  workdir <- "/usr/local/src/myscripts"
}

config <- read_yaml(glue("{workdir}/config.yaml"))
cohorts <- names(config$upload)
cohorts_str <- paste0(cohorts, collapse = ", ")

option_list <- list( 
  make_option(c("-c", "--cohort"), type = "character",
              help=glue("BPC cohort (choices: {cohorts_str}, {config$misc$all})")),
  make_option(c("-u", "--save_synapse"), action="store_true", default = FALSE, 
              help="Save output to Synapse"),
  make_option(c("-a", "--synapse_auth"), type = "character", default = NA,
              help="Path to .synapseConfig file or Synapse PAT (default: normal synapse login behavior)"),
  make_option(c("-v", "--verbose"), action="store_true", default = FALSE, 
              help="Print out verbose output on script progress")
)
opt <- parse_args(OptionParser(option_list=option_list))

cohort_input <- opt$cohort
save_on_synapse <- opt$save_synapse
auth <- opt$synapse_auth
debug <- opt$verbose
waitifnot(!is.null(opt$cohort),
          msg = "Usage: Rscript merge_and_uncode_rca_uploads.R -h")

# setup ----------------------------

tic = as.double(Sys.time())

library(synapser)
library(dplyr)
library(lubridate)

# check user input --------------------

waitifnot(is.element(cohort_input, c(cohorts, config$misc$all)), 
            msg = glue("Error: cohort '{cohort_input}' invalid.  Valid values: {cohorts_str}."))

waitifnot(is.element(save_on_synapse, c(T, F)), 
          msg = glue("Error: save_on_synapse value '{args[3]}' invalid.  Valid values: TRUE or FALSE."))

if (cohort_input == config$misc$all) {
  cohort_input = cohorts
}

# functions ----------------------------

now <- function(timeOnly = F, tz = "US/Pacific") {
  
  Sys.setenv(TZ=tz)
  
  if(timeOnly) {
    return(format(Sys.time(), "%H:%M:%S"))
  }
  
  return(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
}

#' Return a file upload data corresponding to a cohort-site pair as an R object.
#'
#' @param cohort cohort of interest
#' @param site site of interest
#' @param obj upload file object from config
#' @return Contents of a cohort-side data upload file as an R data frame.
#' @example
#' get_bpc_data_upload(cohort, site, list(data1 = "syn12345", data2 = "syn6554332", 
#'                           header1 = "syn39857289375, header2 = "syn11111"),
#' get_bpc_data_upload(cohort, site, list(data1 = "syn12345"))
get_bpc_data_upload <- function(cohort, site, obj) {
  
  data <- c()
  data1 <- c()
  data2 <- c()
  
  # get data 1 (default, should always be specified)
  ent <- synGet(obj$data1)
  data1 <- read.csv(ent$path, 
                    check.names = F,
                    na.strings = c(""), 
                    stringsAsFactors = F,
                    colClasses = "character")
  
  # check for header1
  if (length(obj$header1)) {
    ent <- synGet(obj$header1)
    colnames(data1) <- as.character(read.csv(ent$path, check.names = F,
                                             na.strings = c(""),
                                             stringsAsFactors = F,
                                             colClasses = "character"))
  }
  
  # check for data 2
  if (length(obj$data2)) {
    ent <- synGet(obj$data2)
    data2 <- read.csv(ent$path, check.names = F,
                      na.strings = c(""), 
                      stringsAsFactors = F,
                      colClasses = "character")
  }
  
  # check for header2
  if (length(obj$header2)) {
    ent <- synGet(obj$header2)
    colnames(data2) <- as.character(read.csv(ent$path, check.names = F,
                                             na.strings = c(""),
                                             stringsAsFactors = F,
                                             colClasses = "character"))
  }
  
  if (length(obj$data2)) {
    data <- data1 %>% inner_join(data2, by = c("record_id", 
                                               "redcap_repeat_instrument", 
                                               "redcap_repeat_instance"))
  } else {
    data <- data1
  }
  
  return(data)
}

#' Get Synapse IDs of the most recent PRISSMM documentation files
#' for a cohort.
#'
#' @param synid_table_prissmm Synapse ID of the table containing Synapse IDs
#' of all PRISSM documentation
#' @param cohort cohort of interest
#' @file_name name of the type of documentation file of interest
#' @return Synapse ID of the most recent PRISSMM documentation corresponding
#' to the cohort and type of document.
#' @example
#' get_bpc_synid_prissmm("syn12345", "my_cohort", "Import Template")
get_bpc_synid_prissmm <- function(synid_table_prissmm, cohort,
                                  file_name = c("Import Template", "Data Dictionary non-PHI"),
                                  version = NA) {
  
  query <- ""
  if (is.na(version)) {
    query <- glue("SELECT id FROM {synid_table_prissmm} \
                WHERE cohort = '{cohort}' \
                ORDER BY name DESC LIMIT 1")
  } else {
    query <- glue("SELECT id FROM {synid_table_prissmm} \
                WHERE name = 'v{version}'")
  }
  
  ent_folder <- synTableQuery(query, includeRowIdAndRowVersion = F)
  synid_prissmm_children <- as.list(synGetChildren(as.character(
    read.csv(ent_folder$filepath, stringsAsFactors = F))))
  synid_prissmm_children_ids <- setNames(unlist(lapply(synid_prissmm_children,
                                                       function(x) {return(x$id)})),
                                         unlist(lapply(synid_prissmm_children,
                                                       function(x) {return(x$name)})))
  ent <- synGet(as.character(synid_prissmm_children_ids[file_name]))
  synid_file_template <- ent$get("id")
  
  return(synid_file_template)
}

#' Merge several data frames that may have different columns.
#' 
#' @param datasets List of data frames
#' @return Single data frame
#' @example 
#' merge_datasets(datasets = list(a = data_a, b = data_b, c = data_c))
merge_datasets <- function(datasets, cohort) {
  
  mod <- list()
  config_cohort <- config$upload[[cohort]]
  
  for (i in 1:length(datasets)) {
    mod[[i]] <- data.frame(lapply(get_bpc_data_upload(cohort = cohort, 
                                          site = site, 
                                          obj = config_cohort[[i]]), as.character), 
                           stringsAsFactors = F)
  }
  
  merged <- bind_rows(mod)
  return(merged)
}

get_root_variable_name <- function(str) {
  splt <- strsplit(x = str, split = "___")
  root <- unlist(lapply(splt, FUN = function(x) {return(x[1])}))
  return(root)
}

get_site_from_record_id <- function(x) {
  site <- unlist(lapply(strsplit(x = x, split = "-"), FUN = function(x) {return(x[2])}))
  return(site)
}

trim_string <- function(str) {
  front <- gsub(pattern = "^[[:space:]]+", replacement = "", x = str)
  back <- gsub(pattern = "[[:space:]]+$", replacement = "", x = front)
  
  return(back)
}

merge_last_elements <- function(x, delim) {
  
  y <- c()
  y[1] = x[1]
  y[2] <- paste0(x[2:length(x)], collapse = delim)
  return(y)
}

#' Perform string split operation but only on the first
#' occurrence of the split character.
strsplit_first <- function(x, split) {
  
  unmerged <- strsplit(x = x, split = split)
  remerge <- lapply(unmerged, merge_last_elements, delim = split)
  
  return(remerge)
}

parse_mapping <- function(str) {
  
  clean <- trim_string(gsub(pattern = "\"", replacement = "", x = str))
  splt <- strsplit_first(strsplit(x = clean, split = "|", fixed = T)[[1]], split = ",")
  
  codes <- unlist(lapply(splt, FUN = function(x) {return(trim_string(x[1]))}))
  values <- unlist(lapply(splt, FUN = function(x) {return(trim_string(x[2]))}))
  mapping <- data.frame(cbind(codes, values), stringsAsFactors = F)
  
  return(mapping)
}

parse_mappings <- function(strs, labels) {
  
  mappings <- list()
  
  for (i in 1:length(strs)) {
    
    if (!is.na(strs[[i]])) {
      mappings[[labels[i]]] <- parse_mapping(strs[i])
    } else {
      mappings[[labels[i]]] <- NULL
    }
  }
  
  return(mappings)
}

uncode_code <- function(code, mapping) {
  
  idx <- which(mapping[,1] == code)
  
  if (length(idx)) {
    return(mapping[idx, 2])
  }
 
  return(code) 
}

uncode_column_names <- function(column_names, mappings) {
  
  uncoded_names <- column_names
  var_names <- get_root_variable_name(column_names)
  
  idx <- which(var_names != column_names) 
  for (i in 1:length(idx)) {
    
    mapping <- mappings[[var_names[idx[i]]]]
    coded_value <- strsplit(x = column_names[idx[i]], split = "___")[[1]][2]
    uncoded_value <- uncode_code(code = coded_value, mapping = mapping)
    uncoded_names[idx[i]] <- glue("{var_names[idx[i]]}___{uncoded_value}")
  }
  
  return(uncoded_names)
}

uncode_data_column <- function(col_coded, mapping) {
  
  # map coded to uncoded
  col_uncoded <- data.frame(codes = col_coded) %>%
    mutate(codes_chr = as.character(codes)) %>%
    left_join(mapping, by = c("codes_chr" = "codes")) %>%
    select("values")
  
  return(col_uncoded)
}

merge_mapping <- function(primary, secondary, debug = F) {
  
  u_codes <- union(unlist(primary[,"codes"]), unlist(secondary[,"codes"]))
  mapping <- primary
  
  for (code in u_codes) {
    
    idx_primary <- which(primary[,"codes"] == code)
    idx_secondary <- which(secondary[,"codes"] == code)
    
    if (!length(idx_primary)) {
      
      if (debug) {
        msg <- paste0(secondary[idx_secondary,], collapse = " - ")
        print(glue("Adding: {msg}"))
      }
    
      mapping <- rbind(mapping, secondary[idx_secondary,])
    }
  }
  
  return(mapping)
}

merge_mappings <- function(primarys, secondarys, debug = F) {
  
  mappings <- primarys
  u_item <- union(names(primarys), names(secondarys))
  
  for (item in u_item) {
    
    if (!length(primarys[[item]])) {
      mappings[[item]] <- secondarys[[item]]
    } else  {
      mappings[[item]] <- merge_mapping(primarys[[item]], secondarys[[item]], debug = debug)
    }
  }
  
  return(mappings)
}

#' Map any coded data to actual values as mapped in the 
#' REDCap Data Dictionary (DD).
#' 
#' @param data Data frame of coded data
#' @param mappings Matrix with two columns, first containing a label and
#' second columns a mapping string.  
#' @param secondary_mappings Another mapping matrix that is used secondarily
#' if the label is not found in the primary mapping matrix.
#' @return Data frame of uncoded data.
#' @example
#' map_code_to_value(data = my_data, dd = dd, grs = grs)
uncode_data <- function(df_coded, dd, grs) {
  
  df_uncoded <- df_coded
  
  # merge reference mappings
  mappings_primary <- parse_mappings(strs = grs[,config$column_name$rs_value], 
                                     labels = grs[,config$column_name$rs_label])
  mappings_secondary <- parse_mappings(strs = dd[[config$column_name$variable_mapping]], 
                                labels = dd[[config$column_name$variable_name]])
  mappings <- merge_mappings(mappings_primary, mappings_secondary)
  
  # custom mappings
  mapping_complete <- data.frame(codes = names(config$mapping$complete),
                                 values = as.character(config$mapping$complete),
                                 stringsAsFactors = F)

  for (i in 1:ncol(df_coded)) {
    
    var_name <- get_root_variable_name(names(df_coded)[i])
    field_type <- if (is.element(var_name, dd$`Variable / Field Name`)) dd$`Field Type`[which(dd$`Variable / Field Name`  == var_name)] else 'unknown'
    
    if(grepl(pattern = "complete", x = var_name)) {
      
      # complete column
      df_uncoded[which(df_coded[,i] == "0"), i] = "1"
      
      df_uncoded[,i] <- uncode_data_column(col_coded = df_uncoded[,i], 
                                           mapping = mapping_complete)
    } else if (length(which(names(mappings) == var_name))) {
      
      idx_mapping <- which(names(mappings) == var_name)
      
      if (var_name == names(df_coded)[i]) {
        
        # uncode non-expanded variable according to mapping 
        df_uncoded[,i] <- uncode_data_column(col_coded = df_coded[,i], 
                                             mapping = mappings[[idx_mapping]])
      } else if (var_name != names(df_coded)[i]) {
        
        # replace 0,1 coding for expanded variables according to representative value 
        var_code <- strsplit(names(df_coded)[i], split = "___")[[1]][2]
        code_value <- mappings[[idx_mapping]]$values[which(mappings[[idx_mapping]]$codes == var_code)]
        mapping <- data.frame(codes = c("0","1"), values = c(0, code_value))
        df_uncoded[,i] <- uncode_data_column(col_coded = df_coded[,i], 
                                             mapping = mapping)
        
        # make check box coding for unchecked
        if (field_type == "checkbox") {
          
          idx_zero <- which(df_uncoded[,i] == "0")
          df_uncoded[idx_zero, i] <- NA
        }
      }
    }
  }
  
  return(df_uncoded)
}

convert_string_to_timestamp <- function(x, format = "%Y-%m-%d %H:%M") {
  
  result <- x
  
  idx_slash <- which(grepl(x = x, pattern = "/"))
  if (length(idx_slash)) {
    result[idx_slash] <- format(mdy_hm(x[idx_slash]), format)
  }
  
  idx_19 <- setdiff(which(nchar(x) == 19), idx_slash)
  if (length(idx_19)) {
    result[idx_19] <- format(ymd_hms(x[idx_19]), format)
  }
    
  idx_16 <- setdiff(which(nchar(x) == 16 | nchar(x) == 15 & grepl(x = x, pattern = "-")), idx_slash)
  if (length(idx_16)) {
    result[idx_16] <- format(ymd_hm(x[idx_16]), format)
  }
  
  return(result)
}

convert_string_to_date <- function(x, format = "%Y-%m-%d") {
  
  result <- x
  
  idx_slash <- which(grepl(x = x, pattern = "/"))
  if (length(idx_slash)) {
    result[idx_slash] <- format(mdy(x[idx_slash]), format)
  }
  
  return(result)
}

get_field_type <- function(var_name, dd) {
  
  field_type <- if (is.element(var_name, dd$`Variable / Field Name`)) dd$`Field Type`[which(dd$`Variable / Field Name` == var_name)] else 'unknown'
  return(field_type)
}

replace_na <- function(y, replacement) {
  
  y[is.na(y)] <- replacement
  return(y)
}

format_rca <- function(x, dd) {
  
  # modify date time format
  col_ts <- unlist(dd %>% filter(grepl(pattern = "datetime", x = `Text Validation Type OR Show Slider Number`)) %>% select(`Variable / Field Name`))
  if (length(col_ts)) {
    x[,col_ts] <- lapply(x[,col_ts], convert_string_to_timestamp, format = "%Y-%m-%d %H:%M")
  }
  
  # modify date format
  col_dt <- unlist(dd %>% filter(grepl(pattern = "date_mdy", x = `Text Validation Type OR Show Slider Number`)) %>% select(`Variable / Field Name`))
  if (length(col_dt)) {
    x[,col_dt] <- lapply(x[,col_dt], convert_string_to_date, format = "%Y-%m-%d")
  }
  
  # modify boolean values values
  mapping_yesno <- data.frame(codes = names(config$mapping$yesno_rca),
                              values = as.character(config$mapping$yesno_rca),
                              stringsAsFactors = F)
  idx_ind <- dd$`Variable / Field Name`[which(dd$`Field Type` == "yesno")]
  #x[,idx_ind] <- lapply(x[,idx_ind, drop = F], FUN = function(x) {return(uncode_data_column(x, mapping = mapping_yesno))})
  for (idx in idx_ind) {
    x[,idx] <- as.character(unlist(uncode_data_column(x[,idx], mapping = mapping_yesno)))
  }
  
  # modify instrument
  mapping_instrument <- data.frame(codes = names(config$mapping$instrument),
                                   values = as.character(config$mapping$instrument),
                                   stringsAsFactors = F)
  x$redcap_repeat_instrument <- as.character(unlist(uncode_data_column(x$redcap_repeat_instrument, mapping = mapping_instrument)))
  
  # add missing columns
  x$redcap_data_access_group <- get_site_from_record_id(x$record_id)
  
  return(x)
}

remove_irr <- function(data) {
  
  pri <- data
  
  irr_cases <- grep('-2$|_2$', unique(data$record_id), value = T)
  
  if (length(irr_cases)) {
    pri <- data %>%
      filter(!record_id %in% irr_cases)
  }
  
  return(pri)
}

get_irr <- function(data) {
  
  irr <- NULL
  
  irr_cases <- grep('-2$|_2$', unique(data$record_id), value = T)
  
  if (length(irr_cases)) {
    
    irr_plus_cases <- c(irr_cases, gsub(x = irr_cases, pattern = '-2$|_2$', replacement = ''))
    
    irr <- data %>%
      filter(record_id %in% irr_plus_cases)
  } 
  
  return(irr)
}

save_to_synapse <- function(path, parent_id, file_name = NA, prov_name = NA, prov_desc = NA, prov_used = NA, prov_exec = NA) {
  
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

get_data_dictionary <- function(cohort) {
  synid_dd <- get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                    cohort = cohort,
                                    file_name = "Data Dictionary non-PHI")
  
  dd <- read.csv(synGet(synid_dd)$path, 
                 sep = ",", 
                 stringsAsFactors = F,
                 check.names = F,
                 na.strings = c(""))
  
  return(dd)
}

get_data_uploads <- function(cohort) {
  data_upload <- list()
  for (i in 1:length(config$upload[[cohort]])) {
    
    config_cohort <- config$upload[[cohort]]
    site <- names(config_cohort)[i]
    
    data_upload[[site]] <- get_bpc_data_upload(cohort = cohort, 
                                               site = site, 
                                               obj = config_cohort[[site]])
  }
  
  return(data_upload)
}

get_pri_file_name <- function(cohort) {
  return(glue("{cohort}BPCIntake_data.csv"))
}

get_irr_file_name <- function(cohort) {
  return(glue("{cohort}BPCIntake_irr.csv"))
}

#' Write to file(s) locally
write_output_locally <- function(cohort, data_pri, data_irr) {
  
  files_output <- c()
  
  file_output_pri <- get_pri_file_name(cohort)
  files_output <- append(files_output, file_output_pri)
  write.csv(x = data_pri, file = file_output_pri, row.names = F, na = "")
  
  if(!is.null(data_irr)) {
    file_output_irr <- get_irr_file_name(cohort)
    files_output <- append(files_output, file_output_irr)
    write.csv(data_irr, file = file_output_irr, quote = T, row.names = F, na = "")
  }
  
  return(files_output)
}

#' Write to Synapse and clean up
save_output_synapse <- function(cohort) {
  
  parent_id <- config$synapse$rca_files$id
  file_output_pri <- get_pri_file_name(cohort)
  file_output_irr <- get_irr_file_name(cohort)
  synid_dd <- get_bpc_synid_prissmm(synid_table_prissmm = config$synapse$prissmm$id, 
                                                cohort = cohort,
                                                file_name = "Data Dictionary non-PHI")
  
  save_to_synapse(path = file_output_pri,
                  file_name = gsub(pattern = ".csv|.tsv", replacement = "", x = file_output_pri),
                  parent_id = parent_id,
                  prov_name = "BPC non-IRR upload data",
                  prov_desc = "Merged and uncoded BPC upload data from sites academic REDCap instances with IRR cases removed",
                  prov_used = c(as.character(unlist(config$upload[[cohort]])), 
                                synid_dd,
                                config$synapse$grs$id),
                  prov_exec = "https://github.com/Sage-Bionetworks/Genie_processing/blob/master/bpc/uploads/merge_and_uncode_rca_uploads.R")
  
  if (file.exists(file_output_irr)) {
    save_to_synapse(path = file_output_irr,
                    file_name = gsub(pattern = ".csv|.tsv", replacement = "", x = file_output_irr),
                    parent_id = parent_id,
                    prov_name = "BPC IRR upload data",
                    prov_desc = "Merged and uncoded BPC upload IRR case data from sites academic REDCap instances",
                    prov_used = c(as.character(unlist(config$upload[[cohort]])), 
                                  synid_dd,
                                  config$synapse$grs$id),
                    prov_exec = "https://github.com/Sage-Bionetworks/Genie_processing/blob/master/bpc/uploads/merge_and_uncode_rca_uploads.R")
  }
  
  # clean up locally
  file.remove(file_output_pri)
  if (file.exists(file_output_irr)) {
    file.remove(file_output_irr)
  }
}

# synapse login -------------------

status <- synLogin(auth = auth)

# main ----------------------------

if (debug) {
  print(glue("{now(timeOnly = T)}: Reading global response set..."))
}

grs <- read.csv(synGet(config$synapse$grs$id)$path, 
                sep = ",", 
                stringsAsFactors = F,
                check.names = F,
                na.strings = c(""))

# for each user-specified cohort
for (cohort in cohort_input) {
  
  if (debug) {
    print(glue("{now(timeOnly = T)}: Merging and uncoding data for cohort {cohort} -------------------"))
    print(glue("{now(timeOnly = T)}: Reading data dictionary..."))
  }
  
  dd <- get_data_dictionary(cohort)
  
  if (debug) {
    print(glue("{now(timeOnly = T)}: Reading data uploads..."))
  }
  
  data_upload <- get_data_uploads(cohort)
  
  if (debug) {
    print(glue("{now(timeOnly = T)}: Merging data uploads..."))
  }
  
  # merge
  coded <- merge_datasets(data_upload, cohort)
  
  if (debug) {
    print(glue("{now(timeOnly = T)}: Uncoding data uploads..."))
  }
  
  # uncode
  uncoded <- uncode_data(df_coded = coded, 
                         dd = dd,
                         grs = grs)
  
  if (debug) {
    print(glue("{now(timeOnly = T)}: Formatting uncoded data..."))
  }
  
  # format data
  uncoded_formatted <- format_rca(uncoded, dd = dd)
  
  # separate IRR from non-IRR cases
  data_pri <- remove_irr(uncoded_formatted)
  data_irr <- get_irr(uncoded_formatted)
  
  if (debug) {
    print(glue("{now(timeOnly = T)}: Writing uncoded data to file locally..."))
  }
  
  write_output_locally(cohort, data_pri, data_irr)
  
  if (save_on_synapse) {
    
    if (debug) {
      print(glue("{now(timeOnly = T)}: Saving uncoded data to Synapse..."))
    }
    
    save_output_synapse(cohort)
  }
  
  # clean up for memory
  rm(data_pri)
  rm(data_irr)
  rm(uncoded_formatted)
  rm(uncoded)
  rm(coded)
  rm(data_upload)
}

# close out ----------------------------

toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
