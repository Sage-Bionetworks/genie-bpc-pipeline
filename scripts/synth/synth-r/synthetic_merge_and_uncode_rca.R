#!/usr/bin/env Rscript

# Description: Merge and uncode synthetic REDCap datasets.
# Author: Haley Hunter-Zinck
# Date: 2021-11-29

# pre-setup ----------------------------

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
  make_option(c("-f", "--synid_file_list"), type = "character",
              help="Comma separated list of Synapse IDs"),
  make_option(c("-d", "--synid_file_dd"), type = "character",
              help="Synapse ID of data dictionary file"),
  make_option(c("-s", "--synid_folder_dest"), type="character", 
              help="Synapse ID of folder in which to save the final dataset"),
  make_option(c("-b", "--bpc"), action="store_true", default = FALSE,
              help="Perform GENIE BPC specific hacks to merged dataset"),
  make_option(c("-o", "--output_prefix"), type = "character", default = "cohort",
              help="Prefix to append to output file name"),
  make_option(c("-v", "--verbose"), action="store_true", default = FALSE,
              help="Output messaging to user on script progress")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(!is.null(opt$synid_file_list) && !is.null(opt$synid_file_dd),
          msg = "Usage: Rscript merge_and_uncode_rca_synthetic.R -h")

synid_file_list <- strsplit(opt$synid_file_list, split = ",")[[1]]
synid_file_dd <- opt$synid_file_dd
synid_folder_dest <- opt$synid_folder_dest
bpc <- opt$bpc
output_prefix <- opt$output_prefix
verbose <- opt$verbose

# setup ----------------------------

tic = as.double(Sys.time())

library(glue)
library(synapser)
synLogin()
library(yaml)
library(dplyr)
library(lubridate)

# configuration
config <- read_yaml("config.yaml")

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
                    stringsAsFactors = F)
  
  # check for header1
  if (length(obj$header1)) {
    ent <- synGet(obj$header1)
    colnames(data1) <- as.character(read.csv(ent$path, check.names = F,
                                             na.strings = c(""),
                                             stringsAsFactors = F))
  }
  
  # check for data 2
  if (length(obj$data2)) {
    ent <- synGet(obj$data2)
    data2 <- read.csv(ent$path, check.names = F,
                      na.strings = c(""), 
                      stringsAsFactors = F)
  }
  
  # check for header2
  if (length(obj$header2)) {
    ent <- synGet(obj$header2)
    colnames(data2) <- as.character(read.csv(ent$path, check.names = F,
                                             na.strings = c(""),
                                             stringsAsFactors = F))
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
merge_datasets <- function(datasets) {
  
  mod <- list()
  
  for (i in 1:length(datasets)) {
    mod[[i]] <- data.frame(lapply(datasets[[i]], as.character), 
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

merge_mapping <- function(primary, secondary, verbose = F) {
  
  u_codes <- union(unlist(primary[,"codes"]), unlist(secondary[,"codes"]))
  mapping <- primary
  
  for (code in u_codes) {
    
    idx_primary <- which(primary[,"codes"] == code)
    idx_secondary <- which(secondary[,"codes"] == code)
    
    if (!length(idx_primary)) {
      
      if (verbose) {
        msg <- paste0(secondary[idx_secondary,], collapse = " - ")
        print(glue("Adding: {msg}"))
      }
    
      mapping <- rbind(mapping, secondary[idx_secondary,])
    }
  }
  
  return(mapping)
}

merge_mappings <- function(primarys, secondarys, verbose = F) {
  
  mappings <- primarys
  u_item <- union(names(primarys), names(secondarys))
  
  for (item in u_item) {
    
    if (!length(primarys[[item]])) {
      mappings[[item]] <- secondarys[[item]]
    } else  {
      mappings[[item]] <- merge_mapping(primarys[[item]], secondarys[[item]], verbose = verbose)
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
uncode_data <- function(df_coded, dd, grs, rca = T) {
  
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
        
        # uncode non-expanded variable according to mapping (same for RCC and RCA)
        df_uncoded[,i] <- uncode_data_column(col_coded = df_coded[,i], 
                                             mapping = mappings[[idx_mapping]])
      } else if (rca && var_name != names(df_coded)[i]) {
        
        # replace 0,1 coding for expanded variables according to representative value (RCA only)
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

format_rca <- function(x, dd, format_instrument = F) {
  
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
  if (format_instrument) {
    mapping_instrument <- data.frame(codes = names(config$mapping$instrument),
                                     values = as.character(config$mapping$instrument),
                                     stringsAsFactors = F)
    x$redcap_repeat_instrument <- as.character(unlist(uncode_data_column(x$redcap_repeat_instrument, mapping = mapping_instrument)))
  }
  
  # add missing columns
  x$redcap_data_access_group <- get_site_from_record_id(x$record_id)
  
  return(x)
}

format_rcc <- function(x, dd) {
  
  mappings <- parse_mappings(strs = dd[[config$column_name$variable_mapping]], 
                                            labels = dd[[config$column_name$variable_name]])
  
  colnames(x) <- uncode_column_names(column_names = colnames(x), 
                                              mappings = mappings)
  
  # modify date time format
  idx_ts <- grep(pattern = "datetime", x = dd$`Text Validation Type OR Show Slider Number`)
  if (length(idx_ts)) {
    x[,idx_ts] <- lapply(x[,idx_ts], convert_string_to_timestamp, format = "%Y-%m-%d %H:%M")
  }
  
  # modify date format
  idx_dt <- grep(pattern = "date_mdy", x = dd$`Text Validation Type OR Show Slider Number`)
  if (length(idx_dt)) {
    x[,idx_dt] <- lapply(x[,idx_dt], convert_string_to_date, format = "%Y-%m-%d")
  }
  
  # modify boolean values values
  idx_ind <- dd$`Variable / Field Name`[which(dd$`Field Type` == "yesno")]
  x[,idx_ind] <- lapply(x[,idx_ind, drop = F], FUN = function(x) {return(tolower(as.logical(as.integer(x))))})
  
  # modify row values
  x$redcap_repeat_instrument[which(is.na(x$redcap_repeat_instrument))] = "no-repeat"
  
  # checkbox variable groups must all be NA or 0/1 for each row
  var_names <- get_root_variable_name(colnames(x))
  agg <- aggregate(colnames(x) ~ var_names, FUN = c)
  for (i in 1:nrow(agg)) {
    
    if (get_field_type(var_name = agg[i,1], dd = dd) == "checkbox") {
      
      if (length(unlist(agg[i,2]))) {
        
        n_na <- apply(as.matrix(x[,unlist(agg[i,2])]), 1, function(x) {return(length(which(!is.na(x))))})
        
        idx <- which(n_na > 0 & n_na < length(unlist(agg[i,2])))
        if (length(idx) > 0) {
          x[idx, unlist(agg[i,2])] <- replace_na(x[idx, unlist(agg[i,2])], replacement = 0)
        }
        
      }
    }
  }

  # add missing columns
  site <- get_site_from_record_id(x$record_id)
  status <- rep("Enrolled", nrow(x))
  x = cbind(x, site, status)
  
  # modify column names
  colnames(x)[which(colnames(x) == "site")] <- "Site Name"
  colnames(x)[which(colnames(x) == "status")]<- "Subject Status"
  colnames(x)[which(colnames(x) == "record_id")] <- "Study Subject ID"
  colnames(x)[which(colnames(x) == "redcap_repeat_instrument")] <- "Event Name(Occurrence)"
  colnames(x)[which(colnames(x) == "redcap_repeat_instance")] <- "Instrument Occurrence"
  colnames(x)[which(colnames(x) == "redcap_ca_index")] <- "_redcap_ca_index"
  colnames(x)[which(colnames(x) == "redcap_ca_seq")] <- "_redcap_ca_seq"

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

# read ----------------------------

if (verbose) {
  print(glue("{now(timeOnly = T)}: Reading data dictionary ({synid_file_dd})..."))
}

dd <- read.csv(synGet(synid_file_dd)$path, 
               sep = ",", 
               stringsAsFactors = F,
               check.names = F,
               na.strings = c(""))

if (verbose) {
  print(glue("{now(timeOnly = T)}: Reading global response set..."))
}

grs <- read.csv(synGet(config$synapse$grs$id)$path, 
                sep = ",", 
                stringsAsFactors = F,
                check.names = F,
                na.strings = c(""))

if (verbose) {
  print(glue("{now(timeOnly = T)}: Reading data uploads..."))
}

data_upload <- list()
for (synid_file in synid_file_list) {
  
  if (verbose) {
    print(glue("  {now(timeOnly = T)}: Reading data in file '{synGet(synid_file, downloadFile = F)$properties$name}' ({synid_file})..."))
  }
  
  data_upload[[synid_file]] <- get_synapse_entity_data_in_csv(synid_file)
}

# main ----------------------------

if (verbose) {
  print(glue("{now(timeOnly = T)}: Merging data..."))
}

# merge
coded <- merge_datasets(data_upload)

if (verbose) {
  print(glue("{now(timeOnly = T)}: Uncoding data..."))
}

# uncode
uncoded <- uncode_data(df_coded = coded, 
                       dd = dd,
                       grs = grs,
                       rca = T)

if (verbose) {
  print(glue("{now(timeOnly = T)}: Formatting uncoded data..."))
}

# format data
uncoded_formatted <- format_rca(uncoded, dd = dd, format_instrument = T)

# BPC-specific hacks
if (bpc) {
  if (is.element("drugs_ca___11", colnames(uncoded_formatted))) {
    uncoded_formatted$drugs_ca___11[which(uncoded_formatted$drugs_ca___11 == "Unknown")] <- 11
  }
}

# write ------------------------------

if (verbose) {
  print(glue("{now(timeOnly = T)}: Writing uncoded data to file locally..."))
}

# write to file(s) locally
file_output_pri <- glue("{tolower(output_prefix)}_synthetic_intake_data.csv")
write.csv(x = uncoded_formatted, file = file_output_pri, row.names = F, na = "")

if (!is.null(synid_folder_dest)) {
  
  if (verbose) {
    print(glue("{now(timeOnly = T)}: Saving uncoded data to Synapse to folder '{synGet(synid_folder_dest)$properties$name}' ({synid_folder_dest})..."))
  }
  
  # write to Synapse and clean up
  save_to_synapse(path = file_output_pri,
                  file_name = gsub(pattern = ".csv|.tsv", replacement = "", x = file_output_pri),
                  parent_id = synid_folder_dest,
                  prov_name = "Uncoded RCA data",
                  prov_desc = "Merged and uncoded RCA data from multiple sites",
                  prov_used = c(synid_file_list, 
                                synid_file_dd,
                                config$synapse$grs$id),
                  prov_exec = "https://github.com/Sage-Bionetworks/genie-bpc-pipeline/blob/develop/scripts/synth/merge_and_uncode_rca_synthetic.R")
  
  # clean up locally
  file.remove(file_output_pri)
}

# close out ----------------------------

toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
