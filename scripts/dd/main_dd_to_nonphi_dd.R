# Description: Convert BPC REDCap Data Dictionary to non-PHI Data Dictionary.
# Author: Haley Hunter-Zinck
# Date: 2021-10-13

# setup ----------------------------

tic = as.double(Sys.time())

library(optparse)
library(glue)
library(dplyr)
library(synapser)
synLogin()
source("fxns_dd_to_nonphi_dd.R")

# user input ----------------------------

option_list <- list( 
  make_option(c("-d", "--synid_file_dd"), action="store", default = NULL,
              help = "Synapse ID of data dictionary file"),
  make_option(c("-f", "--synid_folder_dd"), action="store", default = NULL,
              help = "Synapse ID of folder to store non-PHI data dictionary")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(cond = !is.null(opt$synid_file_dd), 
          msg = "Usage: Rscript main_dd_to_nonphi_dd.R -h")

synid_file_dd <- opt$synid_file_dd
synid_folder_dd <- opt$synid_folder_dd

# check user input ----------------------------

waitifnot(cond = is_synapse_entity_id(synid_file_dd), 
          msg = glue("'{synid_file_dd}' argument must be a Synapse ID.  Please check inputs.  "))

if (!is.null(synid_folder_dd)) {
  waitifnot(cond = is_synapse_entity_id(synid_folder_dd), 
            msg = glue("'{synid_folder_dd}' argument must be a Synapse ID.  Please check inputs.  "))
}

# read ----------------------------

dd_file_name <- get_synapse_download_name(synid_file_dd, keep_download = T)
dd <- get_synapse_entity_data_in_csv(synid_file_dd, 
                                     na.strings = c(""), 
                                     check_names = F)

# main ----------------------------

nonphi_dd <- remove_phi_from_dd(dd)

# write ----------------------------

file_local <- glue("nonphi_{dd_file_name}")
write.csv(nonphi_dd, na = "", row.names = F, file = file_local)

if (!is.null(synid_folder_dd)) {

  synapse_file_name <- "Data Dictionary non-PHI"
  save_to_synapse(path = file_local,
                  parent_id = synid_folder_dd,
                  file_name = synapse_file_name,
                  prov_name = "non-PHI data dictionary",
                  prov_desc = "BPC data dictionary with PHI columns removed",
                  prov_used = synid_file_dd,
                  prov_exec = "https://github.com/Sage-Bionetworks/genie-bpc-pipeline/tree/develop/scripts/dd/main_dd_to_nonphi_dd.R")

  file.remove(file_local)
}

# close out ----------------------------

toc = as.double(Sys.time())
if (!is.null(synid_folder_dd)) {
  print(glue("Non-PHI data dictionary of {synid_file_dd} stored to {synid_folder_dd} as '{synapse_file_name}'."))
} else {
  print(glue("Non-PHI data dictionary of {synid_file_dd} written to '{file_local}'."))
}

print(glue("Runtime: {round(toc - tic)} s"))
