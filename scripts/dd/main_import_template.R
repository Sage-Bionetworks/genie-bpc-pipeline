# Description: Create import template from non-PHI Data Dictionary.
# Author: Haley Hunter-Zinck
# Date: 2021-10-13

# setup ----------------------------

tic = as.double(Sys.time())

library(optparse)
library(glue)
library(dplyr)
library(synapser)
synLogin()
source("fxns_import_template.R")

# user input ----------------------------

option_list <- list( 
  make_option(c("-d", "--synid_file_dd"), action="store", default = NULL,
              help = "Synapse ID of non-PHI data dictionary file"),
  make_option(c("-f", "--synid_folder_output"), action="store", default = NULL,
              help = "Synapse ID of folder to store generated import template")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(cond = !is.null(opt$synid_file_dd), 
          msg = "Usage: Rscript main_import_template.R -h")

synid_file_dd <- opt$synid_file_dd
synid_folder_output <- opt$synid_folder_output
synapse_file_name <- "Import Template"

# check user input ----------------------------

waitifnot(cond = is_synapse_entity_id(synid_file_dd), 
          msg = glue("'{synid_file_dd}' argument must be a Synapse ID.  Please check inputs.  "))

if (!is.null(synid_folder_output)) {
  waitifnot(cond = is_synapse_entity_id(synid_folder_output), 
            msg = glue("'{synid_folder_output}' argument must be a Synapse ID of a folder.  Please check inputs.  "))
}

# read ----------------------------

dd <- get_synapse_entity_data_in_csv(synid_file_dd)

# main ----------------------------

import <- create_import_template(dd)

# write ----------------------------

file_local <- "import_template.csv"
write(import, sep = ",", ncolumns = length(import), file = file_local)

if (!is.null(synid_folder_output)) {

  save_to_synapse(path = file_local,
                  parent_id = synid_folder_output,
                  file_name = synapse_file_name,
                  prov_name = "import template",
                  prov_desc = "BPC import template generated from non-PHI data dictionary",
                  prov_used = synid_file_dd,
                  prov_exec = "https://github.com/Sage-Bionetworks/Genie_processing/blob/master/bpc/dd/main_import_template.R")

  file.remove(file_local)
}

# close out ----------------------------

toc = as.double(Sys.time())
if (!is.null(synid_folder_output)) {
  print(glue("Import template of {synid_file_dd} stored to {synid_folder_output} as '{synapse_file_name}'."))
} else {
  print(glue("Import template of {synid_file_dd} written to '{file_local}'."))
}

print(glue("Runtime: {round(toc - tic)} s"))
