# Description: Automatically update HemOnc tables with a new snapshot version.
# Author: Haley Hunter-Zinck
# Date: 2021-11-02

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
  make_option(c("-c", "--concept_file"), type = "character",
              help="Path to file containing HemOnc concepts"),
  make_option(c("-r", "--relationship_file"), type = "character",
              help="Path to file containing HemOnc relationships"),
  make_option(c("-m", "--comment"), type = "character",
              help="Comment for new snapshot version")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(!is.null(opt$concept_file) && !is.null(opt$relationship_file) && !is.null(opt$comment),
          msg = "Rscript update_hemonc_tables.R -h")

file_concept <- opt$concept_file
file_relationship <- opt$relationship_file
comment <- opt$comment

# setup ----------------------------

tic = as.double(Sys.time())

library(glue)
library(synapser)
synLogin()

# synapse
synid_table_concept <- "syn26119153"
synid_table_relationship <- "syn26119155"

# functions ----------------------------


#' Create a Synapse table snapshot version with comment.
#' 
#' @param table_id Synapse ID of a table entity
#' @param comment Message to annotate the new table version
#' @return snapshot version number
#' @example 
#' create_synapse_table_snapshot("syn12345", comment = "my new snapshot")
create_synapse_table_snapshot <- function(table_id, comment) {
  res <- synRestPOST(glue("/entity/{table_id}/table/snapshot"), 
                     body = glue("{'snapshotComment':'{{comment}}'}", 
                                 .open = "{{", 
                                 .close = "}}"))
  
  return(res$snapshotVersionNumber)
}

#' Clear all rows from a Synapse table.
#' 
#' @param table_id Synapse ID of a table
#' @return Number of rows deleted
clear_synapse_table <- function(table_id) {
  
  res <- as.data.frame(synTableQuery(glue("SELECT * FROM {table_id}")))
  tbl <- Table(schema = synGet(table_id), values = res)
  synDelete(tbl)
  
  return(nrow(res))
}

#' Update rows of a Synapse table with new data.
#' 
#' @param table_id Synapse ID of a table
#' @param data Data frame of new data
#' @return Number of rows added
update_synapse_table <- function(table_id, data) {
  
  entity <- synGet(table_id)
  project_id <- entity$properties$parentId
  table_name <- entity$properties$name
  table_object <- synBuildTable(table_name, project_id, data)
  synStore(table_object)
  
  return(nrow(data))
}

#' Clear all data from a table, replace with new data, and 
#' create a new snapshot version.
#' 
#' @param table_id Synapse ID of the table
#' @param data Data frame of new data
#' @param comment Comment string to include with the new snapshot version.
#' @return New snapshot version number
create_new_table_version <- function(table_id, data, comment = "") {
  n_rm <- clear_synapse_table(table_id)
  n_add <- update_synapse_table(table_id, data)
  n_version <- create_synapse_table_snapshot(table_id, comment)
  return(n_version)
}

# read ----------------------------

data_concept <- read.csv(file_concept)
data_relationship <- read.csv(file_relationship)

# main ----------------------------

n_version_concept <- create_new_table_version(table_id = synid_table_concept, 
                                              data = data_concept, 
                                              comment = comment)

n_version_relationship <- create_new_table_version(table_id = synid_table_relationship, 
                                              data = data_relationship, 
                                              comment = comment)

# close out ----------------------------

print(glue("Updated table '{synGet(synid_table_concept)$properties$name}' ({synid_table_concept}) to version {n_version_concept}."))
print(glue("Updated table '{synGet(synid_table_relationship)$properties$name}' ({synid_table_relationship}) to version {n_version_relationship}."))
toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
