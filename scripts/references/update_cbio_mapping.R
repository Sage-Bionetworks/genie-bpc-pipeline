# Update the cbio mapping Synapse Table using the csv file provided by cBioPortal
# Usage: Rscript update_cbio_mapping.R

# set up
library(glue)
library(dplyr)
library(synapser)
synLogin()

file_id <- "syn25585554"
tbl_id <- "syn25712693"
cohorts <- c("NSCLC","CRC","BrCa","PANC","Prostate","BLADDER")

# download the file and get the column names
entity <- synGet(file_id)
mapping_file <- read.csv(entity$path)
columns <- as.list(synGetColumns(tbl_id))
col_names <- sapply(columns, function(x){x$name})

# comment for table snapshot
utc_mod <- as.POSIXct(entity$properties$modifiedOn)
pt_mod <- as.POSIXct(utc_mod, tz="America/Los_Angeles", usetz=TRUE)
comment <- glue("mapping file update from {format(pt_mod, format='%Y-%m-%d')} PT")

# make adjustment to match the table schema
mapping_file <- mapping_file %>%
  dplyr::rename(BrCa=BRCA, Prostate=PROSTATE, inclusion_criteria=inclusion.criteria) %>%
  mutate_at(all_of(cohorts),list(~recode(.,"Y"=TRUE,"N"=FALSE, "TBD"=FALSE))) %>%
  mutate(data_type=tolower(data_type)) %>%
  mutate(data_type=recode(data_type,"tumor_registry"="curated"))

# wipe the Synapse Table
query_for_deletion <- synTableQuery(sprintf("select * from %s", tbl_id))
deleted <- synDelete(query_for_deletion)

# update with mapping table
synStore(Table(tbl_id, mapping_file))
res <- synRestPOST(glue("/entity/{tbl_id}/table/snapshot"), 
                   body = glue("{'snapshotComment':'{{comment}}'}", 
                               .open = "{{", 
                               .close = "}}"))

# close out
print(glue("Updated table {synGet(tbl_id)$properties$name} ({tbl_id}) to version {res$snapshotVersionNumber} with comment '{comment}'"))
