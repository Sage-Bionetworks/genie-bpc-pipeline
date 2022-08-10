# Description: Mark additional cases as production 
#   and sample additional number of SDV and IRR cases.  
# Author: Xindi Guo
# Date: 2022-08-05

# pre-setup --------------------------------

library(optparse)
library(yaml)
library(glue)
source("shared_fxns.R")

# parameters
config <- read_yaml("config.yaml")

# user input ----------------------------

option_list <- list( 
  make_option(c("-p", "--phase"), type = "integer",
              help="BPC phase"),
  make_option(c("-c", "--cohort"), type = "character",
              help="BPC cohort"),
  make_option(c("-s", "--site"), type = "character",
              help="BPC site"),
  make_option(c("-u", "--save_synapse"), action="store_true", default = FALSE, 
              help="Save output to Synapse")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(!is.null(opt$phase) && !is.null(opt$cohort) && !is.null(opt$site),
          msg = "Usage: Rscript mark_additional_cases.R -h")

phase <- opt$phase
cohort <- opt$cohort
site <- opt$site
save_synapse <- opt$save_synapse

# check user input -----------------

phase_str <- paste0(names(config$phase), collapse = ", ")
waitifnot(is.element(phase, names(config$phase)),
          msg = c(glue("Error: phase {phase} is not valid.  Valid values: {phase_str}"),
                  "Usage: Rscript mark_additional_cases.R -h"))

cohort_str <- paste0(names(config$phase[[phase]]$cohort), collapse = ", ")
waitifnot(is.element(phase, names(config$phase)),
          msg = c(glue("Error: cohort {cohort} is not valid for phase {phase}.  Valid values: {cohort_str}"),
                  "Usage: Rscript mark_additional_cases.R -h"))

site_str <- paste0(names(config$phase[[phase]]$cohort[[cohort]]$site), collapse = ", ")
waitifnot(is.element(phase, names(config$phase)),
          msg = c(glue("Error: site {site} is not valid for phase {phase} and cohort {cohort}.  Valid values: {site_str}"),
                  "Usage: Rscript mark_additional_cases.R -h"))

# setup ----------------------------

tic = as.double(Sys.time())

library(dplyr)
library(synapser)
synLogin()

# get random seed
default_site_seed <- config$default$site[[site]]$seed
cohort_site_seed <- config$phase[[phase]]$cohort[[cohort]]$site[[site]]$seed

# file names
file_selection <- tolower(glue("{cohort}_{site}_phase{phase}_case_selection.csv"))

# synapse
synid_folder_output <- get_folder_synid_from_path(synid_folder_root = config$synapse$ids$id, 
                                                  path = glue("{cohort}/{site}"))
synid_file_output <- get_file_synid_from_path(synid_folder_root = config$synapse$ids$id,
                                             path = glue("{cohort}/{site}/{file_selection}"))

# provenance exec
prov_exec <- "https://github.com/Sage-Bionetworks/Genie_processing/blob/master/bpc/case_selection/mark_additional_cases.R"

# provenance used
prov_used <- c(synid_file_output)

# functions ----------------------------

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

# case selection ---------------------

# set default
sdv <- 0.2
irr <- 0.05
n_pressure <- 15
#n_production <- get_production(config, phase, cohort, site) 
n_production <- 775 #775 for DFCI and 980 for MSK

cur_case_selection <- get_synapse_entity_data_in_csv(synid_file_output)

# get current sdv orders and calculate number of additional sdv needed
cur_sdv <- cur_case_selection %>%
  filter(sdv=="sdv") %>%
  select(order) %>%
  pull(order)
n_sdv <- round(n_production * sdv)
n_add_sdv <- n_sdv - length(cur_sdv) + n_pressure

# get current irr orders and calculate number of additional irr needed
cur_irr <- cur_case_selection %>%
  filter(irr=="irr") %>%
  select(order) %>%
  pull(order)
n_irr <- round(n_production * irr)
n_add_irr <- n_irr - length(cur_irr)

# randomly disperse additional SDV cases among non-current sdv/irr
set.seed(seed = if (!is.null(cohort_site_seed)) cohort_site_seed else default_site_seed)
order_add_sdv <- sample(setdiff(1:n_production, c(cur_sdv,cur_irr)),n_add_sdv)

# randomly disperse addition IRR cases among non-sdv and non-current irr
set.seed(seed = if (!is.null(cohort_site_seed)) cohort_site_seed else default_site_seed)
order_add_irr <- sample(setdiff(1:n_production, c(cur_sdv,order_add_sdv,cur_irr)), 
                        n_add_irr)

# update the category, sdv, and irr columns
case_selection <- cur_case_selection %>% 
  mutate(category = ifelse(order > n_production, "extra", "production")) %>%
  mutate(sdv = ifelse(order %in% c(cur_sdv, order_add_sdv), "sdv","")) %>%
  mutate(irr = ifelse(order %in% c(cur_irr, order_add_irr), "irr",""))

# write locally -----------------------
print(glue("{now(timeOnly = T)}: writing case selection to file..."))
write.csv(case_selection, file = file_selection, row.names = F)

# save to synapse ----------------------
if(save_synapse){
  save_to_synapse(path = file_selection, 
                  parent_id = synid_folder_output,  
                  prov_name = "Additional SDV and IRR", 
                  prov_desc = "Sample additional SDV and IRR cases", 
                  prov_used = prov_used, 
                  prov_exec = prov_exec)
  # local clean-up
  file.remove(file_selection)
}

# close out ----------------------------
print(glue("Summary:"))
print(glue("  Phase: {phase}"))
print(glue("  Cohort: {cohort}"))
print(glue("  Site: {site}"))
print(glue("  Preivous number of target cases: {nrow(cur_case_selection %>% filter(category=='production'))}"))
print(glue("  Updated Number of target cases: {n_production}"))
print(glue("  Number of added SDV cases: {n_add_sdv}"))
print(glue("  Number of total SDV cases (excluding pressure): {n_sdv}"))
print(glue("  Number of added IRR cases: {n_add_irr}"))
print(glue("  Number of total IRR cases: {n_irr}"))
print(glue("Outfiles: {file_selection}"))

if (save_synapse) {
  print(glue("Output saved to Synapse ({synid_folder_output})"))
}

toc = as.double(Sys.time())
print(glue("Runtime: {round(toc - tic)} s"))
