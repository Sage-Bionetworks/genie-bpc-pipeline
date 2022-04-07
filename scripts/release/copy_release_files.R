# Description: Copy BPC release folders plus contents to new folder on Synapse 
#   without annotations, provenance, or previous versions. 
# Author: Haley Hunter-Zinck
# Date: 2022-04-04

# setup  ---------------------------

tic = as.double(Sys.time())

library(optparse)
library(glue)
library(dplyr)
library(synapser)

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
  make_option(c("-i", "--synid_folder_input"), 
              type = "character",
              help="Synapse ID of folder and contents to copy"),
  make_option(c("-o", "--synid_folder_output"), 
              type = "character",
              help="Synapse ID of output folder in which to create the folder copy"),
  make_option(c("-v", "--verbose"), 
              action="store_true", 
              default = FALSE, 
              help="Output script messages to the user."),
  make_option(c("-a", "--auth"), 
              type = "character",
              default = NA,
              help="Synapse personal access token or path to .synapseConfig (default: normal synapse login behavior)")
)
opt <- parse_args(OptionParser(option_list=option_list))
waitifnot(!is.null(opt$synid_folder_input) && !is.null(opt$synid_folder_output),
          msg = "Rscript 2022-03-30_haley_copy_folder_plus_contents.R -h")

synid_folder_input <- opt$synid_folder_input
synid_folder_output <- opt$synid_folder_output
verbose <- opt$verbose
auth <- opt$auth

if (verbose) {
  print(glue("Parameters: "))
  print(glue("- Synapse ID of folder to copy:\t\t{synid_folder_input}"))
  print(glue("- Synapse ID of destination folder:\t{synid_folder_output}"))
  print(glue("- verbose:\t\t\t\t{verbose}"))
}

# functions ----------------------------

#' Return current time as a string.
#' 
#' @param timeOnly If TRUE, return only time; otherwise return date and time
#' @param tz Time Zone
#' @return Time stamp as string
#' @example 
#' now(timeOnly = T)
now <- function(timeOnly = F, tz = "US/Pacific") {
  
  Sys.setenv(TZ=tz)
  
  if(timeOnly) {
    return(format(Sys.time(), "%H:%M:%S"))
  }
  
  return(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
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
synLogin <- function(auth = NA, silent = T) {
  
  secret <- Sys.getenv("SCHEDULED_JOB_SECRETS")
  if (secret != "") {
    # Synapse token stored as secret in json string
    syn = synapser::synLogin(silent = T, authToken = fromJSON(secret)$SYNAPSE_AUTH_TOKEN)
  } else if (auth == "~/.synapseConfig" || is.na(auth)) {
    # default Synapse behavior
    syn <- synapser::synLogin(silent = silent)
  } else {
    
    # in case pat passed directly
    token <- auth
    
    # extract token from custom path to .synapseConfig
    if (grepl(x = auth, pattern = "\\.synapseConfig$")) {
      token = get_auth_token(auth)
      
      if (is.na(token)) {
        return(F)
      }
    }
    
    # login with token
    syn <- tryCatch({
      synapser::synLogin(authToken = token, silent = silent)
    }, error = function(cond) {
      return(F)
    })
  }
  
  # NULL returned indicates successful login
  if (is.null(syn)) {
    return(T)
  }
  return(F)
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
#' @return Synapse ID of entity representing file
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
  
  return(file$properties$id)
}

#' Create a new Synape Folder entity. 
#' 
#' @param name Name of the Synapse Folder entity
#' @param parentId Synapse ID of Project or Folder in which to create the new Folder
#' @return Synapse ID of the new Synapse Folder entity
create_synapse_folder <- function(name, parent_id) {
  
  # check if folder already exists
  children <- get_synapse_folder_children(parent_id, include_types = list("folder"))
  if(is.element(name, names(children))) {
    return(as.character(children[name]))
  }
  
  concreteType <- "org.sagebionetworks.repo.model.Folder"
  uri <- "/entity"
  payload <- paste0("{", glue("'name':'{name}', 'parentId':'{parent_id}', 'concreteType':'{concreteType}'"), "}")
  ent <- synRestPOST(uri = uri, body = payload)
  return(ent$id)
}

#' Get the name of a Synapse entity. 
#' 
#' @param synapse_id Synapse ID string
#' @return String representing entity name
#' @example get_synapse_entity_name("syn12345")
get_synapse_entity_name <- function(synapse_id) {
  return(synGet(synapse_id, downloadFile = F)$properties$name)
}

#' Get all child entities of a synapse folder.
#' 
#' @param synapse_id Synapse ID of the folder
#' @param include_types Types of child entities to return
#' @return Vector with values as Synapse IDs and names as entity names.
get_synapse_folder_children <- function(synapse_id, 
                                        include_types=list("folder", "file", "table", "link", "entityview", "dockerrepo")) {
  
  ent <- as.list(synGetChildren(synapse_id, includeTypes = include_types))
  
  children <- c()
  
  if (length(ent) > 0) {
    for (i in 1:length(ent)) {
      children[ent[[i]]$name] <- ent[[i]]$id
    }
  }
  
  return(children)
}

#' Copy Synapse Folder entity and it's child File Entities to a destination folder.  
#' 
#' @param synid_folder_input Synapse ID of Folder with contents to copy 
#' @param synid_folder_output Synapse ID of destination folder to contain the Folder copy
#' @param verbose Indicator for printing progress to the user
#' @return String representing Synapse ID of of new copied Folder entity
copy_folder_and_contents <- function(synid_folder_input, synid_folder_output, verbose = F) {
  
  # get name and children of Folder to copy
  name_folder <- get_synapse_entity_name(synid_folder_input)
  synid_folder_children <- get_synapse_folder_children(synid_folder_input, 
                                                       include_types=list("file"))
  
  # create new Folder in destination 
  if (verbose) {
    print(glue("{now(timeOnly = T)}: creating new folder '{name_folder}' in '{get_synapse_entity_name(synid_folder_output)}' ({synid_folder_output})..."))
  }
  synid_folder_new <- create_synapse_folder(name = name_folder, parent_id = synid_folder_output)
  
  # copy files from input folder to newly created folder
  if (verbose) {
    print(glue("{now(timeOnly = T)}: copying files to new folder..."))
  }
  for (i in 1:length(synid_folder_children)) {
    entity <- synGet(as.character(synid_folder_children[i]))
    synid_file_new <- save_to_synapse(path = entity$path, 
                                      file_name = entity$properties$name,
                                      parent_id = synid_folder_new)
    
    if (verbose) {
      print(glue("\t{now(timeOnly = T)}:copied '{entity$properties$name}' ({entity$properties$id}.{entity$properties$versionNumber}) to new entity {synid_file_new}..."))
    }
  }
  
  return(synid_folder_new)
}

# synpase login --------------------

status <- synLogin(auth=auth)

# read ---------------------

synid_folder_children <- get_synapse_folder_children(synid_folder_input, 
                                                     include_types=list("folder"))
synid_folder_clinical <- as.character(synid_folder_children[grep(pattern = "clinical_data$", x = names(synid_folder_children))])
synid_folder_cbio <- as.character(synid_folder_children[grep(pattern = "cBioPortal_files$", x = names(synid_folder_children))])


synid_folder_child <- get_synapse_folder_children(synid_folder_cbio, 
                                                     include_types=list("folder"))
synid_folder_case <- as.character(synid_folder_child[grep(pattern = "case_lists$", x = names(synid_folder_child))])

# main --------------------------------

# copy clinical folder and files
synid_folder_clinical_copy <- copy_folder_and_contents(synid_folder_input = synid_folder_clinical, 
                        synid_folder_output = synid_folder_output, 
                         verbose = verbose) 

# copy cBioPortal files and file contents
synid_folder_cbio_copy <- copy_folder_and_contents(synid_folder_input = synid_folder_cbio, 
                                             synid_folder_output = synid_folder_output, 
                                             verbose = verbose) 

# copy cBioPortal case files folder and contentss
synid_folder_case_copy <- copy_folder_and_contents(synid_folder_input = synid_folder_case, 
                                                   synid_folder_output = synid_folder_cbio_copy, 
                                                   verbose = verbose) 

# close out ----------------------------

toc = as.double(Sys.time())
if (verbose) {
  print(glue("Runtime: {round(toc - tic)} s"))
}
