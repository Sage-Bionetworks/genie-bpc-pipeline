waitifnot <- function(cond, msg = "") {
  if (!cond) {
    
    for (str in msg) {
      message(str)
    }
    message("Press control-C to exit and try again.")
    
    while(T) {}
  }
}

get_default_global <- function(config, key) {
  return(config$default$global[[key]])
}

get_default_site <- function(config, site, key) {
  return(config$default$site[[site]][[key]])
}

get_custom <- function(config, phase, cohort, site, key) {
  return(config$phase[[phase]]$cohort[[cohort]]$site[[site]][[key]])
}

get_production <- function(config, phase, cohort, site) {
  get_custom(config = config, phase = phase, cohort = cohort, site = site, key = "production")
}

get_adjusted <- function(config, phase, cohort, site) {
  get_custom(config = config, phase = phase, cohort = cohort, site = site, key = "adjusted")
}

get_pressure <- function(config, phase, cohort, site) {
  
  n_pressure_defualt <- config$default$site[[site]]$pressure
  n_pressure_specific <- config$phase[[phase]]$cohort[[cohort]]$site[[site]]$pressure
  n_pressure <- if (!is.null(n_pressure_specific)) n_pressure_specific else n_pressure_defualt
  
  return(n_pressure)
}

get_sdv_or_irr_value <- function(config, phase, cohort, site, key = c("sdv", "irr")) {
  
  n_pressure <- get_pressure(config, phase, cohort, site)
  n_prod <- get_production(config, phase, cohort, site)
  
  # site default
  val_site <- get_default_site(config, site, key)
  if (!is.null(val_site) && is.na(val_site)) {
    if (val_site < 1) {
      return(round(val_site * (n_prod)))
    }
    return(val_site)
  }
  
  # custom
  val_custom <- get_custom(config, phase, cohort, site, key)
  if (!is.null(val_custom) && !is.na(val_custom)) {
    if (val_custom < 1) {
      return(round(val_custom * (n_prod)))
    }
    return(val_custom)
  }
  
  # site default
  val_site <- get_default_site(config, site, key)
  if (!is.null(val_site) && !is.na(val_site)) {
    if (val_site < 1) {
      return(round(val_site * (n_prod)))
    }
    return(val_site)
  }
  
  # global default
  val_global <- get_default_global(config, key)
  if (!is.null(val_global) && !is.na(val_global)) {
    if (val_global < 1) {
      return(round(val_global * (n_prod)))
    }
    return(val_global)
  }
  return(NA)
}

get_sdv <- function(config, phase, cohort, site) {
  return(get_sdv_or_irr_value(config, phase, cohort, site, "sdv"))
}

get_irr <- function(config, phase, cohort, site) {
  return(get_sdv_or_irr_value(config, phase, cohort, site, "irr"))
}

now <- function(timeOnly = F, tz = "US/Pacific") {
  
  Sys.setenv(TZ=tz)
  
  if(timeOnly) {
    return(format(Sys.time(), "%H:%M:%S"))
  }
  
  return(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
}

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

#' Get a synapse ID by following a traditional file path from a root synapse folder entity.
#' 
#' @param synid_folder_root Synapse ID of the root folder
#' @param path Folder path starting in the first subfolder ending in desired folder and delimited with '/'
#' @return Synapse ID of the final subfolder in the path
#' @example get_folder_synid_from_path("syn12345", "first/second/final")
get_folder_synid_from_path <- function(synid_folder_root, path) {
  
  synid_folder_current <- synid_folder_root
  subfolders <- strsplit(path, split = "/")[[1]]
  
  for (i in 1:length(subfolders)) {
    synid_folder_children <- get_synapse_folder_children(synid_folder_current, 
                                                         include_types = list("folder"))
    
    if (!is.element(subfolders[i], names(synid_folder_children))) {
      return(NA)
    }
    
    synid_folder_current <- as.character(synid_folder_children[subfolders[i]])
  }
  
  return(synid_folder_current)
}

#' Get the Synapse ID of a named file entity given a file path and root Synapse ID. 
#' 
#'  @param synid_folder_root  Synapse ID of the root folder
#'  @param path Folder path starting in the first subfolder ending in desired 
#'  file and delimited with '/'
#'  @return Synapse ID of file entity
#'  @example get_file_synid_from_path("syn12345", "first/second/my_file.csv")
get_file_synid_from_path <- function(synid_folder_root, path) {
  
  path_part <- strsplit(path, split = "/")[[1]] 
  file_name <- tail(path_part, n = 1)
  path_abbrev <- paste0(path_part[1:(length(path_part) - 1)], collapse = "/")
  
  synid_folder_dest <- get_folder_synid_from_path(synid_folder_root, 
                                                  path_abbrev)
  
  synid_folder_children <- get_synapse_folder_children(synid_folder_dest, 
                                                       include_types = list("file"))
  
  if (!is.element(file_name, names(synid_folder_children))) {
    return(NA)
  }
  
  return(as.character(synid_folder_children[file_name]))
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
