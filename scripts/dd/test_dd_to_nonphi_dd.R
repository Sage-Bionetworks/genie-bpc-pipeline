# Description: Tests for converting BPC REDCap Data Dictionary to non-PHI Data Dictionary.
# Author: Haley Hunter-Zinck
# Date: 2021-10-13

library(testthat)
library(glue)
library(dplyr)
library(synapser)
synLogin()
source("fxns_dd_to_nonphi_dd.R")

# fixtures --------------------------

synid_file_dd <- "syn26344209"
dd <- get_synapse_entity_data_in_csv(synid_file_dd, 
                                     na.strings = c(""), 
                                     check_names = F)
nonphi <- remove_phi_from_dd(dd)

# tests ------------------------------

test_that("is_synapse_entity_id", {
  expect_true(is_synapse_entity_id("syn25944649"))
  expect_true(is_synapse_entity_id("syn25944660"))
  expect_true(is_synapse_entity_id("syn26128875"))
  expect_false(is_synapse_entity_id("hello world!"))
  expect_false(is_synapse_entity_id("25944649"))
})

test_that("remove_phi_from_dd_basic", {
  expect_equal(length(which(nonphi$`Identifier?` == "y")), 0)
  expect_equal(length(which(nonphi$`Field Type?` == "descriptive")), 0)
  expect_equal(length(which(!is.na(nonphi$`Field Annotation`))), 0)
  expect_equal(length(which(!is.na(nonphi$`Branching Logic (Show field only if...)`))), 0)
  expect_false(all(is.element(nonphi$`Variable / Field Name`, c("cpt_qanotes", "rt_qanotes"))))
  expect_true(nrow(nonphi) <= nrow(dd))
  expect_true(ncol(nonphi) <= ncol(dd))
})

test_that("remove_phi_from_dd_calc", {
  expect_equal(length(which(nonphi$`Field Type?` == "calc")), 0)
  expect_true(length(which(nonphi$`Text Validation Type OR Show Slider Number` == "integer")) > 0)
  expect_true(nonphi %>% 
                filter(`Field Type` == "text") %>% 
                select(`Text Validation Type OR Show Slider Number`) %>% 
                distinct() == "integer")
  expect_true(is.na(nonphi %>% 
                filter(`Field Type` == "text") %>% 
                select(`Choices, Calculations, OR Slider Labels`) %>% 
                distinct()))
})

test_that("remove_phi_from_dd_cur_curator", {
  var_name <- "cur_curator"
  expect_equal(as.character(unlist(nonphi %>% 
                                     filter(`Variable / Field Name` == var_name) %>%
                                     select(`Field Type`))), "text")
  expect_true(is.na(unlist(nonphi %>% 
                             filter(`Variable / Field Name` == var_name) %>%
                             select(`Choices, Calculations, OR Slider Labels`))))
  expect_equal(as.character(unlist(nonphi %>% 
                                     filter(`Variable / Field Name` == var_name) %>%
                                     select(`Text Validation Type OR Show Slider Number`))), "integer")
})

test_that("remove_phi_from_dd_qa_full_reviewer", {
  
  var_name <- "qa_full_reviewer"
  expect_equal(as.character(unlist(nonphi %>% 
                                     filter(`Variable / Field Name` == var_name) %>%
                                     select(`Field Type`))), "text")
  expect_true(is.na(unlist(nonphi %>% 
                             filter(`Variable / Field Name` == var_name) %>%
                             select(`Choices, Calculations, OR Slider Labels`))))
  expect_equal(as.character(unlist(nonphi %>% 
                                     filter(`Variable / Field Name` == var_name) %>%
                                     select(`Text Validation Type OR Show Slider Number`))), "integer")
})

test_that("remove_phi_from_dd_qa_full_reviewer_dual", {
  
  var_name <- "qa_full_reviewer_dual"
  expect_equal(as.character(unlist(nonphi %>% 
                                     filter(`Variable / Field Name` == var_name) %>%
                                     select(`Field Type`))), "text")
  expect_true(is.na(unlist(nonphi %>% 
                             filter(`Variable / Field Name` == var_name) %>%
                             select(`Choices, Calculations, OR Slider Labels`))))
  expect_equal(as.character(unlist(nonphi %>% 
                                     filter(`Variable / Field Name` == var_name) %>%
                                     select(`Text Validation Type OR Show Slider Number`))), "integer")
  expect_true(grepl(x = as.character(unlist(nonphi %>%
                      filter(`Variable / Field Name` == "drugs_drug_1") %>%
                    select(`Choices, Calculations, OR Slider Labels`))), 
                    pattern = "^49135, Investigational Drug | "))
})

test_that("remove_phi_from_dd_drugs", {
  expect_true(grepl(x = as.character(unlist(nonphi %>%
                                              filter(`Variable / Field Name` == "drugs_drug_2") %>%
                                              select(`Choices, Calculations, OR Slider Labels`))), 
                    pattern = "^49135, Investigational Drug | "))
  expect_true(grepl(x = as.character(unlist(nonphi %>%
                                              filter(`Variable / Field Name` == "drugs_drug_3") %>%
                                              select(`Choices, Calculations, OR Slider Labels`))), 
                    pattern = "^49135, Investigational Drug | "))
  expect_true(grepl(x = as.character(unlist(nonphi %>%
                                              filter(`Variable / Field Name` == "drugs_drug_4") %>%
                                              select(`Choices, Calculations, OR Slider Labels`))), 
                    pattern = "^49135, Investigational Drug | "))
  expect_true(grepl(x = as.character(unlist(nonphi %>%
                                              filter(`Variable / Field Name` == "drugs_drug_5") %>%
                                              select(`Choices, Calculations, OR Slider Labels`))), 
                    pattern = "^49135, Investigational Drug | "))
  expect_false(grepl(x = as.character(unlist(nonphi %>%
                                              filter(`Variable / Field Name` == "drugs_drug_5") %>%
                                              select(`Choices, Calculations, OR Slider Labels`))), 
                    pattern = "49135, Investigational Drug \\| $"))
})

