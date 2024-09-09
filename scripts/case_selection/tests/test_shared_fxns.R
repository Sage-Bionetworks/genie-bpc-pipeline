# Description: Tests for converting BPC REDCap Data Dictionary to non-PHI Data Dictionary.

library(testthat)
library(glue)
library(dplyr)
library(synapser)
library(mockery)
synLogin()
source("shared_fxns.R")


test_that("get_main_genie_clinical_id with real data", {
  expect_equal(get_main_genie_clinical_id("17.2-consortium"), 'syn62173555')
})

test_that("get_main_genie_clinical_id returns correct file id when release exists", {
  release <- "v1.0.0"
  
  mock_synTableQuery <- mock(data.frame(id = "syn12345"))
  mock_synGetChildren <- mock(list(list(name = "data_clinical.txt", id = "syn123")))
  
  stub(get_main_genie_clinical_id, "synTableQuery", mock_synTableQuery)
  stub(get_main_genie_clinical_id, "synGetChildren", mock_synGetChildren)

  # Test for expected ID
  result <- get_main_genie_clinical_id(release)
  expect_equal(result, "syn123")
})

test_that("get_main_genie_clinical_id throws an error when release version is invalid", {
  release <- "invalid_release"
  # Mocking the response to return no results
  mock_synTableQuery <- mock(data.frame())
  
  stub(get_main_genie_clinical_id, "synTableQuery", mock_synTableQuery)

  # Test that error is thrown
  expect_error(get_main_genie_clinical_id(release), "The release version is invalid.")
})

test_that("get_main_genie_clinical_id returns NULL when data_clinical.txt does not exist", {
  # Mocking external dependencies
  release <- "v1.0.0"
  
  mock_synTableQuery <- mock(data.frame(id = "syn12345"))
  mock_synGetChildren <- mock(list(list(name = "other_file.txt", id = "syn123")))
  
  stub(get_main_genie_clinical_id, "synTableQuery", mock_synTableQuery)
  stub(get_main_genie_clinical_id, "synGetChildren", mock_synGetChildren)

  # Test that NULL is returned (or whatever behavior is expected if the file isn't found)
  result <- get_main_genie_clinical_id(release)
  expect_null(result)
})
