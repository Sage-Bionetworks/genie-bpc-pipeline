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

test_that("remap_patient_characteristics works as expected", {
  
  # Mock input data
  clinical <- data.frame(
    patient_id = c(1, 2, 3),
    birth_year = c(1980, 1990, 2000),
    ethnicity_detailed = c("Hispanic", "Non-Hispanic", "Hispanic"),
    primary_race_detailed = c("White", "Black", "Asian"),
    secondary_race_detailed = c("Unknown", "White", "Black"),
    tertiary_race_detailed = c("Asian", "Unknown", "White"),
    sex_detailed = c("Male", "Female", "Male")
  )
  
  existing_patients <-  c(1, 2, 3)
  
  ethnicity_mapping <- data.frame(
    DESCRIPTION = c("Hispanic", "Non-Hispanic"),
    CODE = c("1", "2")
  )
  
  race_mapping <- data.frame(
    DESCRIPTION = c("White", "Black", "Asian", "Unknown"),
    CODE = c("1", "2", "3", "99")
  )
  
  sex_mapping <- data.frame(
    DESCRIPTION = c("Male", "Female"),
    CODE = c("M", "F")
  )
  
  # Expected output
  expected_output <- data.frame(
    record_id = c(1, 2, 3),
    redcap_repeat_instrument = c("", "", ""),
    redcap_repeat_instance = c("", "", ""),
    genie_patient_id = c(1, 2, 3),
    birth_year = c(1980, 1990, 2000),
    naaccr_ethnicity_code = c("1", "2", "1"),
    naaccr_race_code_primary = c("1", "2", "3"),
    naaccr_race_code_secondary = c("99", "1", "2"),
    naaccr_race_code_tertiary = c("3", "99", "1"),
    naaccr_sex_code = c("M", "F", "M")
  )
  
  # Run the function
  result <- remap_patient_characteristics(clinical, existing_patients, ethnicity_mapping, race_mapping, sex_mapping)
  
  # Test if the output is as expected
  expect_equal(result, expected_output)
})

test_that("check_for_missing_values - no missing or empty values in centers other than CHOP, PROV, JHU", {
  data <- data.frame(
    col1 = c(1, 2, 3, NA),
    col2 = c("a", "b", "c", ""),
    genie_patient_id = c('a', 'b', 'c', 'CHOP123')
  )
  expect_warning(check_for_missing_values(data, c("col1", "col2")), NA)

})

test_that("check_for_missing_values - NAs are detected in centers other than CHOP, PROV, JHU", {
  data <- data.frame(
    col1 = c(1, NA, 3),
    col2 = c("a", "b", "c"),
    genie_patient_id = c('CHOP123', 'b', 'PROV234')
  )
  expect_warning(check_for_missing_values(data, c("col1", "col2")), 
              "Warning: Missing or empty values found in column\\(s\\): col1")
})

test_that("check_for_missing_values - empty string values are detected in centers other than CHOP, PROV, JHU", {
  data <- data.frame(
    col1 = c(1, 2, 3),
    col2 = c("a", "", "c"),
    genie_patient_id = c('CHOP123', 'b', 'PROV234')
  )
  expect_warning(check_for_missing_values(data, c("col1", "col2")), 
               "Warning: Missing or empty values found in column\\(s\\): col2")
})

test_that("check_for_missing_values - multiple missing and empty values are detected in centers other than CHOP, PROV, JHU", {
  data <- data.frame(
    col1 = c(1, NA, ""),
    col2 = c("a", "", "c"),
    genie_patient_id = c('CHOP123', 'b', 'PROV234')
  )
  expect_warning(check_for_missing_values(data, c("col1", "col2")), 
               "Warning: Missing or empty values found in column\\(s\\): col2, col1")
})

test_that("check_for_missing_values - multiple missing and empty values are detected in CHOP, PROV, JHU centers", {
  data <- data.frame(
    col1 = c(1, NA, 2),
    col2 = c("a", "", "c"),
    genie_patient_id = c('a', 'CHOP123', 'PROV234')
  )
  expect_warning(check_for_missing_values(data, c("col1", "col2")), NA)
})