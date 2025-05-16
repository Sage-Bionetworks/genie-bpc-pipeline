# Description: Tests for perform_case_selection.R
library(testthat)
library(glue)
library(dplyr)
library(mockery)

source("perform_case_selection.R")

# set global variables
phase <- "test_phase"
site <- "test_site"
cohort <- "test_cohort"
debug <- "False"
site_seed <- 123

# eligible_cohort data frame
eligible_cohort <- data.frame(
  order = 1:100,
  PATIENT_ID = paste("patient", 1:100, sep = "_"),
  SAMPLE_IDS = paste("sample", 1:100, sep = "_")
)

test_that("create_selection_matrix throws an error when not enough eligible patients", {
  n_prod <- 200
  n_pressure <- 20
  n_irr <- 30
  n_eligible <- 100
  
  # Test when n_prod > n_eligible
  expect_error(create_selection_matrix(eligible_cohort, n_prod, n_pressure, n_irr), 
               glue("not enough eligible patients for production target ({n_eligible} < {n_prod}) for phase {phase} {site} {cohort}.  Please revise eligibility criteria."), fixed = TRUE)
})

test_that("create_selection_matrix correctly categorizes cases", {
  n_prod <- 50
  n_pressure <- 10
  n_irr <- 10

  # call the function
  categorized_cohort <- create_selection_matrix(eligible_cohort, n_prod, n_pressure, n_irr)

  # validate calls
  expect_equal(nrow(categorized_cohort), nrow(eligible_cohort))
  expect_equal(sum(categorized_cohort$pressure == "pressure"), n_pressure)
  expect_equal(sum(categorized_cohort$irr == "irr"), n_irr)
  expect_equal(sum(categorized_cohort$category == "production"), n_prod)
  expect_equal(sum(categorized_cohort$category == "extra"), 50)
})