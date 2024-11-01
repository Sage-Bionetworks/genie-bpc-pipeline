library(testthat)

source(testthat::test_path("..", "merge_and_uncode_rca_uploads.R"))

# Setup code: This will run before any tests
setup({
  workdir <- file.path(getwd(), "..")
  if (!file.exists("config.yaml")) {
    workdir <- "/usr/local/src/myscripts"
  }
  config <<- read_yaml(glue("{workdir}/config.yaml"))

})


test_that("get_output_folder_id gets expected id when environment is production", {
    folder_id <- get_output_folder_id(config, environment = "production")
    expect_equal(folder_id, "syn23286928")
})


test_that("get_output_folder_id gets expected id when environment is staging", {
    folder_id <- get_output_folder_id(config, environment = "staging")
    expect_equal(folder_id, "syn63887337")
})