library(mockery)
library(testthat)

source(testthat::test_path("..", "merge_and_uncode_rca_uploads.R"))

# Setup code: This will run before any tests
setup({
  workdir <- file.path(getwd(), "..")
  if (!file.exists("config.yaml")) {
    workdir <- "/usr/local/src/myscripts"
  }
  config_path <<- read_yaml(glue("{workdir}/config.yaml"))

  # Mock config
  config <<- list(
    synapse = list(
      prissmm = list(id = "mock_prissmm_id"),
      grs = list(id = "mock_grs_id")
    ),
    upload = list(
      cohort1 = list(
        site1 = list(
          data1 = "mock_data1_id",
          data2 = "mock_data2_id"
        ),
        site2 = list(
          data1 = "mock_data3_id",
          data2 = "mock_data4_id"
        )
      )
    ),
    column_name = list(
      variable_name = "variable_name",
      field_type = "field_type",
      variable_mapping = "variable_mapping"
    )
  )

})


test_that("get_output_folder_id gets expected id when environment is production", {
    folder_id <- get_output_folder_id(config_path, environment = "production")
    expect_equal(folder_id, "syn23286928")
})


test_that("get_output_folder_id gets expected id when environment is staging", {
    folder_id <- get_output_folder_id(config_path, environment = "staging")
    expect_equal(folder_id, "syn63887337")
})


test_that("get_global_response_set returns NULL when use_grs is FALSE", {
  result <- get_global_response_set(use_grs = FALSE)
  expect_null(result)
})


test_that("get_global_response_set calls read.csv and returns data when use_grs is TRUE", {
  mock_read_csv <- mock(data.frame(a = 1:3, b = 4:6))  # Example mock data frame
  stub(get_global_response_set, "read.csv", mock_read_csv)

  mock_synGet <- mock(list(path = "mock_path"))
  stub(get_global_response_set, "synGet", mock_synGet)

  result <- get_global_response_set(use_grs = TRUE)

  # Verify the function returns the mocked data frame
  expect_equal(result, data.frame(a = 1:3, b = 4:6))
})


test_that("get_prov_used returns correct prov when use_grs is FALSE", {
  # Mock the `get_bpc_synid_prissmm` function
  mock_get_bpc_synid_prissmm <- mock("mock_synid_dd")
  stub(get_prov_used, "get_bpc_synid_prissmm", mock_get_bpc_synid_prissmm)
  
  result <- get_prov_used(cohort = "cohort1", use_grs = FALSE)
  expect_equal(result, c("mock_data1_id", "mock_data2_id", "mock_data3_id", "mock_data4_id", "mock_synid_dd"))
})

test_that("get_prov_used returns correct prov when use_grs is TRUE", {
  # Mock the `get_bpc_synid_prissmm` function
  mock_get_bpc_synid_prissmm <- mock("mock_synid_dd")
  stub(get_prov_used, "get_bpc_synid_prissmm", mock_get_bpc_synid_prissmm)

  result <- get_prov_used(cohort = "cohort1", use_grs = TRUE)
  print(result)
  expect_equal(result, c("mock_data1_id", "mock_data2_id", "mock_data3_id", "mock_data4_id", "mock_synid_dd", "mock_grs_id"))
})

test_that("uncode_data decodes codes correctly when use_grs is FALSE", {
  # Mock input
  input_data <- data.frame(
    record_id = c(1, 2, 3),
    yesno_field = c(0, 1, 1),
    dropdown_radio_field = c(1, 2, 3)
  )
  
  dd <- data.frame(
    variable_name = c("record_id", "yesno_field", "dropdown_radio_field"),
    field_type = c("text", "yesno", "dropdown"),
    variable_mapping = c(NA,NA,"1,foo|2,bar|3,test")
  )
  
  # Expected output after decoding
  expected_output <- data.frame(
    record_id = c(1, 2, 3),
    yesno_field = c("No", "Yes", "Yes"),
    dropdown_radio_field = c("foo", "bar", "test"),
    stringsAsFactors = FALSE
  )
  
  # Run function
  result <- uncode_data(input_data, dd, grs=NULL, use_grs=FALSE)
  
  # Test
  expect_equal(result, expected_output)
})
