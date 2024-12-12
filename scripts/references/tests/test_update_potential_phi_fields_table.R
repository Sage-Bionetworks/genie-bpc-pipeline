library(mockery)
library(testthat)

source(testthat::test_path("..", "update_potential_phi_fields_table.R"))

# Setup code: This will run before any tests
setup({
  # Mock synapse function
  mock_synStore <<- mock()
  mock_snapshot_version <<- mock(return_value=3)

  # Create dummy input data
  test_df_update_non_empty <<-  data.frame(col1 = 1:3, col2 = letters[1:3])
  test_df_update_empty <<-  data.frame()
  synid_table <<-  "syn123"
  synid_file_sor <<-  "syn456"
  test_comment <<- "Test comment"
})


test_that("update_red_table does not update table when dry_run is TRUE and df_update has non-empty rows", {
  dry_run <- TRUE
  
  # Use mockery to mock the synStore and snapshot_synapse_table functions
  stub(update_red_table, "synStore", mock_synStore)
  stub(update_red_table, "snapshot_synapse_table", mock_snapshot_version)
  
  # Call the function with dry_run = TRUE
  result <- update_red_table(synid_table, synid_file_sor, test_df_update_non_empty, test_comment, dry_run)
  
  # Assert that synStore was never called
  expect_called(mock_synStore, 0)
  
  # Assert that snapshot_synapse_table was never called
  expect_called(mock_snapshot_version, 0)
  
  # Assert that result is 3 (since update was made)
  expect_equal(result, NA)
})


test_that("update_red_table does not update table when dry_run is TRUE and df_update has empty rows", {
  dry_run <- TRUE
  
  # Use mockery to mock the synStore and snapshot_synapse_table functions
  stub(update_red_table, "synStore", mock_synStore)
  stub(update_red_table, "snapshot_synapse_table", mock_snapshot_version)
  
  # Call the function with dry_run = TRUE
  result <- update_red_table(synid_table, synid_file_sor, test_df_update_empty, test_comment, dry_run)
  
  # Assert that synStore was never called
  expect_called(mock_synStore, 0)
  
  # Assert that snapshot_synapse_table was never called
  expect_called(mock_snapshot_version, 0)
  
  # Assert that result is NA (since no update was made)
  expect_equal(result, NA)
})

test_that("update_red_table updates table when dry_run is FALSE and df_update has non-empty rows", {
  dry_run <- FALSE
  
  # Use mockery to mock the synStore and snapshot_synapse_table functions
  stub(update_red_table, "synStore", mock_synStore)
  stub(update_red_table, "snapshot_synapse_table", mock_snapshot_version)
  
  # Call the function with dry_run = FALSE
  result <- update_red_table(synid_table, synid_file_sor, test_df_update_non_empty, test_comment, dry_run)
  
  # Assert that synStore was called once
  expect_called(mock_synStore, 1)
  
  # Assert that snapshot_synapse_table was called once
  expect_called(mock_snapshot_version, 1)
  
  # Assert that result is 1 (simulated version returned by mock_snapshot_version)
  expect_equal(result, 3)
})


test_that("update_red_table does not update table when dry_run is FALSE and df_update has empty rows", {
  dry_run <- FALSE
  
  # Use mockery to mock the synStore and snapshot_synapse_table functions
  stub(update_red_table, "synStore", mock_synStore)
  stub(update_red_table, "snapshot_synapse_table", mock_snapshot_version)
  
  # Call the function with dry_run = FALSE
  result <- update_red_table(synid_table, synid_file_sor, test_df_update_empty, test_comment, dry_run)
  
  # Assert that synStore was called once
  expect_called(mock_synStore, 1)
  
  # Assert that snapshot_synapse_table was called once
  expect_called(mock_snapshot_version, 1)
  
  expect_equal(result, NA)
})
