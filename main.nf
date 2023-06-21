#!/usr/bin/env nextflow
// Ensure DSL2
nextflow.enable.dsl = 2

// IMPORT MODULES
include { run_quac_upload_report_error } from './modules/run_quac_upload_report_error.'
include { run_quac_upload_report_warning } from './modules/run_quac_upload_report_warning'
include { merge_and_uncode_rca_uploads } from './modules/merge_and_uncode_rca_uploads'
include { remove_patients_from_merged } from './modules/remove_patients_from_merged'
include { update_data_table } from './modules/update_data_table'
include { update_date_tracking_table } from './modules/update_date_tracking_table'
include { run_quac_table_report } from './modules/run_quac_table_report'
include { run_quac_comparison_report } from './modules/run_quac_comparison_report'
include { create_masking_report } from './modules/create_masking_report'
include { update_case_count_table } from './modules/update_case_count_table'

params.cohort = 'NSCLC'
/* 
Note: For multi-word strings like in the param comment here, everywhere that calls $comment as an argument
needed to be enclosed with double quotes so that nextflow interprets it as an entire string and 
not separate command line arguments 
*/
params.comment = 'NSCLC public release update'
// testing or production pipeline
params.production = false

// Check if cohort is part of allowed cohort list
def allowed_cohorts = [
   "BLADDER", 
   "BrCa", 
   "CRC", 
   "NSCLC", 
   "PANC", 
   "Prostate", 
   "CRC2", 
   "NSCLC2", 
   "MELANOMA", 
   "OVARIAN", 
   "ESOPHAGO", 
   "RENAL"
]

if (!allowed_cohorts.contains(params.cohort)) {
   exit 1, 'Invalid cohort name'
}

workflow {
   ch_cohort = Channel.value(params.cohort)
   ch_comment = Channel.value(params.comment)
   ch_production = Channel.value(params.production)

   run_quac_upload_report_error(ch_cohort)
   run_quac_upload_report_warning(run_quac_upload_report_error.out, ch_cohort, ch_production)
   merge_and_uncode_rca_uploads(run_quac_upload_report_warning.out, ch_cohort, ch_production)
   remove_patients_from_merged(merge_and_uncode_rca_uploads.out, ch_cohort, ch_production)
   update_data_table(remove_patients_from_merged.out, ch_comment, ch_production)
   update_date_tracking_table(update_data_table.out, ch_cohort, ch_comment, ch_production)
   run_quac_table_report(update_date_tracking_table.out, ch_cohort, ch_production)
   run_quac_comparison_report(run_quac_table_report.out, ch_cohort, ch_production)
   create_masking_report(run_quac_comparison_report.out, ch_cohort, ch_production)
   update_case_count_table(create_masking_report.out, ch_comment, ch_production)
}

