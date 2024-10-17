#!/usr/bin/env nextflow
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    genie-bpc-pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Github : https://github.com/Sage-Bionetworks/genie-bpc-pipeline
*/

nextflow.enable.dsl = 2

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    SET DEFAULT PARAMETERS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

params.cohort = 'NSCLC'
/* 
Note: For multi-word strings like in the param comment here, everywhere that calls $comment as an argument
needed to be enclosed with double quotes so that nextflow interprets it as an entire string and 
not separate command line arguments 
*/
params.comment = 'NSCLC public release update'
params.production = false
params.schema_ignore_params = ""
params.help = false
params.step = "update_potential_phi_fields_table"

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    VALIDATE & PRINT PARAMETER SUMMARY
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

// Print help to screen if parameter set
if (params.help){
   command = "nextflow run main.nf"
   log.info NfcoreSchema.paramsHelp(workflow, params, command)
   System.exit(0)
}
// Validate input parameters
NfcoreSchema.validateParameters(workflow, params, log)

// Check mandatory parameters
if (params.cohort == null) { exit 1, 'cohort parameter not specified!' }
if (params.comment == null) { exit 1, 'comment parameter not specified!' }
if (params.production == null) { exit 1, 'production parameter not specified!' }
if (params.step == null) { exit 1, 'step parameter not specified!' }


// Print parameter summary log to screen
log.info "Running step: ${params.step}"

// Print message for production mode vs test mode
if (params.production) { 
   log.warn "----------------------------------------------------"
   log.warn "RUNNING IN PRODUCTION MODE"
   log.warn "----------------------------------------------------"
} 
else { 
   log.warn "----------------------------------------------------"
   log.warn "RUNNING IN TEST MODE"
   log.warn "----------------------------------------------------"
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT LOCAL MODULES/SUBWORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { update_potential_phi_fields } from './modules/update_potential_phi_fields'
include { run_quac_upload_report_error } from './modules/run_quac_upload_report_error'
include { run_quac_upload_report_warning } from './modules/run_quac_upload_report_warning'
include { merge_and_uncode_rca_uploads } from './modules/merge_and_uncode_rca_uploads'
// include { remove_patients_from_merged } from './modules/remove_patients_from_merged'
include { update_data_table } from './modules/update_data_table'
include { update_date_tracking_table } from './modules/update_date_tracking_table'
include { run_quac_table_report } from './modules/run_quac_table_report'
include { run_quac_comparison_report } from './modules/run_quac_comparison_report'
include { create_masking_report } from './modules/create_masking_report'
include { update_case_count_table } from './modules/update_case_count_table'
include { run_clinical_release } from './modules/run_clinical_release'
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow BPC_PIPELINE {
   ch_cohort = Channel.value(params.cohort)
   ch_comment = Channel.value(params.comment)
   
   if (params.step == "update_potential_phi_fields") {
    update_potential_phi_fields(ch_comment, params.production)
    // validate_data.out.view()
   } else if (params.step == "genie_bpc_pipeline"){
    update_potential_phi_fields(ch_comment, params.production)
    run_quac_upload_report_error(update_potential_phi_fields.out, ch_cohort)
    run_quac_upload_report_warning(run_quac_upload_report_error.out, ch_cohort, params.production)
    merge_and_uncode_rca_uploads(run_quac_upload_report_warning.out, ch_cohort, params.production)
    // remove_patients_from_merged(merge_and_uncode_rca_uploads.out, ch_cohort, params.production)
    update_data_table(merge_and_uncode_rca_uploads.out, ch_comment, params.production)
    update_date_tracking_table(update_data_table.out, ch_cohort, ch_comment, params.production)
    run_quac_table_report(update_date_tracking_table.out, ch_cohort, params.production)
    run_quac_comparison_report(run_quac_table_report.out, ch_cohort, params.production)
    create_masking_report(run_quac_comparison_report.out, ch_cohort, params.production)
    update_case_count_table(create_masking_report.out, ch_comment, params.production)
   } else {
    exit 1, 'step not supported'
   }
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    THE END
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow CLINICAL_RELEASE {
    run_clinical_release('', params.cohort, params.production)
}
