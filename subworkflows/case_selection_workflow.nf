// Case selection workflow
nextflow.enable.dsl = 2

// phase
params.phase = "1"
// cohort
params.cohort = "NSCLC"
// center
params.center = "DFCI"
params.production = false

// import modules
include { run_workflow_case_selection } from '../modules/run_workflow_case_selection.nf'

workflow case_selection_workflow {
    run_workflow_case_selection(params.phase, params.cohort, params.center, params.production)
}