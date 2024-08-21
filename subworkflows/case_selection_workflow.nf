// Case selection workflow
nextflow.enable.dsl = 2

// phase
params.phase = "1"
// cohort
params.cohort = "NSCLC"
// center
params.center = "DFCI"
params.production = false
export_phase = "phase " + params.phase

if (params.production) {
    bpc_output = "syn20798271"
} else {
    bpc_output = "syn62147862"
}

// import modules
include { run_workflow_case_selection } from '../modules/run_workflow_case_selection'
include { run_export_bpc_selected_cases } from '../modules/run_export_bpc_selected_cases'

workflow export_bpc_cases {
    run_export_bpc_selected_cases(params.bpc_input, bpc_output, export_phase, params.cohort, params.center)
}

workflow case_selection {
    run_workflow_case_selection(params.phase, params.cohort, params.center, params.production)
}

// TODO: This is commented out because the two steps currently don't connect together in a smooth way
// workflow case_selection_workflow {
//     run_workflow_case_selection(params.phase, params.cohort, params.center, params.production)
//     run_export_bpc_selected_cases(params.bpc_input, bpc_output, params.phase, params.cohort, params.center)
// }
