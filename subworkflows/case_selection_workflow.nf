// Case selection workflow
nextflow.enable.dsl = 2

// phase
params.phase = "1"
// cohort
params.cohort = "NSCLC"
// center
params.center = "DFCI"
params.production = false
params.bpc_input = "syn53294194"
params.bpc_output = "syn62147862"
params.release = "17.2-consortium"
export_phase = "phase " + params.phase

// entry validation
valid_entry_points = ['export_bpc_cases', 'case_selection']
if (!params.entry) {
    error "Entry point must be specified using --entry. Select one of: ${valid_entry_points.join(', ')}"
}
if (!valid_entry_points.contains(params.entry)) {
    error "Invalid entry point: '${params.entry}'. Valid options are: ${valid_entry_points.join(', ')}"
}

// import modules
include { run_workflow_case_selection } from '../modules/run_workflow_case_selection'
include { run_export_bpc_selected_cases } from '../modules/run_export_bpc_selected_cases'


workflow {
    if (params.entry == "export_bpc_cases"){
        // run_workflow_case_selection(params.phase, params.cohort, params.center, params.production)
        run_export_bpc_selected_cases(params.bpc_input, params.bpc_output, export_phase, params.cohort, params.center, params.release)
    } else if (params.entry == "case_selection"){
        run_workflow_case_selection(params.phase, params.cohort, params.center, params.release, params.production)
        // run_export_bpc_selected_cases(params.bpc_input, params.bpc_output, params.phase, params.cohort, params.center)
    } else {
        error "Invalid entry point: '${params.entry}'. Valid options are: '${valid_entry_points.join(', ')}'"
    }
}

// TODO: This is commented out because the two steps currently don't connect together in a smooth way
// workflow case_selection_workflow {
//     run_workflow_case_selection(params.phase, params.cohort, params.center, params.production)
//     run_export_bpc_selected_cases(params.bpc_input, params.bpc_output, params.phase, params.cohort, params.center)
// }
