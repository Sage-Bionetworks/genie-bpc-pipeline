/*
Run workflow case selection
*/
process run_export_bpc_selected_cases {
    container 'sagebionetworks/genie-bpc-pipeline-case-selection'
    secret 'SYNAPSE_AUTH_TOKEN'
    debug true

    input:
    val bpc_input
    val output_synid
    val phase
    val cohort
    val center
    val release

    output:
    stdout

    script:
    """
    cd /usr/local/src/myscripts/
    Rscript export_bpc_selected_cases.R -i $bpc_input -o $output_synid --phase "$phase" --cohort $cohort --site $center --release $release
    """
}
