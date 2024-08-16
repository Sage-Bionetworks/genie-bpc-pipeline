/*
Run workflow case selection
*/
process run_workflow_case_selection {
    container 'sagebionetworks/genie-bpc-pipeline-case-selection'
    secret 'SYNAPSE_AUTH_TOKEN'
    debug true

    input:
    val phase
    val cohort
    val center
    val production

    output:
    stdout
    // path "nsclc_dfci_phase1_case_selection.csv"
    // path "nsclc_dfci_phase1_eligibility_matrix.csv"
    // path "${params.cohort}_dfci_phase1_eligibility_matrix.csv"

    script:
    if (production) {
        """
        cd /case_selection
        python3 workflow_case_selection.py --phase $phase --cohort $cohort --site $center -u
        """
    }
    else {
        """
        cd /case_selection
        python3 workflow_case_selection.py -p $phase -c $cohort -s $center
        """
    }
}
