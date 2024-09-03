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
    val main_patient
    val main_sample
    val production

    output:
    stdout
    // path "nsclc_dfci_phase1_case_selection.csv"
    // path "nsclc_dfci_phase1_eligibility_matrix.csv"
    // path "${params.cohort}_dfci_phase1_eligibility_matrix.csv"

    script:
    if (production) {
        """
        cd /usr/local/src/myscripts/
        Rscript workflow_case_selection.R --phase $phase --cohort $cohort --site $center --main_patient $main_patient --main_sample $main_sample --production
        """
    }
    else {
        """
        cd /usr/local/src/myscripts/
        Rscript workflow_case_selection.R -p $phase -c $cohort -s $center --main_patient $main_patient --main_sample $main_sample
        """
    }
}
