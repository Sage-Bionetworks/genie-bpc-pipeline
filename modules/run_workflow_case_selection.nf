/*
Run workflow case selection
*/
process run_workflow_case_selection {
    container 'ghcr.io/sage-bionetworks/genie-bpc-pipeline:case_selection'
    secret 'SYNAPSE_AUTH_TOKEN'
    debug true

    input:
    val phase
    val cohort
    val center
    val production

    output:
    stdout

    script:
    if (production) {
        """
        python3 workflow_case_selection.py --phase $phase --cohort $cohort --site $center -u
        """
    }
    else {
        """
        python3 workflow_case_selection.py -p $phase -c $cohort -s $center
        """
    }
}

