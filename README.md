# GENIE BPC Pipeline

## Overview

This NextFlow workflow runs a preliminary version of the GENIE BPC pipeline for the initial steps of processing data uploads.  

Workflow proceses
1. Check cohort selection against list of valid cohorts specified in the `nextflow.config` file
2. Extract SYNAPSE_AUTH_TOKEN from `.synapseConfig` file. 
3. Run the upload QA report to detect error level issues.  Note: this containerized R script returns an exit code corresponding to the number of issues detected.  If the number of issues is 0, the workflow continues.  If the number of issues is greater than 0, the workflow stops with an error. 
4. Run the upload QA report to detect warning level issues.  
5. Merge and uncode the REDCap dataset corresponding to the cohort.
6. Update Synapse tables with the merged and uncoded data.  
7. Update a Synapse table with references important for running the table QA reports.
8. Run the table QA report, which checks the newly updated Synapse tables.
9. Run the comparison QA report, which compares the newly updated Synapse tables with the previous table version.
10. Generate the drug masking reports.
11. Update the case count table on Synapse.  

## Installation

1. Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline
```

2. Install Nextflow and Docker.  Instructions are available at the following links: 

- https://www.nextflow.io/docs/latest/getstarted.html
- https://docs.docker.com/get-docker/

## Synapse credentials

Cache your Synapse credentials in `bin/.synapseConfig` with the following format:
```
[authentication]

# cache authtoken below
authtoken = {your_personal_access_token_here}
```

## Usage

To run the Nextflow pipeline:
```
nextflow run main.nf --cohort {cohort} --comment {comment}
```

Input parameters:
- `cohort`: BPC cohort code (for valid cohort codes see `nextflow.config`)
- `comment`: message to use for Synapse table snapshots regarding the update (e.g. "NSCLC public release")
