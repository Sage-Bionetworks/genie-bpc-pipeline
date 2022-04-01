# GENIE BPC Pipeline

## Overview

This NextFlow workflow runs a preliminary version of the GENIE BPC pipeline for the initial steps of processing data uploads.  

Workflow proceses
1. Extract SYNAPSE_AUTH_TOKEN from .synapseConfig file.  This is necessary because the second step is a containerized R script that requires Synapse personal access token to be passes as a parameter or an environmental variable.
2. Run the upload QA report.  This containerized R script returns an exit code corresponding to the number of issues detected.  If the number of issues is 0, the workflow continues.  If the number of issues is greater than 0, the workflow stops with an error. 
3. Merge and uncode the REDCap dataset corresponding to the cohort specified in `nextflow.config` file.
4. Update Synapse tables with the merged and uncoded data.  
5. Update a Synapse table with references important for running the table QA reports.
6. Run the table QA report, which checks the newly updated Synapse tables.
7. Run the comparison QA report, which compares the newly updated Synapse tables with the previous table version.
8. Generate the drug masking reports.  The reports are automatically uploaded to Synapse.
9. Update the case count table on Synapse.  

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
nextflow run main.nf
```
