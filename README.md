# GENIE BPC Pipeline

## Overview

This NextFlow workflow runs a preliminary version of the GENIE BPC pipeline for the initial steps of processing data uploads.  

Workflow proceses
1. Check cohort selection against list of valid cohorts specified in the `nextflow.config` file
1. Run the upload QA report to detect error level issues.  Note: this containerized R script returns an exit code corresponding to the number of issues detected.  If the number of issues is 0, the workflow continues.  If the number of issues is greater than 0, the workflow stops with an error. 
1. Run the upload QA report to detect warning level issues.  
1. Merge and uncode the REDCap dataset corresponding to the cohort.
1. Remove patient IDs in the temporary retraction table.  These IDs mostly correspond to redacted patients for which PHI redaction issues are still being resolved pending decision from the stats team.  
1. Update Synapse tables with the merged and uncoded data.  
1. Update a Synapse table with references important for running the table QA reports.
1. Run the table QA report, which checks the newly updated Synapse tables.
1. Run the comparison QA report, which compares the newly updated Synapse tables with the previous table version.
1. Generate the drug masking reports.
1. Update the case count table on Synapse.  

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

Cache your Synapse personal access token (PAT) as a secret named `SYNAPSE_AUTH_TOKEN` or store in a file named `.synapseConfig` with the following format:
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
- `synapseConfig`: (optional) path to `.synapseConfig` file.  If not specified, program uses `SYNAPSE_AUTH_TOKEN` secret.

## DockerHub builds
The docker images built through this repo are automatically built everytime a change to pushed into the `develop` branch.  This is automatically set up via the automated build feature on DockerHub.

- [sagebionetworks/genie-bpc-pipeline-case-selection](https://hub.docker.com/repository/docker/sagebionetworks/genie-bpc-pipeline-case-selection)

    [![automated](https://img.shields.io/docker/cloud/automated/sagebionetworks/genie-bpc-pipeline-case-selection)](https://hub.docker.com/r/sagebionetworks/genie-bpc-pipeline-case-selection)
    ![status](https://img.shields.io/docker/cloud/build/sagebionetworks/genie-bpc-pipeline-case-selection)

- [sagebionetworks/genie-bpc-pipeline-references](https://hub.docker.com/repository/docker/sagebionetworks/genie-bpc-pipeline-references)

    [![automated](https://img.shields.io/docker/cloud/automated/sagebionetworks/genie-bpc-pipeline-references)](https://hub.docker.com/r/sagebionetworks/genie-bpc-pipeline-references)
    ![status](https://img.shields.io/docker/cloud/build/sagebionetworks/genie-bpc-pipeline-references)

- [sagebionetworks/genie-bpc-pipeline-table-updates](https://hub.docker.com/repository/docker/sagebionetworks/genie-bpc-pipeline-table-updates)

    [![automated](https://img.shields.io/docker/cloud/automated/sagebionetworks/genie-bpc-pipeline-table-updates)](https://hub.docker.com/r/sagebionetworks/genie-bpc-pipeline-table-updates)
    ![status](https://img.shields.io/docker/cloud/build/sagebionetworks/genie-bpc-pipeline-table-updates)

- [sagebionetworks/genie-bpc-pipeline-uploads](https://hub.docker.com/repository/docker/sagebionetworks/genie-bpc-pipeline-uploads)

    [![automated](https://img.shields.io/docker/cloud/automated/sagebionetworks/genie-bpc-pipeline-uploads)](https://hub.docker.com/r/sagebionetworks/genie-bpc-pipeline-uploads)
    ![status](https://img.shields.io/docker/cloud/build/sagebionetworks/genie-bpc-pipeline-uploads)

- [sagebionetworks/genie-bpc-pipeline-masking](https://hub.docker.com/repository/docker/sagebionetworks/genie-bpc-pipeline-masking)

    [![automated](https://img.shields.io/docker/cloud/automated/sagebionetworks/genie-bpc-pipeline-masking)](https://hub.docker.com/r/sagebionetworks/genie-bpc-pipeline-masking)
    ![status](https://img.shields.io/docker/cloud/build/sagebionetworks/genie-bpc-pipeline-masking)
