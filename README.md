# BPC Case Selection

## Installation

Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline/scripts/case_selection
```

Install all required R packages:
```
R -e 'renv::restore()'
```

## Synapse credentials

Cache your Synapse personal access token (PAT) as an environmental variable:
```
export SYNAPSE_AUTH_TOKEN={your_personal_access_token_here}
```

or store in ~/.synapseConfig with the following format:
```
[authentication]

# either authtoken OR username and password
authtoken = {your_personal_access_token_here}
```

## Usage: running the genie bpc nextflow pipeline

Skip to this section to learn about how to develop and run the nextflow pipeline locally: ([nextflow development](#nextflow-development))

## Usage: case selection

To display the command line interface:
```
Rscript workflow_case_selection.R -h
```

The command line interface will display as follows:
```
Usage: workflow_case_selection.R [options]


Options:
	-p PHASE, --phase=PHASE
		BPC phase

	-c COHORT, --cohort=COHORT
		BPC cohort

	-s SITE, --site=SITE
		BPC site

	-u, --save_synapse
		Save output to Synapse

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript workflow_case_selection.R  -p 1 -c NSCLC -s DFCI -u
```

## Usage: create GENIE export files

To display the command line interface:
```
Rscript export_bpc_selected_cases.R -h
```

The command line interface will display as follows:
```
Usage: export_bpc_selected_cases.R [options]


Options:
	-i INPUT, --input=INPUT
		Synapse ID of the input file that has the BPC selected cases

	-o OUTPUT, --output=OUTPUT
		Synapse ID of the BPC output folder. Default: syn20798271

	-p PHASE, --phase=PHASE
		BPC phase. i.e. pilot, phase 1, phase 1 additional

	-c COHORT, --cohort=COHORT
		BPC cohort. i.e. NSCLC, CRC, BrCa, and etc.

	-s SITE, --site=SITE
		BPC site. i.e. DFCI, MSK, UHN, VICC, and etc.

	-h, --help
		Show this help message and exit
```

## Usage: update table counts

To display the command line interface:
```
Rscript update_case_count_table.R -h
```

The command line interface will display as follows:
```
Usage: update_case_count_table.R [options]


Options:
	-s, --save_synapse
		Save updated counts on Synapse

	-c COMMENT, --comment=COMMENT
		Comment for new table snapshot version

	-a SYNAPSE_AUTH, --synapse_auth=SYNAPSE_AUTH
		Path to .synapseConfig file or Synapse PAT (default: '~/.synapseConfig')

	-h, --help
		Show this help message and exit
```

Example run: 
```
Rscript update_case_count_table.R -s -c 'nsclc phase 2 update' -a $SYNAPSE_AUTH_TOKEN
```

## Usage: update table with case selection criteria

To display the command line interface:
```
Rscript update_case_criteria_table.R -h
```

The command line interface will display as follows:
```
Usage: update_case_criteria_table.R [options]


Options:
        -s, --save_synapse
                Save updated counts on Synapse (default: FALSE)

        -c COMMENT, --comment=COMMENT
                Comment for new table snapshot version (default: 'update to case selection criteria')

        -v, --verbose
                Output script messages to the user (default: FALSE)

        -a AUTH, --auth=AUTH
                Synapse personal access token or path to .synapseConfig (default: normal synapse login behavior)

        -h, --help
                Show this help message and exit```
```

Example run: 
```
Rscript update_case_criteria_table.R -s  -c 'example run' -v
```

## Configuration

### Case selection for a new cohort
To conduct case selection for a new cohort, modify the `config.yaml` file following the template from another cohort.  Case selection for a novel cohort requires the cohort phase, name, production targets for each site, max and min date of sequencing, and a list of eligible OncoTree codes.  The root OncoTree code is just for reporting and chosen by Sage (usually just a parent code of eligible OncoTree codes).  Site-specific sequencing dates or SDV and IRR targets can be specified underneath each site key in the respective cohort.  The `adjusted` key under each site may be set to the production target on initialization.  
### Case selection for a new site
To conduct case selection for a new site, modify the `config.yaml` file. Add in the new site under `default` by adding the `pressure` and `seed` values. Also add in the `production` and `adjusted` case counts for each cohort for the new site following the template from another site. Make sure you create a new folder named after the new site in each cohort subfolder under this folder (syn20781633).

### Adjusted target numbers
During curation, sites may find cases to be ineligible and may have no additional case for which to substitute the ineligible case.  To account for expected reductions in the target count, update the `adjusted` target number in the `config.yaml` file.  These numbers will then be reflected in the `Case Selection Counts` Table on Synapse (syn26228746) and used in the QA checks to verify the number of submitted cases from each site matches the adjusted target.  

### Selection of additional samples
After initial case selection and curation, additional samples for selected cases may be submitted through the main GENIE releases.  These additional samples are collected for curators upon request.  These cohorts are defined in much the same way as other cohorts but without production targets.  See format under the `1_additional` key in the `config.yaml` file for an example.

## Nextflow development

Follow instructions here for running the genie BPC pipeline nextflow workflow locally

An EC2 instance is **required** to run processing and develop locally. Follow instructions using [Service-Catalog-Provisioning](https://help.sc.sageit.org/sc/Service-Catalog-Provisioning.938836322.html) to create an ec2 on service catalog. You will also want to follow the section [SSM with SSH](https://help.sc.sageit.org/sc/Service-Catalog-Provisioning.938836322.html#ServiceCatalogProvisioning-SSMwithSSH) if you want to use VS code to run/develop.

For GENIE BPC, here are the specification recommendations when launching an EC2 instance:

- EC2 Product: Linux with Docker
- EC2 Instance Type: t3.2xlarge
- Disk size: ~100 GB

### Nextflow Dependencies

- [Java 8 or later](https://www.java.com/en/download/)
- [Nextflow 21.04.x or later](https://www.nextflow.io/docs/latest/getstarted.html#get-started)


### Nextflow Configuration

Prior to running the pipeline, you will need to create a Nextflow secret called `SYNAPSE_AUTH_TOKEN`
with a Synapse personal access token ([docs](#authentication)).

### Authentication

This workflow takes care of transferring files to and from Synapse. Hence, it requires a secret with a personal access token for authentication. To configure Nextflow with such a token, follow these steps:

1. Generate a personal access token (PAT) on Synapse using [this dashboard](https://www.synapse.org/#!PersonalAccessTokens:). Make sure to enable the `view`, `download`, and `modify` scopes since this workflow both downloads and uploads to Synapse.
2. Create a secret called `SYNAPSE_AUTH_TOKEN` containing a Synapse personal access token using the [Nextflow CLI](https://nextflow.io/docs/latest/secrets.html)

### Commands

You can visit [parameters](https://github.com/Sage-Bionetworks/genie-bpc-pipeline/blob/develop/main.nf#L2-L11) to see the list of currently available parameters/flags and their default values if you don't specify any.

### Running nextflow using an EC2

1. For an ec2 instance with Linux and docker, see here for installing Java 11: [How do I install a software package from the Extras Library on an EC2 instance running Amazon Linux 2?](https://aws.amazon.com/premiumsupport/knowledge-center/ec2-install-extras-library-software/)

2. Install nextflow by following instructions here: [Get started — Nextflow](https://www.nextflow.io/docs/latest/getstarted.html#get-started). Update your `PATH` variable to include the directory where your nextflow executable is installed at.

3. Make sure to set any nextflow secrets using the Nextflow Cli: [Secrets — Nextflow](https://www.nextflow.io/docs/latest/secrets.html#command-line). You will need to set a `SYNAPSE_AUTH_TOKEN` secret for running the nextflow genie repo by doing 

```
nextflow secrets set SYNAPSE_AUTH_TOKEN “INSERT YOUR SYNAPSE TOKEN HERE”
```

4. Run the pipeline with the default parameter settings

```bash
nextflow main.nf
```

If you want to pass values to the parameter settings, you can use the help flag to see what parameters you can set:

```bash
nextflow main.nf --help
```

If you want to run the pipeline in production mode with the default parameter settings:

```bash
nextflow main.nf --production
```

Note: you can also chose what version of nextflow to run with using:

```bash
NXF_VER=<nextflow_version> nextflow main.nf
```

## Citations
This pipeline uses some code and infrastructure developed and maintained by the [nf-core](https://nf-co.re) community, reused here under the [MIT license](https://github.com/nf-core/tools/blob/master/LICENSE).
