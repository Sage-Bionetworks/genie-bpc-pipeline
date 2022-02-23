# GENIE BPC Pipeline

## Installation

1. Clone this repository and navigate to the directory:
```
git clone git@github.com:Sage-Bionetworks/genie-bpc-pipeline.git
cd genie-bpc-pipeline
```

2. Install Nextflow and Docker.  Instructions are available at the following links: 

- https://www.nextflow.io/docs/latest/getstarted.html

- https://docs.docker.com/get-docker/

3. Build the Docker container:
```
docker build -t geniebpc .
```

## Synapse credentials

Cache your Synapse personal access token (PAT) as an environmental variable:
```
export SYNAPSE_AUTH_TOKEN={your_personal_access_token_here}
```

## Usage

To run the Nextflow pipeline:
```
nextflow run geniebpc.nf
```

