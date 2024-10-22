# genie-bpc-pipeline: Contributing Guidelines

## Nextflow Pipeline contribution

Here is how to contribute to the nextflow workflow of the genie-bpc-pipeline

### Adding a new process step

If you wish to contribute a new step, please use the following guidelines:

1. Add a new process step as a nextflow module under `genie-bpc-pipeline/modules`
2. Write the process step code and add to the appropriate module folder under `script/<module_folder_name>`
3. Add the process step to the workflow section in `main.nf` as a step
4. Add to any pre-existing process steps that needs this step as an input and vice versa
5. Add any new parameters to `nextflow_schema.json` with help text.
6. Add any new parameter's default values to the set parameter default values section in `main.nf`.
7. Add any additional validation for all relevant parameters. See validation section in `main.nf`

### Adding a new process module

We have automated docker builds to GHCR whenever there are changes to the scripts within a "module" as each module has its own image. Whenever a new module gets added, the github workflow `.github/workflows/build-docker-images.yml` should be updated.

1. Under `jobs` add your module name to `matrix:`
1. Once you push your changes, your docker image will build and will in the form: `<registry>/<repo>:<folder_name>-<branch>` (Example: `ghcr.io/genie-bpc-pipeline:references-gen-1485-update-potential-phi`)

### Default values

Parameters should be initialized / defined with default values in the set parameter default values section in `main.nf`

### Default processes resource requirements

Defaults for process resource requirements (CPUs / memory / time) for a process should be defined in `nextflow.config`.
