# genie-bpc-pipeline: Contributing Guidelines

## Nextflow Pipeline contribution

Here is how to contribute to the nextflow workflow of the genie-bpc-pipeline

### Adding a new process step

If you wish to contribute a new step, please use the following guidelines:

1. Add a new process step as a module under `genie-bpc-pipeline/modules`
2. Write the process step code
3. Add the process step to the workflow section in `main.nf` as a step
4. Add to any pre-existing process steps that needs this step as an input and vice versa
5. Add any new parameters to `nextflow_schema.json` with help text.
6. Add any new parameter's default values to the set parameter default values section in `main.nf`.
7. Add any additional validation for all relevant parameters. See validation section in `main.nf`

### Default values

Parameters should be initialized / defined with default values in the set parameter default values section in `main.nf`

### Default processes resource requirements

Defaults for process resource requirements (CPUs / memory / time) for a process should be defined in `nextflow.config`.
