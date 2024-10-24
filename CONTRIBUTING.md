# genie-bpc-pipeline: Contributing Guidelines

## Getting started
1. [Clone the repository](https://help.github.com/articles/cloning-a-repository/) to your local machine so you can begin making changes.
2. On your local machine make sure you have the latest version of the `develop` branch:

    ```
    git checkout develop
    git pull origin develop
    ```
3. Create a feature branch off the `develop` branch and work on it. The branch should be named the same as the JIRA issue you are working on in **lowercase** (e.g., `gen-1234-{feature-here}`). Make sure the branch name as informative as possible. 
    ```
    git checkout develop
    git checkout -b gen-1234-{feature-here}
    ```
4. Once you have made your additions or changes, make sure you write tests and run [comparison scripts](https://github.com/Sage-Bionetworks/Genie_processing/blob/ed806d163fa4063a84920483e8ada21ea4b6cf47/README.md#comparisons-between-two-synapse-entities) to ensure changes are expected.
5. At this point, you have only created the branch locally, you need to push this to your fork on GitHub.

    ```
    git add your file
    git commit -m"your commit information"
    git push --set-upstream origin gen-1234-{feature-here}
    ```
6. Create a pull request from the feature branch to the develop branch. When changes are made to `script/<module_folder_name>` (only applies to scripts/references and scripts/table_updates for now), a Github action will be triggered to create a docker image for the branch, you can check it [here](https://github.com/Sage-Bionetworks/genie-bpc-pipeline/pkgs/container/genie-bpc-pipeline). 

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

### Testing
1. To test locally,  you can’t use ghcr.io/sage-bionetworks/docker_image_name locally directly. You will need to pull the docker image to local and test with it. 
2. To test on Sequra, go to the test_bpc_pipeline, edit the pipeline by pointing it to your feature branch then update. Doing this will allow you to select parameters from the dropdown menu directly. 
