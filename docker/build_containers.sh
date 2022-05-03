#!/bin/sh

# Description: remove cache, build all docker containers, and push to repository.
# Author: Haley Hunter-Zinck
# Usage: sh build_containers.sh {docker_username}
# Date: 2022-04-01

docker_username=$1

docker system prune -a

docker build -t $docker_username/genie-bpc-pipeline-uploads ../scripts/uploads/
docker push $docker_username/genie-bpc-pipeline-uploads

docker build -t $docker_username/genie-bpc-pipeline-table-updates ../scripts/table_updates/
docker push $docker_username/genie-bpc-pipeline-table-updates

docker build -t $docker_username/genie-bpc-pipeline-references ../scripts/references/
docker push $docker_username/genie-bpc-pipeline-references

docker build -t $docker_username/genie-bpc-pipeline-masking ../scripts/masking/
docker push $docker_username/genie-bpc-pipeline-masking

docker build -t $docker_username/genie-bpc-pipeline-case-selection ../scripts/case_selection/
docker push $docker_username/genie-bpc-pipeline-case-selection
