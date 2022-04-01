#!/bin/sh

# Description: build all docker containers and push to repository.
# Author: Haley Hunter-Zinck
# Usage: sh build_containers.sh {docker_username}
# Date: 2022-04-01

docker_username=$1

docker build -t $docker_username/merge-and-uncode-rca-uploads ../scripts/uploads/
docker push $docker_username/merge-and-uncode-rca-uploads

docker build -t $docker_username/update-data-table ../scripts/table_updates/
docker push $docker_username/update-data-table

docker build -t $docker_username/update-date-tracking-table ../scripts/references/
docker push $docker_username/update-date-tracking-table

docker build -t $docker_username/masking-report ../scripts/masking/
docker push $docker_username/masking-report

docker build -t $docker_username/update-case-count-table ../scripts/case_selection/
docker push $docker_username/update-case-count-table
