#!/usr/bin/env nextflow

synapse_config = Channel.fromPath( 'bin/.synapseConfig' )
SYNAPSE_AUTH_TOKEN = Channel.value()

process getSynapseAuthToken {

   input:
   file file_synapse_config from synapse_config

   output: 
   env x into SYNAPSE_AUTH_TOKEN

   script:
   """
   x=`grep 'authtoken = ' ${file_synapse_config} | cut -f 3 -d ' '`
   rm ${file_synapse_config}
   """
}

process quacUploadReport {

   input:
   val SYNAPSE_AUTH_TOKEN

   output:
   stdout into outUploadReport

   script:
   """
   docker run -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN --rm $docker_username/genie-bpc-quac -c $cohort -s all -r upload -l error -v
   """
}

outUploadReport.view()

process mergeAndUncodeRcaUploads {

   input:
   val SYNAPSE_AUTH_TOKEN

   script:
   """
   docker run -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN --rm $docker_username/merge-and-uncode-rca-uploads -c $cohort
   """
}

process updateDataTable {

   input:
   val SYNAPSE_AUTH_TOKEN

   script:
   """
   docker run -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN --rm $docker_username/update-data-table -m $comment primary
   """
}

process updateDateTrackingTable {

   input:
   val SYNAPSE_AUTH_TOKEN

   script:
   """
   docker run -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN --rm $docker_username/update-date-tracking-table
   """
}

process quacTableReport {

   input:
   val SYNAPSE_AUTH_TOKEN

   output:
   stdout into outTableReport

   script:
   """
   docker run -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN --rm $docker_username/genie-bpc-quac -c $cohort -s all -r table -l error -v
   """
}

outTableReport.view()

process quacComparisonReport {

   input:
   val SYNAPSE_AUTH_TOKEN

   output:
   stdout into outComparisonReport

   script:
   """
   docker run -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN --rm $docker_username/genie-bpc-quac -c $cohort -s all -r comparison -l error -v
   """
}

outComparisonReport.view()

process maskingReport {

   input:
   val SYNAPSE_AUTH_TOKEN

   output:
   stdout into outMaskingReport

   script:
   """
   date_today=$(date +'%Y-%m-%d')
   docker run -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN --rm $docker_username/masking-report -c $cohort -d $date_today -s 
   """
}

outMaskingReport.view()

process updateCaseCountTable {

   input:
   val SYNAPSE_AUTH_TOKEN

   output:
   stdout into outCaseCount

   script:
   """
   date_today=$(date +'%Y-%m-%d')
   docker run -e SYNAPSE_AUTH_TOKEN=$SYNAPSE_AUTH_TOKEN --rm $docker_username/update-case-count-table -c $comment -s 
   """
}

outCaseCount.view()

process deleteSynapseConfigFileFromWorkDir {
   script:
   """
   rm $workDir/.synapseConfig
   """
}
