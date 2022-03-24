#!/usr/bin/env nextflow

synapse_config = Channel.fromPath( 'bin/.synapseConfig' )

process pyTest {

   input:
   file file_synapse_config from synapse_config

   output:
   file 'result.txt' 

   script:
   """
   send_email.py -c ${file_synapse_config} > result.txt
   """
}
