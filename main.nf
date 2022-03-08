#!/usr/bin/env nextflow

syn_token = Channel.value("$SYNAPSE_AUTH_TOKEN")

process pyTest {

   input:
   env SYNAPSE_AUTH_TOKEN from syn_token

   output:
   file 'result.txt' 

   script:
   """
   send_email.py > result.txt
   """
}
