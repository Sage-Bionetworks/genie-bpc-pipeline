#!/usr/bin/env nextflow

process pyTest {
   output:
   file 'result.txt' 
   """
   send_email.py > result.txt
   """
}
