#!/usr/bin/env nextflow

project_dir = projectDir

process pyTest {
   output:
   file 'result.txt' 
   """
   python $project_dir/bin/send_email.py > result.txt
   """
}
