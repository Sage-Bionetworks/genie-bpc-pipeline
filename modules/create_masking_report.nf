/*
Create drug masking report files on most recent Synapse table data.  
*/
process create_masking_report {

   container 'sagebionetworks/genie-bpc-pipeline-masking'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous
   val cohort
   val production

   output:
   stdout

   script:
   if (production) {
      """
      cd /usr/local/src/myscripts/
      Rscript workflow_unmasked_drugs.R -c $cohort -d `date +'%Y-%m-%d'` -s
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript workflow_unmasked_drugs.R -c $cohort -d `date +'%Y-%m-%d'`
      """
   }
}

