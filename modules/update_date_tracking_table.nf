/*
Update reference table storing the date of current and previous Synapse table updates
for later quality assurance checklist reports.s
*/
process update_date_tracking_table {

   container 'sagebionetworks/genie-bpc-pipeline-references'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous
   val cohort
   val comment
   val production

   output:
   stdout

   script:
   if (production) {
      """
      cd /usr/local/src/myscripts/
      Rscript update_date_tracking_table.R -c $cohort -d `date +'%Y-%m-%d'` -s "$comment"
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript update_date_tracking_table.R -c $cohort -d `date +'%Y-%m-%d'`
      """
   }
}