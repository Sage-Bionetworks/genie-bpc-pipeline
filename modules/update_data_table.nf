/*
Update Synapse tables with merged and uncoded data.
*/
process update_data_table {

   container 'sagebionetworks/genie-bpc-pipeline-table-updates'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous
   val cohort
   val production
   val comment
   val dry_run

   output:
   stdout

   script:
   if (production) {
      """
      cd /root/scripts/
      python update_data_table.py -p /root/scripts/config.json -c $cohort -m "$comment" primary -pd
      """
   } else {
      """
      cd /root/scripts/
      python update_data_table.py -p /root/scripts/config.json -c $cohort -m "$comment" primary
      """
   }
}
