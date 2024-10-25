/*
Update Synapse tables with merged and uncoded data.
*/
process update_data_table {
   container "$params.table_updates_docker"

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
