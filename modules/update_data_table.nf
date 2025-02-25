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
   val replace_patient_tier1a
   val replace_sample_tier1a

   output:
   stdout

   script:
   if (production) {
      """
      cd /root/scripts/
      python update_data_table.py -p /root/scripts/config.json -c $cohort -m "$comment" primary -pd -rp $replace_patient_tier1a -rs $replace_sample_tier1a
      """
   } else {
      """
      cd /root/scripts/
      python update_data_table.py -p /root/scripts/config.json -c $cohort -m "$comment" primary -rp $replace_patient_tier1a -rs $replace_sample_tier1a
      """
   }
}
