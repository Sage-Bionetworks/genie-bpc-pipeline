#!/usr/bin/env nextflow

params.cohort = 'NSCLC'
params.comment = 'NSCLC public release update'
params.synapse_config = false  // Default
ch_synapse_config = params.synapse_config ? Channel.value(file(params.synapse_config)) : "null"

ch_cohort = Channel.value(params.cohort)
ch_comment = Channel.value(params.comment)
ch_synapse_config = Channel.value(file(params.synapse_config))

/*
Check cohort code is one of the valid values.
*/
process checkCohortCode {
    input:
    val cohort from ch_cohort

    output:
    stdout into outCheckCohortCode

    script:
    """
    echo $cohorts | tr ' ' '\n' | grep -c ^$cohort\$
    """
}

/*
Run quality asssurance checklist for the upload report at error level.  
Stop the workflow if any errors are detected.
*/
process quacUploadReportError {

   container 'hhunterzinck/genie-bpc-quac'

   secret 'SYNAPSE_AUTH_TOKEN'

   input:
   val previous from outCheckCohortCode
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outUploadReportError

   script:
   if ( params.synapse_config ) {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r upload -l error -v -a $syn_config
      """
   } else {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r upload -l error -v
      """
   }
}

outUploadReportError.view()

/*
Run quality asssurance checklist for the upload report at warning level.  
Do not stop the workflow if any issues are detected.
*/
process quacUploadReportWarning {

   container 'hhunterzinck/genie-bpc-quac'
   errorStrategy 'ignore'
   secret 'SYNAPSE_AUTH_TOKEN'

   input:
   val previous from outUploadReportError
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outUploadReportWarning

   script:
   if ( params.synapse_config ) {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r upload -l warning -u -v -a $syn_config
      """
   } else {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r upload -l warning -u -v
      """
   }
}

outUploadReportWarning.view()

/*
Merge and uncode REDcap export data files.   
*/
process mergeAndUncodeRcaUploads {

   container 'hhunterzinck/genie-bpc-pipeline-uploads'
   secret 'SYNAPSE_AUTH_TOKEN'

   input:
   val previous from outUploadReportWarning
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outMergeAndUncodeRcaUploads

   script:
   if ( params.synapse_config ) {
      """
      cd /usr/local/src/myscripts/
      Rscript merge_and_uncode_rca_uploads.R -c $cohort -u -a $syn_config -v
      """
   } else {
      """
      cd /usr/local/src/myscripts/
      Rscript merge_and_uncode_rca_uploads.R -c $cohort -u -v
      """
   }
}

outMergeAndUncodeRcaUploads.view()

/*
Update Synapse tables with merged and uncoded data.
*/
process updateDataTable {

   container 'hhunterzinck/genie-bpc-pipeline-table-updates'
   secret 'SYNAPSE_AUTH_TOKEN'

   input:
   val previous from outMergeAndUncodeRcaUploads
   file syn_config   from ch_synapse_config
   val comment       from ch_comment

   output:
   stdout into outUpdateDataTable

   script:
   if ( params.synapse_config ) {
      """
      cd /root/scripts/
      python update_data_table.py -s $syn_config -p /root/scripts/config.json -m $comment primary
      """
   } else {
      """
      cd /root/scripts/
      python update_data_table.py -p /root/scripts/config.json -m $comment primary
      """
   }
}

outUpdateDataTable.view()

/*
Update reference table storing the date of current and previous Synapse table updates
for later quality assurance checklist reports.s
*/
process updateDateTrackingTable {

   container 'hhunterzinck/genie-bpc-pipeline-references'
   secret 'SYNAPSE_AUTH_TOKEN'

   input:
   val previous from outUpdateDataTable
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort
   val comment       from ch_comment

   output:
   stdout into outUpdateDateTrackingTable

   script:
   if ( params.synapse_config ) {
      """
      cd /usr/local/src/myscripts/
      Rscript update_date_tracking_table.R -c $cohort -d `date +'%Y-%m-%d'` -s $comment -a $syn_config
      """
   } else {
      """
      cd /usr/local/src/myscripts/
      Rscript update_date_tracking_table.R -c $cohort -d `date +'%Y-%m-%d'` -s $comment
      """
   }
}

outUpdateDateTrackingTable.view()

/*
Run quality asssurance checklist for the table report at error and warning level.  
Do not stop the workflow if any issues are detected.
*/
process quacTableReport {

   container 'hhunterzinck/genie-bpc-quac'
   errorStrategy 'ignore'
   secret 'SYNAPSE_AUTH_TOKEN'

   input:
   val previous from outUpdateDateTrackingTable
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outTableReport

   script:
   if ( params.synapse_config ) {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l error -u -v -a $syn_config
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l warning -u -v -a $syn_config
      """
   } else {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l error -u -v
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l warning -u -v
      """
   }
}

outTableReport.view()

/*
Run quality asssurance checklist for the comparison report at error and warning level.  
Do not stop the workflow if any issues are detected.
*/
process quacComparisonReport {

   container 'hhunterzinck/genie-bpc-quac'
   errorStrategy 'ignore'
   secret 'SYNAPSE_AUTH_TOKEN'

   input:
   val previous from outTableReport
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outComparisonReport

   script:
   if ( params.synapse_config ) {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l error -u -v -a $syn_config
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l warning -u -v -a $syn_config
      """
   } else {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l error -u -v
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l warning -u -v
      """
   }
}

outComparisonReport.view()

/*
Create drug masking report files on most recent Synapse table data.  
*/
process maskingReport {

   container 'hhunterzinck/genie-bpc-pipeline-masking'
   secret 'SYNAPSE_AUTH_TOKEN'

   input:
   val previous from outComparisonReport
   file syn_config   from ch_synapse_config
   val cohort        from ch_cohort

   output:
   stdout into outMaskingReport

   script:
   if ( params.synapse_config ) {
      """
      cd /usr/local/src/myscripts/
      Rscript workflow_unmasked_drugs.R -c $cohort -d `date +'%Y-%m-%d'` -s -a $syn_config
      """
   } else {
      """
      cd /usr/local/src/myscripts/
      Rscript workflow_unmasked_drugs.R -c $cohort -d `date +'%Y-%m-%d'` -s
      """
   }
}

outMaskingReport.view()

/*
Update the case count table with current case counts calculated
from Synapse tables. 
*/
process updateCaseCountTable {

   container 'hhunterzinck/genie-bpc-pipeline-case-selection'
   secret 'SYNAPSE_AUTH_TOKEN'

   input:
   val previous from outMaskingReport
   file syn_config   from ch_synapse_config
   val comment       from ch_comment

   output:
   stdout into outCaseCount

   script:
   if ( params.synapse_config ) {
      """
      cd /usr/local/src/myscripts/
      Rscript update_case_count_table.R -s -c $comment -a $syn_config
      """
   } else {
      """
      cd /usr/local/src/myscripts/
      Rscript update_case_count_table.R -s -c $comment
      """
   }
}

outCaseCount.view()

