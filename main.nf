#!/usr/bin/env nextflow

params.cohort = 'NSCLC'
params.comment = 'NSCLC public release update'
// testing or production pipeline
params.production = false

// Check if cohort is part of allowed cohort list
def allowed_cohorts = ["BLADDER", "BrCa", "CRC", "NSCLC", "PANC", "Prostate", "CRC2", "NSCLC2", "MELANOMA", "OVARIAN", "ESOPHAGO", "RENAL"]
if (!allowed_cohorts.contains(params.cohort)) {exit 1, 'Invalid cohort name'}

ch_cohort = Channel.value(params.cohort)
ch_comment = Channel.value(params.comment)

/*
Run quality asssurance checklist for the upload report at error level.  
Stop the workflow if any errors are detected.
*/
process quacUploadReportError {

   container 'sagebionetworks/genie-bpc-quac'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   // val previous from outCheckCohortCode
   val cohort        from ch_cohort

   output:
   stdout into outUploadReportError

   script:
   """
   cd /usr/local/src/myscripts/
   Rscript genie-bpc-quac.R -c $cohort -s all -r upload -l error -v
   """
}

outUploadReportError.view()

/*
Run quality asssurance checklist for the upload report at warning level.  
*/
process quacUploadReportWarning {

   container 'sagebionetworks/genie-bpc-quac'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous from outUploadReportError
   val cohort        from ch_cohort

   output:
   stdout into outUploadReportWarning

   script:
   if (params.production) {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r upload -l warning -u -v
      """
   } 
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r upload -l warning -v
      """
   }
}

outUploadReportWarning.view()

/*
Merge and uncode REDcap export data files.   
*/
process mergeAndUncodeRcaUploads {

   container 'sagebionetworks/genie-bpc-pipeline-uploads'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous from outUploadReportWarning
   val cohort        from ch_cohort

   output:
   stdout into outMergeAndUncodeRcaUploads

   script:
   if (params.production) {
      """
      cd /usr/local/src/myscripts/
      Rscript merge_and_uncode_rca_uploads.R -c $cohort -u -v
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript merge_and_uncode_rca_uploads.R -c $cohort -v
      """
   }
}

outMergeAndUncodeRcaUploads.view()

/*
Remove temporarily retracted (redacted) patients from merged upload file.
*/
process tmpRemovePatientsFromMerged {

   container 'sagebionetworks/genie-bpc-pipeline-uploads'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous from outMergeAndUncodeRcaUploads
   val cohort        from ch_cohort

   output:
   stdout into outTmpRemovePatientsFromMerged

   script:
   if (params.production) {
      """
      cd /usr/local/src/myscripts/
      Rscript remove_patients_from_merged.R -c $cohort -v
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript remove_patients_from_merged.R -c $cohort -v -o NA
      """
   }
}

outTmpRemovePatientsFromMerged.view()

/*
Update Synapse tables with merged and uncoded data.
*/
process updateDataTable {

   container 'sagebionetworks/genie-bpc-pipeline-table-updates'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous from outTmpRemovePatientsFromMerged
   val comment       from ch_comment

   output:
   stdout into outUpdateDataTable

   script:
   if (params.production) {
      """
      cd /root/scripts/
      python update_data_table.py -p /root/scripts/config.json -m "$comment" primary
      """
   }
   else {
      """
      cd /root/scripts/
      python update_data_table.py -p /root/scripts/config.json -m "$comment" primary -d
      """
   }
}

outUpdateDataTable.view()

/*
Update reference table storing the date of current and previous Synapse table updates
for later quality assurance checklist reports.s
*/
process updateDateTrackingTable {

   container 'sagebionetworks/genie-bpc-pipeline-references'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous from outUpdateDataTable
   val cohort        from ch_cohort
   val comment       from ch_comment

   output:
   stdout into outUpdateDateTrackingTable

   script:
   if (params.production) {
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

outUpdateDateTrackingTable.view()

/*
Run quality asssurance checklist for the table report at error and warning level.  
Do not stop the workflow if any issues are detected.
*/
process quacTableReport {

   container 'sagebionetworks/genie-bpc-quac'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous from outUpdateDateTrackingTable
   val cohort        from ch_cohort

   output:
   stdout into outTableReport

   script:
   if (params.production) {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l error -u -v
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l warning -u -v
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l error -v
      Rscript genie-bpc-quac.R -c $cohort -s all -r table -l warning -v
      """
   }
}

outTableReport.view()

/*
Run quality asssurance checklist for the comparison report at error and warning level.  
*/
process quacComparisonReport {

   container 'sagebionetworks/genie-bpc-quac'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous from outTableReport
   val cohort        from ch_cohort

   output:
   stdout into outComparisonReport

   script:
   if (params.production) {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l error -u -v
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l warning -u -v
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l error -v
      Rscript genie-bpc-quac.R -c $cohort -s all -r comparison -l warning -v
      """
   }
}

outComparisonReport.view()

/*
Create drug masking report files on most recent Synapse table data.  
*/
process maskingReport {

   container 'sagebionetworks/genie-bpc-pipeline-masking'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous from outComparisonReport
   val cohort        from ch_cohort

   output:
   stdout into outMaskingReport

   script:
   if (params.production) {
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

outMaskingReport.view()

/*
Update the case count table with current case counts calculated
from Synapse tables. 
*/
process updateCaseCountTable {

   container 'sagebionetworks/genie-bpc-pipeline-case-selection'
   secret 'SYNAPSE_AUTH_TOKEN'
   debug true

   input:
   val previous from outMaskingReport
   val comment       from ch_comment

   output:
   stdout into outCaseCount

   script:
   if (params.production) {
      """
      cd /usr/local/src/myscripts/
      Rscript update_case_count_table.R -s -c "$comment"
      """
   }
   else {
      """
      cd /usr/local/src/myscripts/
      Rscript update_case_count_table.R
      """
   }
}

outCaseCount.view()
