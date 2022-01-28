# standard imports
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# format jname to standard form
class Formatjname(beam.DoFn):
  def process(self, element):
    # extract each field of info from a given element
    jur_record = element
    jname = jur_record.get('jname')
    fipscode = jur_record.get('fipscode')
    state = jur_record.get('state')
    sub_hs_count = jur_record.get('sub_hs_count')
    hs_count = jur_record.get('hs_count')
    some_college_count = jur_record.get('some_collge_count')
    college_count = jur_record.get('college_count')
    sub_hs_pct = jur_record.get('sub_hs_pct')
    hs_pct = jur_record.get('hs_pct')
    ruc = jur_record.get('ruc')
    uic = jur_record.get('uic')
    econ_type = jur_record.get('econ_type')
    pop_chg_16 = jur_record.get('pop_chg_16')
    pop_chg_18 = jur_record.get('pop_chg_18')
    int_mig_16 = jur_record.get('int_mig_16')
    int_mig_18 = jur_record.get('int_mig_18')
    dom_mig_16 = jur_record.get('dom_mig_16')
    dom_mig_18 = jur_record.get('dom_mig_18')
    net_mig_16 = jur_record.get('net_mig_16')
    net_mig_18 = jur_record.get('net_mig_18')
    int_mig_rate_16 = jur_record.get('int_mig_rate_16')
    int_mig_rate_18 = jur_record.get('int_mig_rate_18')
    dom_mig_rate_16 = jur_record.get('dom_mig_rate_16')
    dom_mig_rate_18 = jur_record.get('dom_mig_rate_18')
    net_mig_rate_16 = jur_record.get('net_mig_rate_16')
    net_mig_rate_18 = jur_record.get('net_mig_rate_18')
    civ_labor_force_18 = jur_record.get('civ_labor_force_18')
    civ_labor_force_16 = jur_record.get('civ_labor_force_16')
    employed_count_16 = jur_record.get('employed_count_16')
    employed_count_18 = jur_record.get('employed_count_18')
    unemployed_count_16 = jur_record.get('unemployed_count_16')
    unemployed_count_18 = jur_record.get('unemployed_count_18')
    unemployment_rate_16 = jur_record.get('unemployment_count_16')
    unemployment_rate_18 = jur_record.get('unemployment_count_18')
    mhi_18 = jur_record.get('mhi_18')
    relative_mhi_18 = jur_record.get('relative_mhi_18')
    pop_16 = jur_record.get('pop_16')
    pop_18 = jur_record.get('pop_18')
    net_unemployment_change_16_18 = unemployed_count_16 - unemployed_count_18
    population_change_16_18 = pop_16 - pop_18
    pov_count_18 = jur_record.get('pov_count_18')
    pov_pct_18 = jur_record.get('pov_pct_18')
    pov_minor_count_18 = jur_record.get('pov_minor_count_18')
    pov_minor_pct_18 = jur_record.get('pov_minor_pct_18')
    pov_youth_count_18 = jur_record.get('pov_youth_count_18')
    pov_youth_pct_18 = jur_record.get('pov_youth_pct_18')
    pov_child_count_18 = jur_record.get('pov_child_count_18')
    pov_child_pct_18 = jur_record.get('pov_child_pct_18')
    pov_adult_count_18 = pov_count_18 - pov_minor_count_18
    
    if 'County' in jname:
        jname = jname.replace(' County', '')
    
    # update this element
    jur_record['jname'] = jname
    jur_record['fipscode'] = fipscode
    jur_record['net_unemployment_change_16_18'] = net_unemployment_change_16_18 
    jur_record['pov_adult_count_18'] = pov_adult_count_18
    jur_record['population_change_16_18'] = population_change_16_18
        
    
    # create key, value pairs
    key = state + jname
    jur_tuple = (key, jur_record)
    return [jur_tuple]

# remove duplicate jurisdictions
class DedupJurRecords(beam.DoFn):
  def process(self, element):
     key, jur_object = element # jur_obj is an _UnwindowedValues type
     jur_list = list(jur_object) # cast to list type to extract record
     jur_record = jur_list[0] # grab first jurisdiction record
     
     
     
     # return singular copy of jurisdiction
     return [jur_record]  

def run():
     PROJECT_ID = 'alert-result-266803' # change to your project id

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)
    
     # access entries with known cleaning issues
     sql = 'SELECT state, jname, fipscode, sub_hs_count, hs_count, college_count, some_college_count, sub_hs_pct, hs_pct, some_college_pct, college_pct, ruc, uic, econ_type, pop_16, pop_18, pop_chg_16, pop_chg_18, int_mig_16, int_mig_18, dom_mig_16, dom_mig_18, net_mig_16, net_mig_18, int_mig_rate_16, int_mig_rate_18, dom_mig_rate_16, dom_mig_rate_18, net_mig_rate_16, net_mig_rate_18, civ_labor_force_16, civ_labor_force_18, employed_count_16, employed_count_18, unemployment_rate_16, unemployment_rate_18, unemployed_count_16, unemployed_count_18,  pov_count_18, pov_minor_count_18, mhi_18, relative_mhi_18, pov_pct_18, pov_minor_pct_18, pov_youth_count_18, pov_youth_pct_18, pov_child_count_18, pov_child_pct_18 FROM ers_modeled.Jurisdiction_Demography where state = "NH" order by state, jname'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
     # write query_results to log file
     query_results | 'Write log 0' >> WriteToText('input.txt')

     # apply ParDo to format jname  
     formatted_jname_pcoll = query_results | 'Format JNAME' >> beam.ParDo(Formatjname())

     # write PCollection to log file
     # formatted_jname_pcoll | 'Write log 1' >> WriteToText('formatted_jname_pcoll.txt')

     # group jurisdictions by (state, jname)
     grouped_jname_pcoll = formatted_jname_pcoll | 'Group by jname' >> beam.GroupByKey()

     # write PCollection to log file
     # grouped_jname_pcoll | 'Write log 2' >> WriteToText('grouped_jname_pcoll.txt')

     # remove duplicate student records
     distinct_jur_pcoll = grouped_jname_pcoll | 'Dedup jurisdiction records' >> beam.ParDo(DedupJurRecords())
     # write PCollection to log file
     distinct_jur_pcoll | 'Write log 3' >> WriteToText('output.txt')

     dataset_id = 'ers_modeled'
     table_id = 'Jurisdiction_Demography_Beam'
     schema_id = 'state:STRING,jname:STRING,fipscode:INTEGER, sub_hs_count:INTEGER, hs_count:INTEGER, some_college_count:INTEGER, college_count:INTEGER, sub_hs_pct:FLOAT, hs_pct:FLOAT, some_college_pct:FLOAT, college_pct:FLOAT, ruc:INTEGER, uic:INTEGER, econ_type:INTEGER, pop_16:INTEGER, pop_18:INTEGER, pop_chg_16:INTEGER, pop_chg_18:INTEGER, population_change_16_18:INTEGER, int_mig_16:INTEGER, int_mig_18:INTEGER, dom_mig_16:INTEGER, dom_mig_18:INTEGER, net_mig_16:INTEGER, net_mig_18:INTEGER, int_mig_rate_16:FLOAT, int_mig_rate_18:FLOAT, dom_mig_rate_16:FLOAT, dom_mig_rate_18:FLOAT, net_mig_rate_16:FLOAT, net_mig_rate_18:FLOAT, civ_labor_force_16:INTEGER, civ_labor_force_18:INTEGER, employed_count_16:INTEGER, employed_count_18:INTEGER, unemployed_count_16:INTEGER, unemployed_count_18:INTEGER, net_unemployment_change_16_18:INTEGER, unemployment_rate_16:FLOAT, unemployment_rate_18:FLOAT, mhi_18:INTEGER, relative_mhi_18:FLOAT, pov_count_18:INTEGER, pov_adult_count_18:INTEGER, pov_pct_18:FLOAT, pov_minor_count_18:INTEGER, pov_minor_pct_18:FLOAT, pov_youth_count_18:INTEGER, pov_youth_pct_18:FLOAT, pov_child_count_18:INTEGER, pov_child_pct_18:FLOAT'
     # write PCollection to new BQ table
     distinct_jur_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  batch_size=int(100))
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()