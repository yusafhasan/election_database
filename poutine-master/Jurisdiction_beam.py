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
    state = jur_record.get('state')
    jname = jur_record.get('jname')
    fipscode = jur_record.get('fipscode')
    # suppress following comments to shorten hdv_modeled.ipynb
    # these print statements were used in debugging
    # print('jname: ' + jname)
    # print('fipscode: ' + fipscode)

    # fix a specific problem in VA
    # capitalized counties/cities have an extra suffix
    # Richmond and Roanoke are both county and city names
    # Each has a different fipscode
    if state == 'VA':
        if jname.endswith('COUNTY') :
            if jname == 'RICHMOND COUNTY':
                fipscode = 5115900000

            jname = jname[0:(jname.find(' COUNTY'))]
        elif jname.endswith('CITY'):
            if jname == 'RICHMOND CITY':
                fipscode = 5176000000
            else:
                jname = jname[0:(jname.find(' CITY'))]
    
    # fix a specific problem in NH
    # wards in NH have multiple formats
    if state == 'NH':
        if '-' in jname:
            jname = jname.replace('- ', '')
    
    # remove all periods in all strings
    if '.' in jname:
        jname = jname.replace('.', '')
    
    # standardize format of each jname
    jname = (jname.lower()).title()
    
    # simplifies fipscode to a 5-digit serial number
    # this format is more widely-used
    fipscode = fipscode//100000              
    
    # suppress output to reduce size of hdv_modeled.ipynb
    # used in debugging
    # print(state, jname, str(fipscode))        
    
    # update this element
    jur_record['jname'] = jname
    jur_record['fipscode'] = fipscode
            
        
    
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
     
     # suppress following print statement to reduce size of hdv_modeled.ipynb
     # used in debugging   
     # print('jur_record: ' + str(jur_record))
     
     # return singular copy of jurisdiction
     return [jur_record]  

# remove duplicate jurisdictions
class DedupJurRecords(beam.DoFn):
  def process(self, element):
     key, jur_object = element # student_obj is an _UnwindowedValues type
     jur_list = list(jur_object) # cast to list type to extract record
     jur_record = jur_list[0] # grab first jurisdiction record
     
     # suppress following print statement to reduce size of hdv_modeled.ipynb
     # used in debugging   
     # print('jur_record: ' + str(jur_record))
     
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
     sql = 'SELECT state, jname, fipscode FROM hdv_modeled.Jurisdiction where state = "NH" order by state, jname'
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

     dataset_id = 'hdv_modeled'
     table_id = 'Jurisdiction_Beam'
     schema_id = 'state:STRING,jname:STRING,fipscode:INTEGER'

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