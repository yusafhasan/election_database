# standard imports
import datetime, logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FormatCan(beam.DoFn):
  def process(self, element):
    # extract data from each record
    can_record = element
    cname = can_record.get('cname')
    party = can_record.get('party')
    # remove whitespace
    cname = cname.strip()
    
    # remove quotation marks in name
    if('"' in cname):
        cname = cname.replace('"','')
        
    if("'" in cname):
        cname = cname.replace("'",'')
    
    # remove parenthetical phrases in name, if present
    x = cname.find('(')
    y = cname.find(')')
    if (x != -1 and y != -1):
        x = cname.index('(')
        y = cname.index(')')
        cname = cname[0:x].strip() + ' ' + cname[(y+1):len(cname)].strip()
    
    # remove whitespace
    cname = cname.strip()
        
    # parse first and last names and store them as an ordered field
    split_name = cname.split(',')
    # special case that we could not correct via BQ
    if len(split_name) == 3:
        cname = 'Trey Hollingsworth'
    # if a comma is present
    elif len(split_name) > 1:
        cname = split_name[1].strip() + ' ' + split_name[0].strip()
    # if no comma present
    else:
        cname = split_name[0].strip()
    
    # standardize naming convention
    cname = (cname.lower()).title()
    if('.' in cname):
        cname = cname.replace('.','')
    if('Ii' in cname):
        cname = cname.replace('Ii', 'II')
    if('Iii' in cname):
        cname = cname.replace('Iii', 'III')
    if('Iv' in cname):
        cname = cname.replace('Iv', 'IV')
    
    # define key
    key = cname
    
    can_record['cname'] = cname
    can_record['party'] = party
    # create key, value pairs
    can_tuple = (key, can_record)
    return [can_tuple]

class DedupCanRecords(beam.DoFn):
  def process(self, element):
     key, can_obj = element # can_obj is an _UnwindowedValues type
     can_list = list(can_obj) # cast to list 
     
     # if a candidate appears multiple times due to having an 'Other'
     # party assigned, take the largest party candidate belongs to 
     # and use this party to be the party candidate is associated with
     for i in range(len(can_list)):
            current_party = can_list[i].get('party')
            if current_party == 'Democrat':
                return [can_list[i]]
            if current_party == 'Republican':
                return [can_list[i]]
            
     # if only has 'Other' as a party       
     can_record = can_list[0] # grab the first candidate record
     return [can_record]  

def run():
    PROJECT_ID = 'alert-result-266803' # change to your project id
    BUCKET = 'gs://poutine_bucket' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'student-df3'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

     # Create beam pipeline using local runner
    p = beam.Pipeline(options=options)
    
     # access entries with known cleaning issues
    sql = 'SELECT cname, party FROM hdv_modeled.Candidate'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
     # write query_results to log file
     # query_results | 'Write log 0' >> WriteToText('input.txt')

     # apply ParDo to format jname  
    formatted_can_pcoll = query_results | 'Format CNAME' >> beam.ParDo(FormatCan())

     # write PCollection to log file
     # formatted_jname_pcoll | 'Write log 1' >> WriteToText('formatted_jname_pcoll.txt')

     # group jurisdictions by (state, jname)
    grouped_can_pcoll = formatted_can_pcoll | 'Group by key' >> beam.GroupByKey()

     # write PCollection to log file
     # grouped_jname_pcoll | 'Write log 2' >> WriteToText('grouped_jname_pcoll.txt')

     # remove duplicate student records
    distinct_can_pcoll = grouped_can_pcoll | 'Dedup candidate records' >> beam.ParDo(DedupCanRecords())
     # write PCollection to log file
     # distinct_jur_pcoll | 'Write log 3' >> WriteToText('output.txt')

    dataset_id = 'hdv_modeled'
    table_id = 'Candidate_Beam_DF'
    schema_id = 'cname:STRING,party:STRING'

     # write PCollection to new BQ table
    distinct_can_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  batch_size=int(1000))
     
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()