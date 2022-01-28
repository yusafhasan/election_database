# standard imports
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

# format jname to standard form
class FormatResults(beam.DoFn):
  def process(self, element):
    # extract each field of info from a given element
    res_record = element
    state = res_record.get('state')
    jname = res_record.get('jname')
    fipscode = res_record.get('fipscode')
    cname = res_record.get('cname')
    party = res_record.get('party')
    office = res_record.get('office')
    year = res_record.get('year')
    votes = res_record.get('votes')
    total_votes = res_record.get('total_votes')
    # suppress following comments to shorten hdv_modeled.ipynb
    # these print statements were used in debugging
    # print('jname: ' + jname)
    # print('fipscode: ' + fipscode)

    # fix a specific problem in VA
    # capitalized counties/cities have an extra suffix
    if state == 'VA':
        if jname.endswith('COUNTY') :
            jname = jname[0:(jname.find(' COUNTY'))]
        elif jname.endswith('CITY'):
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
    res_record['jname'] = jname
    res_record['fipscode'] = fipscode
    
    
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
    
    res_record['cname'] = cname
    res_record['party'] = party
    
    # create a vote_pct column
    # initialize a winner column to be defined later
    res_record['vote_pct'] = round(votes/total_votes * 100,2)
    res_record['winner'] = 0
    # create key, value pairs
    key = cname
    res_tuple = (key, res_record)
    return [res_tuple]

# remove duplicate candidates
class DedupCanRecords(beam.DoFn):
  def process(self, element):
     key, can_obj = element # can_obj is an _UnwindowedValues type
     can_list = list(can_obj) # cast to list 
     
     # if a candidate appears multiple times due to having an 'Other'
     # party assigned, take the largest party candidate belongs to 
     # and use this party to be the party candidate is associated with
     correct_party = 'Other'
     for i in range(len(can_list)):
            current_party = can_list[i].get('party')
            if current_party == 'Democrat':
                correct_party = 'Democrat'
                break
            if current_party == 'Republican':
                correct_party = 'Republican'
                break
     # yield each record with the correct party
     for i in range(len(can_list)):
            can = can_list[i]
            can['party'] = correct_party
            yield [can]

# Group candidates that run in the same race in the same year/jurisdiction
class GroupCandidates(beam.DoFn):
    def process(self, element):
        # extract each field of info from a given element
        res_record = element[0]
        state = res_record.get('state')
        jname = res_record.get('jname')
        fipscode = res_record.get('fipscode')
        cname = res_record.get('cname')
        party = res_record.get('party')
        office = res_record.get('office')
        year = res_record.get('year')
        votes = res_record.get('votes')
        total_votes = res_record.get('total_votes')
        vote_pct = res_record.get('vote_pct')
        
        key = state + jname + office + str(year)
        res_tuple = (key, res_record)
        return [res_tuple]

# define winners and losers
class AssignWinners(beam.DoFn):
  def process(self, element):
     key, res_object = element # res_obj is an _UnwindowedValues type
     res_list = list(res_object) # cast to list type to extract record
     # criterion for winner: most votes
     max_votes = max([x.get('votes') for x in res_list])
     # define each candidate in this race as a winner or loser
     for x in res_list:
        if x.get('votes') == max_votes:
            x['winner'] = 1
        else:
            x['winner'] = 0
        yield x
     # res_record = res_list[0] # grab first jurisdiction record
     
     # suppress following print statement to reduce size of hdv_modeled.ipynb
     # used in debugging   
     # print('jur_record: ' + str(jur_record))
     
     # return singular copy of jurisdiction
     # return [res_record]  

# define the primary key for each record
class GroupResults(beam.DoFn):
    def process(self, element):
        # extract each field of info from a given element
        res_record = element
        state = res_record.get('state')
        jname = res_record.get('jname')
        fipscode = res_record.get('fipscode')
        cname = res_record.get('cname')
        party = res_record.get('party')
        office = res_record.get('office')
        year = res_record.get('year')
        votes = res_record.get('votes')
        total_votes = res_record.get('total_votes')
        vote_pct = res_record.get('vote_pct')
        
        key = state + jname + cname + party + office + str(year)
        res_tuple = (key, res_record)
        return [res_tuple]
    
# dedup results records    
class DedupResRecords(beam.DoFn):
  def process(self, element):
     key, res_object = element # student_obj is an _UnwindowedValues type
     res_list = list(res_object) # cast to list type to extract record
     res_record = res_list[0] # grab first jurisdiction record
     
     # suppress following print statement to reduce size of hdv_modeled.ipynb
     # used in debugging   
     # print('jur_record: ' + str(jur_record))
     
     # return singular copy of jurisdiction
     return [res_record]  


        
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
     sql = 'SELECT state, jname, fipscode, cname, party, office, year, votes, total_votes FROM hdv_modeled.Results WHERE state = "DE" '
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
     # write query_results to log file
     # query_results | 'Write log 0' >> WriteToText('input.txt')

     # apply ParDo to format jname  
     formatted_res_pcoll = query_results | 'Format JNAME' >> beam.ParDo(FormatResults())

     # write PCollection to log file
     # formatted_jname_pcoll | 'Write log 1' >> WriteToText('formatted_jname_pcoll.txt')

     # group jurisdictions by (state, jname)
     grouped_res_pcoll = formatted_res_pcoll | 'Group by jname' >> beam.GroupByKey()

     # write PCollection to log file
     # grouped_jname_pcoll | 'Write log 2' >> WriteToText('grouped_jname_pcoll.txt')

     # remove duplicate student records
     distinct_res_pcoll = grouped_res_pcoll | 'Dedup jurisdiction records' >> beam.ParDo(DedupCanRecords())
     # write PCollection to log file
     # distinct_jur_pcoll | 'Write log 3' >> WriteToText('output.txt')
     formatted_win_pcoll = distinct_res_pcoll | 'Group by election' >> beam.ParDo(GroupCandidates())
    
     grouped_win_pcoll = formatted_win_pcoll | 'Form election groups' >> beam.GroupByKey()
    
     distinct_win_pcoll = grouped_win_pcoll | 'Assign winners/losers' >> beam.ParDo(AssignWinners())
        
     formatted_fin_pcoll = distinct_win_pcoll | 'Group by primary key' >> beam.ParDo(GroupResults())
    
     grouped_fin_pcoll = formatted_fin_pcoll | 'Group duplicates together' >> beam.GroupByKey()
    
     distinct_fin_pcoll = grouped_fin_pcoll | 'Dedup results records' >> beam.ParDo(DedupResRecords())
    
     dataset_id = 'hdv_modeled'
     table_id = 'Results_Beam'
     schema_id = 'state:STRING,jname:STRING,fipscode:INTEGER,cname:STRING,party:STRING,office:STRING,year:INTEGER,votes:INTEGER,total_votes:INTEGER,vote_pct:FLOAT,winner:INTEGER'

     # write PCollection to new BQ table
     distinct_fin_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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