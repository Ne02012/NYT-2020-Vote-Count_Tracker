import csv
import collections
import git
import json
import subprocess
import requests
import glob, os
import requests
import threading
import concurrent.futures
import io
import numpy as np

#state_names=["pennsylvania","georgia","wisconsin","michigan","arizona","nevada","virginia","minnesota","alabama","alaska","arkansas","california","colorado","connecticut","delaware","district-of-columbia","florida","hawaii","idaho","illinois","indiana","iowa","kansas","kentucky","louisiana","maine","maryland","massachusetts","mississippi","missouri","montana","nebraska","new-hampshire","new-jersey","new-mexico","new-york","north-carolina","north-dakota","ohio","oklahoma","oregon","rhode-island","south-carolina","south-dakota","tennessee","texas","utah","vermont","washington","west-virginia","wyoming"]
#state_short=["PA","GA","WI","MI","AZ","NV","VA","MN","AL","AK","CA","CO","CT","DE","DC","FL","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MS","MO","MT","NE","NH","NJ","NM","NY","NC","ND","OH","OK","OR","RI","SC","SD","TN","TX","UT","VT","WA","WV","WY"]
AK_INDEX = 0
AZ_INDEX = 3
GA_INDEX = 10
MI_INDEX = 22
NC_INDEX = 27
NV_INDEX = 33
PA_INDEX = 38
STATE_INDEXES = range(51) # 50 States + DC

state_short2Index = {'GA':GA_INDEX, 'MI':MI_INDEX, 'NC':NC_INDEX, 'PA':PA_INDEX}

precinct_metadata_url_list_files = ['GAPrecinctURLList.txt', 'MIPrecinctURLList.txt', 'NCPrecinctURLList.txt', 'PAPrecinctURLList.txt']

CACHE_DIR = '_cache'

InputRecord = collections.namedtuple(
    'InputRecord',
    [
        'state_index',
        'file_timestamp',
        'Votes_Remaining_File',
        'Precinct_Metadata_Filename',
        'state_last_updated',
        'County_Name',
        'eevp_value',
        'Precincts_Total',
        'Precincts_Reporting',
        'provisional_outstanding',
        'provisional_count_progress',
        'Total_Votes',
        'Total_Non_Absentee_Votes',
        'Total_Absentee_Votes',
        'Biden_Votes',
        'Trump_Votes',
        'Jorgensen_Votes',
        'Biden_Non_Absentee_Votes',
        'Trump_Non_Absentee_Votes',
        'Jorgensen_Non_Absentee_Votes',
        'Biden_Absentee_Votes',
        'Trump_Absentee_Votes',
        'Jorgensen_Absentee_Votes',
        'Biden_ElectionDay_Votes',
        'Trump_ElectionDay_Votes',
        'Jorgensen_ElectionDay_Votes',
        'Biden_Early_Votes',
        'Trump_Early_Votes',
        'Jorgensen_Early_Votes',
        'Biden_Provisional_Votes',
        'Trump_Provisional_Votes',
        'Jorgensen_Provisional_Votes',
    ],
)

recordFetched = {}

def git_commits_for(path):
    return subprocess.check_output(['git', 'log', "--format=%H", path]).strip().decode().splitlines()

def git_save(ref, name):
    # Open a temporary file in the cache directory to save the committed file, if it doesn't already exist
    cache_path = os.path.join(CACHE_DIR, ref + ".json")
    if os.path.exists(cache_path):
        return cache_path

    repo = git.Repo('.', odbt=git.db.GitCmdObjectDB)
    commit_tree = repo.commit(ref).tree
    with open(cache_path, mode='wb+') as f:
        commit_tree[name].stream_data(f)
        f.close

    return cache_path

def fetch_precinct_data(session, url):
    # extract original URL if it is Wayback Machine format like https://web.archive.org/web/20201104130928/https://static01.nyt.com/elections-assets/2020/data/api/2020-11-03/precincts/FLGeneralConcatenator-2020-11-04T10:34:53.284Z.js
    url = 'https://' + url.split('https://')[-1]
    filename = url.split('/')[-1].replace(':','-')
    cache_path = os.path.join(CACHE_DIR, 'precinct_metadata', filename)
    if not os.path.exists(cache_path):
        try:
            os.makedirs(os.path.dirname(cache_path))
        except FileExistsError:
            pass
        r = session.get(url, allow_redirects=True)
        if True==r.ok:
            with open(cache_path, mode='w+', encoding="utf8") as f:
                r.encoding="utf8"
                f.write(r.text)
                f.close()
    with open(cache_path, encoding="utf8") as f:
        js = json.load(f)
        
    if "county_by_vote_type" in js:
        max_size = len(js['county_by_vote_type'])
        js_list = js['county_by_vote_type']
    else:
        max_size = len(js['precincts'])
        js_list = js['precincts']
        
    out_array = np.zeros((254, max_size, 20), dtype=np.int32)
    out_locality_dict = {}
    locality_num = 0
    
    for js_locality in js_list:
        js_locality_name = js_locality['locality_name'].upper()
        if js_locality_name not in out_locality_dict:
            out_locality_dict[js_locality_name] = locality_num
            locality_num += 1
            locality_entry_index = 0
        if js_locality['is_reporting']:
            if 'absentee' == js_locality['vote_type']:
                # Store absentee vote totals
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 0] = int(js_locality['votes']) if None != js_locality['votes'] else 0
                # Store Biden absentee votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 5] = int(js_locality['results']['bidenj']) if None != js_locality['results']['bidenj'] else 0
                # Store Trump absentee votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 10] = int(js_locality['results']['trumpd']) if None != js_locality['results']['trumpd'] else 0
                # Store Jorgenesen absentee votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 15] = int(js_locality['results']['jorgensenj']) if None != js_locality['results']['jorgensenj'] else 0
            elif 'early' == js_locality['vote_type']:
                # Store early vote totals
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 1] = int(js_locality['votes']) if None != js_locality['votes'] else 0
                # Store Biden early votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 6] = int(js_locality['results']['bidenj']) if None != js_locality['results']['bidenj'] else 0
                # Store Trump early votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 11] = int(js_locality['results']['trumpd']) if None != js_locality['results']['trumpd'] else 0
                # Store Jorgenesen early votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 16] = int(js_locality['results']['jorgensenj']) if None != js_locality['results']['jorgensenj'] else 0
            elif 'electionday' == js_locality['vote_type']:
                # Store electionday vote totals
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 2] = int(js_locality['votes']) if None != js_locality['votes'] else 0
                # Store Biden electionday votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 7] = int(js_locality['results']['bidenj']) if None != js_locality['results']['bidenj'] else 0
                # Store Trump electionday votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 12] = int(js_locality['results']['trumpd']) if None != js_locality['results']['trumpd'] else 0
                # Store Jorgenesen electionday votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 17] = int(js_locality['results']['jorgensenj']) if None != js_locality['results']['jorgensenj'] else 0
            elif 'provisional' == js_locality['vote_type']:
                # Store provisional vote totals
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 3] = int(js_locality['votes']) if None != js_locality['votes'] else 0
                # Store Biden provisional votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 8] = int(js_locality['results']['bidenj']) if None != js_locality['results']['bidenj'] else 0
                # Store Trump provisional votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 13] = int(js_locality['results']['trumpd']) if None != js_locality['results']['trumpd'] else 0
                # Store Jorgenesen provisional votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 18] = int(js_locality['results']['jorgensenj']) if None != js_locality['results']['jorgensenj'] else 0
            elif 'total' == js_locality['vote_type']:
                # Store aggregate vote totals
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 4] = int(js_locality['votes']) if None != js_locality['votes'] else 0
                # Store Biden aggregate votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 9] = int(js_locality['results']['bidenj']) if None != js_locality['results']['bidenj'] else 0
                # Store Trump aggregate votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 14] = int(js_locality['results']['trumpd']) if None != js_locality['results']['trumpd'] else 0
                # Store Jorgenesen aggregate votes
                out_array[out_locality_dict[js_locality_name], locality_entry_index, 19] = int(js_locality['results']['jorgensenj']) if None != js_locality['results']['jorgensenj'] else 0
            locality_entry_index += 1

    # return number of localities, dictionary to locality indices and 2D array with sum of each vote type for each locality
    return locality_num, out_locality_dict, out_array.sum(axis=1)

def fetch_precinct_record(record, search_index, precinct_array):

        Total_Non_Absentee_Votes = record.Total_Non_Absentee_Votes if 'NA' != record.Total_Non_Absentee_Votes else precinct_array[search_index, 1]+precinct_array[search_index, 2]+precinct_array[search_index, 3]
        Total_Absentee_Votes = record.Total_Absentee_Votes if 'NA' != record.Total_Absentee_Votes else precinct_array[search_index, 0]
        Total_Votes = record.Total_Votes if 'NA' != record.Total_Votes else Total_Absentee_Votes + Total_Non_Absentee_Votes
        # Check for votes that were reported as aggregates
        if 0 == Total_Votes:
            Total_Votes = precinct_array[search_index, 0]
        Biden_Absentee_Votes = record.Biden_Absentee_Votes if 'NA' != record.Biden_Absentee_Votes else precinct_array[search_index, 5]
        Trump_Absentee_Votes = record.Trump_Absentee_Votes if 'NA' != record.Biden_Absentee_Votes else precinct_array[search_index, 10]
        Jorgensen_Absentee_Votes = record.Jorgensen_Absentee_Votes if 'NA' != record.Jorgensen_Absentee_Votes else precinct_array[search_index, 15]
        Biden_ElectionDay_Votes = precinct_array[search_index, 7]
        Trump_ElectionDay_Votes = precinct_array[search_index, 12]
        Jorgensen_ElectionDay_Votes = precinct_array[search_index, 17]
        Biden_Early_Votes = precinct_array[search_index, 6]
        Trump_Early_Votes = precinct_array[search_index, 11]
        Jorgensen_Early_Votes = precinct_array[search_index, 16]
        Biden_Provisional_Votes = precinct_array[search_index, 8]
        Trump_Provisional_Votes = precinct_array[search_index, 13]
        Jorgensen_Provisional_Votes = precinct_array[search_index, 18]
        Biden_Non_Absentee_Votes = record.Biden_Non_Absentee_Votes if 'NA' != record.Biden_Non_Absentee_Votes else Biden_ElectionDay_Votes+Biden_Early_Votes+Biden_Provisional_Votes
        Trump_Non_Absentee_Votes = record.Trump_Non_Absentee_Votes if 'NA' != record.Trump_Non_Absentee_Votes else Trump_ElectionDay_Votes+Trump_Early_Votes+Trump_Provisional_Votes
        Jorgensen_Non_Absentee_Votes = record.Jorgensen_Non_Absentee_Votes if 'NA' != record.Jorgensen_Non_Absentee_Votes else Jorgensen_ElectionDay_Votes+Jorgensen_Early_Votes+Jorgensen_Provisional_Votes
        Biden_Votes = record.Biden_Votes if 'NA' != record.Biden_Votes else Biden_Absentee_Votes+Biden_Non_Absentee_Votes
        # Check for votes that were reported as aggregates
        if 0 == Biden_Votes:
            Biden_Votes = precinct_array[search_index, 9]
        Trump_Votes = record.Trump_Votes if 'NA' != record.Trump_Votes else Trump_Absentee_Votes+Trump_Non_Absentee_Votes
        # Check for votes that were reported as aggregates
        if 0 == Trump_Votes:
            Trump_Votes = precinct_array[search_index, 14]
        Jorgensen_Votes = record.Jorgensen_Votes if 'NA' != record.Jorgensen_Votes else Jorgensen_Absentee_Votes+Jorgensen_Non_Absentee_Votes
        # Check for votes that were reported as aggregates
        if 0 == Jorgensen_Votes:
            Jorgensen_Votes = precinct_array[search_index, 19]
      
        return record._replace(
                            Total_Votes = Total_Votes,
                            Total_Non_Absentee_Votes = Total_Non_Absentee_Votes,
                            Total_Absentee_Votes = Total_Absentee_Votes,
                            Biden_Votes = Biden_Votes,
                            Trump_Votes = Trump_Votes,
                            Jorgensen_Votes = Jorgensen_Votes,
                            Biden_Non_Absentee_Votes = Biden_Non_Absentee_Votes,
                            Trump_Non_Absentee_Votes = Trump_Non_Absentee_Votes,
                            Jorgensen_Non_Absentee_Votes = Jorgensen_Non_Absentee_Votes,
                            Biden_Absentee_Votes = Biden_Absentee_Votes,
                            Trump_Absentee_Votes = Trump_Absentee_Votes,
                            Jorgensen_Absentee_Votes = Jorgensen_Absentee_Votes,
                            Biden_ElectionDay_Votes = Biden_ElectionDay_Votes,
                            Trump_ElectionDay_Votes = Trump_ElectionDay_Votes,
                            Jorgensen_ElectionDay_Votes = Jorgensen_ElectionDay_Votes,
                            Biden_Early_Votes = Biden_Early_Votes,
                            Trump_Early_Votes = Trump_Early_Votes,
                            Jorgensen_Early_Votes = Jorgensen_Early_Votes,
                            Biden_Provisional_Votes = Biden_Provisional_Votes,
                            Trump_Provisional_Votes = Trump_Provisional_Votes,
                            Jorgensen_Provisional_Votes = Jorgensen_Provisional_Votes
                        )


def fetch_record(session, ref, out, lock):
        global recordFetched
        cache_path = git_save(ref, 'results.json')
        with open(cache_path, encoding="utf8") as f:
                js = json.load(f)
        races = js['data']['races']
        file_timestamp = js['meta']['timestamp']
        num_states = min(51, len(races))
        max_counties = max([len(race['counties']) for race in races])
        county_strings = np.block([[[race['precinct_metadata']['timestamped_url'] if 'precinct_metadata' in race else 'NA',
                                     race['last_updated'],
                                     county['name'].upper(),
                                     county['eevp_value'],
                                     county['precincts'],
                                     county['reporting'],
                                     county['provisional_outstanding'] if None != county['provisional_outstanding'] else 'null',
                                     county['provisional_count_progress'] if None != county['provisional_count_progress'] else 'null'] for county in race['counties']]
                                     + [['null']*8]*(max_counties - len(race['counties'])) for race in races])
        county_results = np.block([[[int(county['votes']) if None != county['votes'] else 0,
                                     int(county['results']['bidenj']) if None != county['results']['bidenj'] else 0,
                                     int(county['results']['trumpd']) if None != county['results']['trumpd'] else 0,
                                     int(county['results']['jorgensenj']) if None != county['results']['jorgensenj'] else 0] for county in race['counties']]
                                     + [[0]*4]*(max_counties - len(race['counties'])) for race in races])
        county_results_absentee = np.block([[[int(county['absentee_votes']) if None != county['absentee_votes'] else 0,
                                              int(county['results_absentee']['bidenj']) if None != county['results_absentee']['bidenj'] else 0,
                                              int(county['results_absentee']['trumpd']) if None != county['results_absentee']['trumpd'] else 0,
                                              int(county['results_absentee']['jorgensenj']) if None != county['results_absentee']['jorgensenj'] else 0] for county in race['counties']]
                                              + [[0]*4]*(max_counties - len(race['counties'])) for race in races])
        county_results_non_absentee = county_results-county_results_absentee
        
        for index in range(num_states):
                # Check if the entry has already been added
                dictKey = f'{index}:{county_strings[index][0][1]}'
                lock.acquire()

                if dictKey in recordFetched:
                        lock.release()
                        continue
                else:
                        recordFetched[dictKey] = True
                        lock.release()

                precinct_metadata_url = county_strings[index][0][0] if 'null' != county_strings[index][0][0] and 'NA' != county_strings[index][0][0] else 'NA'
                precinct_metadata_filename = precinct_metadata_url.split('/')[-1].replace(':','-') if 'NA' != precinct_metadata_url else 'NA'

                # Fetch precinct metadata, if available
                if 'NA' != precinct_metadata_url:
                    num_localities, locality_dict, precinct_array = fetch_precinct_data(session, precinct_metadata_url)
                
                for i in range(county_results.shape[1]):
                    if 'null' != county_strings[index][i][0]:
                        record = InputRecord(
                                    index,
                                    file_timestamp,
                                    f'{ref}.json',
                                    precinct_metadata_filename,
                                    *county_strings[index][i][1:],
                                    county_results[index][i][0],
                                    county_results_non_absentee[index][i][0],
                                    county_results_absentee[index][i][0],
                                    *county_results[index][i][1:],
                                    *county_results_non_absentee[index][i][1:],
                                    *county_results_absentee[index][i][1:],
                                    *(['NA']*9)
                                )

                        # Fetch precinct metadata, if available
                        if 'NA' != precinct_metadata_url and county_strings[index][i][2] in locality_dict:
                                record = fetch_precinct_record(record, locality_dict[county_strings[index][i][2]], precinct_array)

                        out.append(record)
        return True

def fetch_precinct_only_record(session, url, out):
    state_short = url.split('/')[-1][:2]
    timestamp = url.split('/')[-1][-29:-10] + 'Z'
    state_index = state_short2Index[state_short]

    num_localities, locality_dict, precinct_array = fetch_precinct_data(session, url)

    for locality in locality_dict:
        record = InputRecord(
                    state_index,
                    timestamp,
                    'NA',
                    url.split('/')[-1].replace(':','-'),
                    timestamp,
                    locality,
                    *(['NA']*26)
                )

        # Fetch precinct metadata
        record = fetch_precinct_record(record, locality_dict[locality], precinct_array)

        if record.Total_Votes != 0:            
            out.append(record)
        
##    print(f'fetch_precinct_only_record(): done with {url}');    
    return True
    
    
def fetch_all_records():
        # Use connection pooling to improve network performance
        s = requests.Session()
        
        commits = git_commits_for("results.json")

        out = []

        lock = threading.Lock()

##        for ref in commits:
##            fetch_record(s, ref, out, lock)

        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
                # Start the load operations and mark each future with its reference
                future_to_ref = {executor.submit(fetch_record, s, ref, out, lock): ref for ref in commits}
                for future in concurrent.futures.as_completed(future_to_ref):
                        ref = future_to_ref[future]
                        try:
                            res = future.result()
                        except Exception as exc:
                            print('%s generated an exception: %s' % (ref, exc))

        PRECINCT_METADATA_URLS = []
        #Read the files containing the precinct_metadata URL lists for some states
        for filename in precinct_metadata_url_list_files:
            with open(filename, mode='r', encoding="utf8") as url_f:
                PRECINCT_METADATA_URLS.extend(url_f.read().splitlines())

        #Remove the precinct metadata URLs from the above list that are already in the results.json list
        for row in out:
            if 'NA' == row.Precinct_Metadata_Filename:
                continue
            for url in PRECINCT_METADATA_URLS:
                filename = url.split('/')[-1].replace(':','-')
                if filename == row.Precinct_Metadata_Filename:
                    PRECINCT_METADATA_URLS.remove(url)
                    break

##        for url in PRECINCT_METADATA_URLS:
##            fetch_precinct_only_record(s, url, out)

        # Now fetch and create records for precinct metadata URLs that were not captured in the results.json snapshot list
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
                # Start the load operations and mark each future with its reference
                future_to_ref = {executor.submit(fetch_precinct_only_record, s, url, out): url for url in PRECINCT_METADATA_URLS}
                for future in concurrent.futures.as_completed(future_to_ref):
                        url = future_to_ref[future]
                        try:
                            res = future.result()
                        except Exception as exc:
                            print('%s generated an exception: %s' % (url, exc))                            

        out.sort(key=lambda row: row.file_timestamp)
        grouped = collections.defaultdict(list)
        for row in out:
                grouped[row.state_index].append(row)

        return grouped

     
def write_csv(grouped):
        with open('CountyVoteCounts.csv', 'w', newline='') as csvfile:
                wr = csv.writer(csvfile)
                wr.writerow(('state', 'timestamp',) + InputRecord._fields[2:])
                for state_index in sorted(grouped):
                        state_records = grouped[state_index]
                        ref = state_records[-1].Votes_Remaining_File[:-5]
                        cache_path = git_save(ref, 'results.json')
                        with open(cache_path, encoding="utf8") as f:
                            js = json.load(f)
                        race = js['data']['races'][state_index]
                        for state_record in state_records:
                            wr.writerow((race['state_name'],) + state_record[1:])
        return                        
                        

grouped = fetch_all_records()
write_csv(grouped)

