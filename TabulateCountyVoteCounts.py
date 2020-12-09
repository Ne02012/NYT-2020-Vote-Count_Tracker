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

CACHE_DIR = '_cache'

InputRecord = collections.namedtuple(
    'InputRecord',
    [
        'state_index',
        'file_timestamp',
        'Votes_Remaining_File',
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

def fetch_record(session, ref, out, lock):
        global recordFetched
        cache_path = git_save(ref, 'results.json')
        with open(cache_path, encoding="utf8") as f:
                js = json.load(f)
        races = js['data']['races']
        file_timestamp = js['meta']['timestamp']
        num_states = min(51, len(races));
        for index in range(num_states):
                race = races[index]
                try:
                    timestamp = race['last_updated']
                except Exception as exc:
                    print('%s at index %d generated an exception: %s' % (ref, index, exc))
                    continue

                # Check if the entry has already been added
                dictKey = f'{race["state_id"]}:{timestamp}'
                lock.acquire()

                if dictKey in recordFetched:
                        lock.release()
                        continue
                else:
                        recordFetched[dictKey] = True
                        lock.release()
                
                for county in race['counties']:
                    record = InputRecord(
                                index,
                                file_timestamp,
                                f'{ref}.json',
                                timestamp,
                                county['name'],
                                county['eevp_value'],
                                county['precincts'],
                                county['reporting'],
                                county['provisional_outstanding'] if None != county['provisional_outstanding'] else 'null',
                                county['provisional_count_progress'] if None != county['provisional_count_progress'] else 'null',
                                county['votes'],
                                county['votes'] - county['absentee_votes'],
                                county['absentee_votes'],
                                county['results']['bidenj'],
                                county['results']['trumpd'],
                                county['results']['jorgensenj'],
                                county['results']['bidenj'] - county['results_absentee']['bidenj'],
                                county['results']['trumpd'] - county['results_absentee']['trumpd'],
                                county['results']['jorgensenj'] - county['results_absentee']['jorgensenj'],
                                county['results_absentee']['bidenj'],
                                county['results_absentee']['trumpd'],
                                county['results_absentee']['jorgensenj']
                            )

                    out.append(record)
        return True
    
def fetch_all_records():
        # Use connection pooling to improve network performance
        s = requests.Session()
        
        commits = git_commits_for("results.json")

        out = []

        lock = threading.Lock()

        #for ref in commits:
            #fetch_record(s, ref, out, lock)

        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
                # Start the load operations and mark each future with its reference
                future_to_ref = {executor.submit(fetch_record, s, ref, out, lock): ref for ref in commits}
                for future in concurrent.futures.as_completed(future_to_ref):
                        ref = future_to_ref[future]
                        try:
                            res = future.result()
                        except Exception as exc:
                            print('%s generated an exception: %s' % (ref, exc))

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

