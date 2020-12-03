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
        'Total_Votes',
        'Total_Absentee_Votes',
        'Trump_Votes',
        'Trump_Absentee_Votes',
        'Biden_Votes',
        'Biden_Absentee_Votes',
        'Jorgensen_Votes',
        'Jorgensen_Absentee_Votes',
        'Counties_Total_Votes',
        'Counties_Total_Absentee_Votes',
        'Counties_Total_Max_Absentee_Ballots',
        'Counties_Trump_Votes',
        'Counties_Trump_Absentee_Votes',
        'Counties_Biden_Votes',
        'Counties_Biden_Absentee_Votes',
        'Counties_Jorgensen_Votes',
        'Counties_Jorgensen_Absentee_Votes',
        'Precinct_Metadata_Filename',
        'Precinct_Meta_Counties_Total_Votes',
        'Precinct_Meta_Counties_Total_Electionday_Votes',
        'Precinct_Meta_Counties_Total_Absentee_Votes',
        'Precinct_Meta_Counties_Total_Provisional_Votes',
        'Precinct_Meta_Counties_uncounted_mail_ballots',
        'Precinct_Meta_Counties_Trump_Votes',
        'Precinct_Meta_Counties_Trump_Electionday_Votes',
        'Precinct_Meta_Counties_Trump_Absentee_Votes',
        'Precinct_Meta_Counties_Trump_Provisional_Votes',
        'Precinct_Meta_Counties_Biden_Votes',
        'Precinct_Meta_Counties_Biden_Electionday_Votes',
        'Precinct_Meta_Counties_Biden_Absentee_Votes',
        'Precinct_Meta_Counties_Biden_Provisional_Votes',
        'Precinct_Meta_Counties_Jorgensen_Votes',
        'Precinct_Meta_Counties_Jorgensen_Electionday_Votes',
        'Precinct_Meta_Counties_Jorgensen_Absentee_Votes',
        'Precinct_Meta_Counties_Jorgensen_Provisional_Votes',
        'Precincts_Total_Votes',
        'Precincts_Trump_Votes',
        'Precincts_Biden_Votes',
        'Precincts_Jorgensen_Votes',
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

def fetch_precinct_data(session, record, url):

        precinct_Meta_Counties_Total_Votes = 0
        precinct_Meta_Counties_Total_Electionday_Votes = 0
        precinct_Meta_Counties_Total_Absentee_Votes = 0
        precinct_Meta_Counties_Total_Provisional_Votes = 0
        precinct_Meta_Counties_uncounted_mail_ballots = 0
        precinct_Meta_Counties_Trump_Votes = 0
        precinct_Meta_Counties_Trump_Electionday_Votes = 0
        precinct_Meta_Counties_Trump_Absentee_Votes = 0
        precinct_Meta_Counties_Trump_Provisional_Votes = 0
        precinct_Meta_Counties_Biden_Votes = 0
        precinct_Meta_Counties_Biden_Electionday_Votes = 0
        precinct_Meta_Counties_Biden_Absentee_Votes = 0
        precinct_Meta_Counties_Biden_Provisional_Votes = 0
        precinct_Meta_Counties_Jorgensen_Votes = 0
        precinct_Meta_Counties_Jorgensen_Electionday_Votes = 0
        precinct_Meta_Counties_Jorgensen_Absentee_Votes = 0
        precinct_Meta_Counties_Jorgensen_Provisional_Votes = 0
        precincts_Total_Votes = 0
        precincts_Trump_Votes = 0
        precincts_Biden_Votes = 0
        precincts_Jorgensen_Votes = 0
        
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
                
        if "precinct_totals" in js:
                precinct_json_key = "precinct_totals"
                for county in js['county_totals']:
                    if 'uncounted_mail_ballots' in county:
                        precinct_Meta_Counties_uncounted_mail_ballots += int(county['uncounted_mail_ballots'])
                    # Florida precinct files show votes in county_totals
                    if 'FL'==filename[:2]:
                        precinct_Meta_Counties_Total_Votes += int(county['votes'])
                        precinct_Meta_Counties_Trump_Votes += int(county['results']['trumpd'])
                        precinct_Meta_Counties_Biden_Votes += int(county['results']['bidenj'])
                        if 'null'!=county['results']['jorgensenj']:
                            precinct_Meta_Counties_Jorgensen_Votes += int(county['results']['jorgensenj'])
                        
                for county in js['county_by_vote_type']:
                        if None==county['votes']:
                            continue
                        # Pennsylvania precinct files show votes in county_by_vote_type
                        if 'PA'==filename[:2]:
                            precinct_Meta_Counties_Total_Votes += int(county['votes'])
                            precinct_Meta_Counties_Trump_Votes += int(county['results']['trumpd'])
                            precinct_Meta_Counties_Biden_Votes += int(county['results']['bidenj'])
                            if None!=county['results']['jorgensenj']:
                                precinct_Meta_Counties_Jorgensen_Votes += int(county['results']['jorgensenj'])
                            
                        if 'electionday'==county['vote_type']:
                                precinct_Meta_Counties_Total_Electionday_Votes += int(county['votes'])
                                precinct_Meta_Counties_Trump_Electionday_Votes += int(county['results']['trumpd'])
                                precinct_Meta_Counties_Biden_Electionday_Votes += int(county['results']['bidenj'])
                                if None!=county['results']['jorgensenj']:
                                    precinct_Meta_Counties_Jorgensen_Electionday_Votes += int(county['results']['jorgensenj'])
                        if 'absentee'==county['vote_type']:
                                if None!=county['votes']:
                                    precinct_Meta_Counties_Total_Absentee_Votes += int(county['votes'])
                                if None!=county['results']['trumpd']:
                                    precinct_Meta_Counties_Trump_Absentee_Votes += int(county['results']['trumpd'])
                                if None!=county['results']['bidenj']:
                                    precinct_Meta_Counties_Biden_Absentee_Votes += int(county['results']['bidenj'])
                                if None!=county['results']['jorgensenj']:
                                    precinct_Meta_Counties_Jorgensen_Absentee_Votes += int(county['results']['jorgensenj'])
                        if 'provisional'==county['vote_type']:
                                if None!=county['votes']:
                                    precinct_Meta_Counties_Total_Provisional_Votes += int(county['votes'])
                                if None!=county['results']['trumpd']:
                                    precinct_Meta_Counties_Trump_Provisional_Votes += int(county['results']['trumpd'])
                                if None!=county['results']['bidenj']:
                                    precinct_Meta_Counties_Biden_Provisional_Votes += int(county['results']['bidenj'])
                                if None!=county['results']['jorgensenj']:
                                    precinct_Meta_Counties_Jorgensen_Provisional_Votes += int(county['results']['jorgensenj'])
        else:
                precinct_json_key = "precincts"

        for precinct in js[precinct_json_key]:
                if 'COUNTY'!=precinct['precinct_id']:
                        precincts_Total_Votes += int(precinct['votes'])
                        precincts_Trump_Votes += int(precinct['results']['trumpd'])
                        precincts_Biden_Votes += int(precinct['results']['bidenj'])
                        if 'jorgensenj' in precinct['results']:
                            precincts_Jorgensen_Votes += int(precinct['results']['jorgensenj'])
        return record._replace(
                            Precinct_Meta_Counties_Total_Votes = precinct_Meta_Counties_Total_Votes,
                            Precinct_Meta_Counties_Total_Electionday_Votes = precinct_Meta_Counties_Total_Electionday_Votes,
                            Precinct_Meta_Counties_Total_Absentee_Votes = precinct_Meta_Counties_Total_Absentee_Votes,
                            Precinct_Meta_Counties_Total_Provisional_Votes = precinct_Meta_Counties_Total_Provisional_Votes,
                            Precinct_Meta_Counties_uncounted_mail_ballots = precinct_Meta_Counties_uncounted_mail_ballots,
                            Precinct_Meta_Counties_Trump_Votes = precinct_Meta_Counties_Trump_Votes,
                            Precinct_Meta_Counties_Trump_Electionday_Votes = precinct_Meta_Counties_Trump_Electionday_Votes,
                            Precinct_Meta_Counties_Trump_Absentee_Votes = precinct_Meta_Counties_Trump_Absentee_Votes,
                            Precinct_Meta_Counties_Trump_Provisional_Votes = precinct_Meta_Counties_Trump_Provisional_Votes,
                            Precinct_Meta_Counties_Biden_Votes = precinct_Meta_Counties_Biden_Votes,
                            Precinct_Meta_Counties_Biden_Electionday_Votes = precinct_Meta_Counties_Biden_Electionday_Votes,
                            Precinct_Meta_Counties_Biden_Absentee_Votes = precinct_Meta_Counties_Biden_Absentee_Votes,
                            Precinct_Meta_Counties_Biden_Provisional_Votes = precinct_Meta_Counties_Biden_Provisional_Votes,
                            Precinct_Meta_Counties_Jorgensen_Votes = precinct_Meta_Counties_Jorgensen_Votes,
                            Precinct_Meta_Counties_Jorgensen_Electionday_Votes = precinct_Meta_Counties_Jorgensen_Electionday_Votes,
                            Precinct_Meta_Counties_Jorgensen_Absentee_Votes = precinct_Meta_Counties_Jorgensen_Absentee_Votes,
                            Precinct_Meta_Counties_Jorgensen_Provisional_Votes = precinct_Meta_Counties_Jorgensen_Provisional_Votes,
                            Precinct_Metadata_Filename = filename,
                            Precincts_Total_Votes = precincts_Total_Votes,
                            Precincts_Trump_Votes = precincts_Trump_Votes,
                            Precincts_Biden_Votes = precincts_Biden_Votes,
                            Precincts_Jorgensen_Votes = precincts_Jorgensen_Votes
                        )

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
                dictKey = f'{race["state_id"]}:{file_timestamp}'
                lock.acquire()

                if dictKey in recordFetched:
                        lock.release()
                        continue
                else:
                        recordFetched[dictKey] = True
                        lock.release()

                trump_stats = {}
                biden_stats = {}
                jorgensen_stats = {}
                
                for candidate_stats in race['candidates']:
                        if 'trumpd' == candidate_stats['candidate_key']:
                                trump_stats = candidate_stats
                        elif 'bidenj' == candidate_stats['candidate_key']:
                                biden_stats = candidate_stats
                        elif 'jorgensenj' == candidate_stats['candidate_key']:
                                jorgensen_stats = candidate_stats

                Counties_Total_Votes = 0
                Counties_Total_Absentee_Votes = 0
                Counties_Total_Max_Absentee_Ballots = 0
                Counties_Trump_Votes = 0
                Counties_Trump_Absentee_Votes = 0
                Counties_Biden_Votes = 0
                Counties_Biden_Absentee_Votes = 0
                Counties_Jorgensen_Votes = 0
                Counties_Jorgensen_Absentee_Votes = 0
                
                
                for county in race['counties']:
                        Counties_Total_Votes += int(county['votes'])
                        Counties_Total_Absentee_Votes += int(county['absentee_votes'])
                        if ('absentee_max_ballots' in county) and None!=county['absentee_max_ballots']: 
                            Counties_Total_Max_Absentee_Ballots += int(county['absentee_max_ballots'])
                        Counties_Trump_Votes += int(county['results']['trumpd'])
                        Counties_Trump_Absentee_Votes += int(county['results_absentee']['trumpd'])
                        Counties_Biden_Votes += int(county['results']['bidenj'])
                        Counties_Biden_Absentee_Votes += int(county['results_absentee']['bidenj'])
                        if 'jorgensenj' in county['results']:
                            Counties_Jorgensen_Votes += int(county['results']['jorgensenj'])
                            Counties_Jorgensen_Absentee_Votes += int(county['results_absentee']['jorgensenj'])
 
                record = InputRecord(
                            index,
                            file_timestamp,
                            f'{ref}.json',
                            timestamp,
                            race['votes'],
                            race['absentee_votes'],
                            trump_stats['votes'] if 'votes' in trump_stats else 0,
                            trump_stats['absentee_votes'] if 'absentee_votes' in trump_stats else 0,
                            biden_stats['votes'] if 'votes' in biden_stats else 0,
                            biden_stats['absentee_votes'] if 'absentee_votes' in biden_stats else 0,
                            jorgensen_stats['votes'] if 'votes' in jorgensen_stats else 0,
                            jorgensen_stats['absentee_votes'] if 'absentee_votes' in jorgensen_stats else 0,
                            Counties_Total_Votes,
                            Counties_Total_Absentee_Votes,
                            Counties_Total_Max_Absentee_Ballots,
                            Counties_Trump_Votes,
                            Counties_Trump_Absentee_Votes,
                            Counties_Biden_Votes,
                            Counties_Biden_Absentee_Votes,
                            Counties_Jorgensen_Votes,
                            Counties_Jorgensen_Absentee_Votes,
                            'NA',
                            *([0]*21)
                        )

                # Fetch precinct metadata, if available
                if 'precinct_metadata' in race:
                        record = fetch_precinct_data(session, record, race['precinct_metadata']['timestamped_url'])
                
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

def search_matching_record(timestamp, state_records, next_search_index):
        while timestamp >= min(state_records[next_search_index].file_timestamp, state_records[next_search_index].state_last_updated):
                next_search_index += 1
                if next_search_index == len(state_records):
                    break
                
        if next_search_index > 0:
                return next_search_index - 1
        else:
                return 0
        
def write_csv_align_timeseries(grouped):
        with open('VoteCounts.csv', 'w', newline='') as csvfile:
                wr = csv.writer(csvfile)
                wr.writerow(('state', 'timestamp', 'timeseries_votes', 'eevp', 'Trump Share', 'Biden Share',) + InputRecord._fields[2:])
                for state_index in sorted(grouped):
                        state_records = grouped[state_index]
                        ref = state_records[-1].Votes_Remaining_File[:-5]
                        cache_path = git_save(ref, 'results.json')
                        with open(cache_path, encoding="utf8") as f:
                            js = json.load(f)
                        race = js['data']['races'][state_index]
                        next_state_record_index = 0;
                        ts = race['timeseries']
                        for ts_index in range(len(ts)):
                                # Ignore any entry in timeseries which has anomalous higher timestamp than the following entry
                                if ts_index != (len(ts)-1) and ts[ts_index]['timestamp'] > ts[ts_index+1]['timestamp']:
                                    continue
                                xts = ts[ts_index]
                                next_state_record_index = search_matching_record(xts['timestamp'], state_records, next_state_record_index)
                                wr.writerow((race['state_name'], xts['timestamp'], xts['votes'], xts['eevp'], xts['vote_shares']['trumpd'], xts['vote_shares']['bidenj'],) + state_records[next_state_record_index][2:])

                        #Now write out any votes remaining page updates that were not captured in the timeseries
                        for state_record_index in range(next_state_record_index+1, len(state_records)):
                            wr.writerow((race['state_name'], xts['timestamp'], xts['votes'], xts['eevp'], xts['vote_shares']['trumpd'], xts['vote_shares']['bidenj'],) + state_records[state_record_index][2:])
        return                        
                        

grouped = fetch_all_records()
write_csv_align_timeseries(grouped)

