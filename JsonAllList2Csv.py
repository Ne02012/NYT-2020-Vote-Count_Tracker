import csv
import json
import sys
import glob, os

CACHE_DIR = '_cache'

# Get the CSV header row
def get_csv_header(js, jsObName):
    jsOb = js[jsObName]
    ret_list = ['Precinct_MetaData_File']
    for list_elem in sorted(jsOb[0]):
        if isinstance(jsOb[0][list_elem], dict):
            for sublist_elem in sorted(jsOb[0][list_elem]):
                ret_list.append(f'{list_elem}.{sublist_elem}')
        else:
            ret_list.append(list_elem)
    return ret_list

# Get the CSV row
def get_csv_row(filename, rowOb):
    ret_list = [filename]
    for list_elem in sorted(rowOb):
        if isinstance(rowOb[list_elem], dict):
            for sublist_elem in sorted(rowOb[list_elem]):
                ret_list.append(rowOb[list_elem][sublist_elem])
        else:
            if None != rowOb[list_elem]:
                ret_list.append(rowOb[list_elem])
            else:
                ret_list.append('null')
    return ret_list

precinct_urllist_file = sys.argv[1]
jsObName = sys.argv[2]
outFile = sys.argv[3]

with open(precinct_urllist_file, encoding="utf8") as url_f:
                PRECINCT_METADATA_URLS = url_f.read().splitlines()

with open(outFile, 'w', newline='') as csvfile:
    wr = csv.writer(csvfile)
    csvHdrWritten = False    
    for url in PRECINCT_METADATA_URLS:
        filename = url.split('/')[-1].replace(':','-')
        cache_path = os.path.join(CACHE_DIR, 'precinct_metadata', filename)

        with open(cache_path, encoding="utf8") as f:
            js = json.load(f)

        # Make sure that jsObName is a list
        if not isinstance(js[jsObName], list):
            sys.exit(f'{jsObName} is not a list object')

        if not csvHdrWritten:    
            wr.writerow(get_csv_header(js, jsObName))
            csvHdrWritten = True
            
        for rowOb in js[jsObName]:
            wr.writerow(get_csv_row(filename, rowOb))
