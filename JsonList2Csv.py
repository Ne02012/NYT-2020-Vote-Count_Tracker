import csv
import json
import sys
import glob, os

# Get the CSV header row
def get_csv_header(js, jsObName):
    jsOb = js[jsObName]
    ret_list = []
    for list_elem in sorted(jsOb[0]):
        if isinstance(jsOb[0][list_elem], dict):
            for sublist_elem in sorted(jsOb[0][list_elem]):
                ret_list.append(f'{list_elem}.{sublist_elem}')
        else:
            ret_list.append(list_elem)
    return ret_list

# Get the CSV row
def get_csv_row(rowOb):
    ret_list = []
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

inFile = sys.argv[1]
jsObName = sys.argv[2]
outFile = sys.argv[3]

with open(inFile, encoding="utf8") as f:
    js = json.load(f)

# Make sure that jsObName is a list
if not isinstance(js[jsObName], list):
    sys.exit(f'{jsObName} is not a list object')

with open(outFile, 'w', newline='') as csvfile:
    wr = csv.writer(csvfile)
    wr.writerow(get_csv_header(js, jsObName))
    for rowOb in js[jsObName]:
        wr.writerow(get_csv_row(rowOb))
