"""
Read records from in_filename and write records to out_filename if
the string up to the first comma between positions 11 and 16 of
line is found in the set keys.

"""

import datetime
import time

counter = 0
start = time.time()

in_filename = "/Volumes/MyBook Max/03_Studium/TUM/S5_WS1718/MA/Data/GHT/mysql-2018-02-01/commits.csv"
out_filename = "commits_filtered.csv"


min_year = 2015
max_year = 2018

proj_keys = set(line.strip() for line in open('repo_ids.csv'))
# proj_keys = set(['6227'])
# proj_keys.add('6227'.strip())

lap = time.time()

with open(in_filename) as in_f, open(out_filename, 'w') as out_f:
    for line in in_f:
        counter += 1

        if counter % 1000000 == 0:
            print(time.time()-lap)
            print (counter)
            lap = time.time()

        date_pos1 = len(line) - 21
        date_posend = date_pos1 + 4

        proj_posend = date_pos1 - 2
        proj_pos1 = str.rfind(line, ',', proj_posend-15, proj_posend) + 1

        proj = line[proj_pos1:proj_posend]

        if proj in proj_keys:
            year = int(line[date_pos1:date_posend])
            if min_year <= year <= max_year:
                out_f.write(line)

duration = time.time() - start
print(duration)

