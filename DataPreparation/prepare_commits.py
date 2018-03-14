"""
This script filters commits from the commits.csv file which is included in the GHT MySQL dump.
The script selects commits that...
    - ... were made during the specified period ...
    - ... to all repositories under consideration

The file 'repo_ids.csv' holds GHT repository ids of those repositories which were sampled from the population of
relevant projects.

"""

import time

counter = 0
start = time.time()

in_filename = "/Volumes/MyBook Max/03_Studium/TUM/S5_WS1718/MA/Data/GHT/mysql-2018-02-01/commits.csv"
out_filename = "commits_filtered.csv"


min_year = 2014
max_year = 2017

min_month = 1
max_month = 8

proj_keys = set(line.strip() for line in open('repo_ids.csv'))

lap = time.time()

with open(in_filename) as in_f, open(out_filename, 'w') as out_f:
    for line in in_f:
        counter += 1

        if counter % 1000000 == 0:
            print(time.time()-lap)
            print(counter/1000000)
            lap = time.time()

        year_pos1 = len(line) - 21
        year_posend = year_pos1 + 4
        year = int(line[year_pos1:year_posend])

        if min_year <= year <= max_year:
            proj_posend = len(line) - 23
            proj_pos1 = str.rfind(line, ',', proj_posend - 15, proj_posend) + 1
            proj = line[proj_pos1:proj_posend]

            if proj in proj_keys:
                month_pos1 = year_posend + 1
                month_posend = month_pos1 + 2
                month = int(line[month_pos1:month_posend])

                if min_month <= month <= max_month:
                    out_f.write(line)

duration = time.time() - start
print(duration)

