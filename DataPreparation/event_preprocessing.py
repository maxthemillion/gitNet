"""
MODULE: event preprocessing

Takes a list of events as input and splits them in separate csv files (owner-wise)
"""

import cProfile
import pandas as pd
import time
import os.path
import warnings

def main():
    fp = '/Users/Max/Desktop/MA/Data/BigQuery/Extracts/201601/IssueCommentEvent.csv'

    warnings.warn("For final data preprocessing change order of column headers")


    print("processing: " + fp)
    start = time.time()

    counter = 0

    for chunk in pd.read_csv(fp,
                             chunksize=1000000,
                             header=None,
                             names=["event_id", "type", "repo_name", "repo_id", "actor_id", "actor_login",
                                    "org_id", "org_login", "event_time", "ght_repo_id", "ght_forked_from",
                                    "action", "other", "owner_name"]):

        if counter == 1:
            pass
            # break
        else:
            counter += 1

        lapstart = time.time()

        # convert to category
        chunk['owner_name'] = chunk['owner_name'].astype('category')

        # set the index to be this and don't drop
        chunk.set_index(keys=['owner_name'], drop=False, inplace=True, verify_integrity=False)

        # get a list of owner names
        names = chunk['owner_name'].unique().tolist()

        # now we can perform a lookup on a 'view' of the dataframe
        for name in names:
            temp = chunk.loc[chunk.owner_name == name]
            filename = name.replace('/', '-')
            exp = '/Users/Max/Desktop/MA/Data/BigQuery/Extracts/201601/IssueComments/' + filename
            file_exists = os.path.isfile(exp)

            temp.to_csv(exp, mode='a', header=(not file_exists))


        print("Chunk processed ({0:.2f}s)".format(time.time() - lapstart))

    print()
    print("-----------------------")
    print("Done!")
    print("Time required:  {0:.2f}s".format(time.time() - start))

if __name__ == '__main__':
    #cProfile.run("main()", sort="tottime")
    main()