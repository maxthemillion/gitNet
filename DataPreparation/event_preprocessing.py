"""
MODULE: event preprocessing

Takes a list of events as input and splits them in separate csv files (owner/repository-wise)
"""
import cProfile
import pandas as pd
import time

def main():
    fp = '/Users/Max/Desktop/MA/Data/BigQuery/Extracts/20180219/IssueCommentEvent.csv'

    print("processing: " + fp)
    start = time.time()

    for chunk in pd.read_csv(fp,
                             chunksize=10000,
                             header=None,
                             names=["event_id", "type", "owner_name", "repo_name", "repo_id", "actor_id", "actor_login",
                                    "org_id", "org_login", "event_time", "ght_repo_id", "ght_forked_from",
                                    "action", "other"]):
        lapstart = time.time()

        # set the index to be this and don't drop
        chunk.set_index(keys=['repo_name'], drop=False, inplace=True)

        # get a list of owner names
        names = chunk['owner_name'].unique().tolist()

        # now we can perform a lookup on a 'view' of the dataframe
        for name in names:
            temp = chunk.loc[chunk.repo_name == name]
            filename = name.replace('/', '-')
            exp = '/Users/Max/Desktop/MA/Data/BigQuery/Extracts/201601/IssueComments/' + filename

            temp.to_csv(exp, mode='a', header=False)

        print("Chunk processed ({0:.2f}s)".format(time.time() - lapstart))

    print()
    print("-----------------------")
    print("Done!")
    print("Time required:  {0:.2f}s".format(time.time() - start))

if __name__ == '__main__':
    cProfile.run("main()", sort="cumtime")
    #main()