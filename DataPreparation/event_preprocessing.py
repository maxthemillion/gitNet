"""
MODULE: event preprocessing

Takes a list of events as input and splits them in separate csv files (owner-wise)
"""

# import cProfile
import pandas as pd
from pandas.io.json import json_normalize
import json
import time
import os.path


def main():
    # preprocess()
    preprocess('CommitCommentEvent')

def preprocess(file=None):

    standard_files = ['IssuesEvent',
                      'CreateEvent',
                      'MemberEvent',
                      'PullRequestEvent',
                      'ReleaseEvent']

    comment_files = ['IssueCommentEvent',
                     'PullRequestReviewCommentEvent',
                     'CommitCommentEvent']

    if file is None:

        for file in standard_files:
            prep_standard(file)

        for file in comment_files:
            prep_comments(file)

    else:
        if file in standard_files:
            prep_standard(file)
        elif file in comment_files:
            prep_comments(file)
        else:
            raise ValueError("Provided filename not in file lists")



def prep_standard(file):
    """
    Takes a file, flattens the "other" column, adds a header and saves
    the processed file to csv.

    :return:
    """
    import_path = '/Users/Max/Desktop/MA/Data/BigQuery/Extracts/201601/' + file

    export_path = '/Users/Max/Desktop/MA/Data/BigQuery/Extracts/201601/Prepared/' + file + "_prep"
    chunksize = 100000

    # remove existing files
    file_exists = os.path.isfile(export_path)
    if file_exists:
        print("remove files")
        os.remove(export_path)

    print("processing: " + file)
    counter = 0
    for chunk in pd.read_csv(import_path,
                             chunksize=chunksize,
                             header=None,
                             names=["event_id", "type", "repo_name", "repo_id", "actor_id", "actor_login",
                                    "org_id", "org_login", "event_time", "ght_repo_id", "ght_forked_from",
                                    "action", "other", "owner_name"]):

        start = time.time()

        chunk = drop_cols(chunk)

        chunk = normalize_other(chunk, chunksize, counter)

        write_out_file(chunk, export_path)

        counter += 1
        print("Chunk processed {0} ({1:.2f}s)".format(counter, time.time() - start))


def prep_comments(file):
    """
    Takes a comment file and sorts each row to the owner of its repository
    in a new csv file

    :return:
    """

    import_path = '/Users/Max/Desktop/MA/Data/BigQuery/Extracts/201601/' + file
    export_dir = '/Users/Max/Desktop/MA/Data/BigQuery/Extracts/201601/Prepared/' + file + '/'
    chunksize = 1000000

    # remove existing files
    filelist = os.listdir(export_dir)
    for f in filelist:
        os.remove(os.path.join(export_dir, f))

    print("processing: " + import_path)
    start = time.time()

    counter = 0

    for chunk in pd.read_csv(import_path,
                             chunksize=chunksize,
                             header=None,
                             names=["event_id", "type", "repo_name", "repo_id", "actor_id", "actor_login",
                                    "org_id", "org_login", "event_time", "ght_repo_id", "ght_forked_from",
                                    "action", "other", "owner_name"]):

        lapstart = time.time()

        chunk = drop_cols(chunk)

        # convert to category
        chunk['owner_name'] = chunk['owner_name'].astype('category')

        # normalize the 'other' column
        norm = json_normalize(chunk['other'].apply(json.loads).tolist())
        norm.index += counter*chunksize

        chunk = chunk.join(norm)
        chunk = chunk.drop("other", axis=1)

        # set the index to be 'owner_name' and don't drop
        chunk.set_index(keys=['owner_name'], drop=False, inplace=True, verify_integrity=False)

        # get a list of owner names
        names = chunk['owner_name'].unique().tolist()

        # now we can perform a lookup on a 'view' of the dataframe
        for name in names:
            part = chunk.loc[chunk.owner_name == name]
            filename = name.replace('/', '-')
            exp = export_dir + filename

            write_out_file(part, exp)

        counter += 1

        print("Chunk processed ({0:.2f}s)".format(time.time() - lapstart))

    print()
    print("-----------------------")
    print("Done!")
    print("Time required:  {0:.2f}s".format(time.time() - start))


def normalize_other(chunk, chunksize, counter):

    if chunk['other'].dtype is str:
        start_index = chunksize * counter
        norm = json_normalize(chunk['other'].apply(json.loads).tolist())
        norm.index += start_index
        # chunk = chunk.join(norm)
        chunk = pd.concat([chunk, norm], axis=1)

    chunk = chunk.drop("other", axis=1)
    return chunk


def drop_cols(chunk):
    drop_list = ['type',
                 'action']
    for col in drop_list:
        chunk = chunk.drop(col, axis=1)

    return chunk


def write_out_file(chunk, exp_path):
    file_exists = os.path.isfile(exp_path)
    chunk.to_csv(exp_path, mode='a', index=False, header=(not file_exists))


if __name__ == '__main__':
    # cProfile.run("main()", sort="tottime")
    main()