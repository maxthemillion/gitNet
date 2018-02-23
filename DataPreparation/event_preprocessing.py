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

    start = time.time()
    preprocess()

    print()
    print("-----------------------")
    print("Done!")
    print("Time required:  {0:.2f}s".format(time.time() - start))


def preprocess(file=None):

    standard_files = ['IssuesEvent',
                      'CreateEvent',
                      'MemberEvent',
                      'PullRequestEvent',
                      'ReleaseEvent',
                      'IssueCommentEvent',
                      'PullRequestReviewCommentEvent',
                      'CommitCommentEvent'
                      ]

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

        if file in comment_files:
            prep_comments(file)


def prep_standard(file):
    """
    Takes a file, flattens the "other" column, adds a header and saves
    the processed file to csv.

    :return:
    """
    import_path = '../Import_DataPrep/' + file

    export_path = '../Export_DataPrep/' + file + "_prep"
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
                             names=["event_id", "type", "owner_name", "repo_name", "repo_id", "actor_id", "actor_login",
                                    "org_id", "org_login", "event_time", "ght_repo_id", "ght_forked_from",
                                    "action", "other"]):

        start = time.time()

        chunk = drop_cols(chunk)

        chunk = normalize_other(chunk, chunksize, counter)

        if 'comment_body' in chunk.columns:
            chunk = chunk.drop('comment_body', axis=1)

        write_out_file(chunk, export_path)

        counter += 1
        print("Chunk processed {0} ({1:.2f}s)".format(counter, time.time() - start))

    print()


def prep_comments(file):
    """
    Takes a comment file and sorts each row to the owner of its repository
    in a new csv file

    :return:
    """

    import_path = '../Import_DataPrep/' + file
    export_dir = '../Export_DataPrep/' + file + '/'
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
                             names=["event_id", "type", "owner_name", "repo_name", "repo_id", "actor_id", "actor_login",
                                    "org_id", "org_login", "event_time", "ght_repo_id", "ght_forked_from",
                                    "action", "other"]):

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


def normalize_other(chunk, chunksize, counter):

    try:
        start_index = chunksize * counter
        norm = json_normalize(chunk['other'].apply(json.loads).tolist())
        norm.index += start_index

        # chunk = chunk.join(norm)
        chunk = pd.concat([chunk, norm], axis=1)
    except TypeError:
        print("skipping 'other' column!")

    finally:
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
