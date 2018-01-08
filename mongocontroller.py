# to run this script, establish a ssh connection with the following commands:
# ssh -L 27017:dutihr.st.ewi.tudelft.nl:27017 ghtorrent@dutihr.st.ewi.tudelft.nl
# then in a second terminal window start the mongo shell using
# mongo -u ghtorrentro -p ghtorrentro github

from pymongo import MongoClient
import pprint as pp
import json
import time


def main():
    mongo_c = MongoClient()
    db = mongo_c.github

    coll_issue_c = db.issue_comments
    coll_pullreq_c = db.pull_request_comments
    # coll_commit_c = db.commit_comments TODO: add support or commit_comments

    collections = [coll_pullreq_c, coll_issue_c]
    owners = ["Homebrew", "d3"]

    for c in collections:
        cname = ""
        if c is coll_pullreq_c:
            cname = "pc"
        if c is coll_issue_c:
            cname = "ic"

        for owner in owners:  # TODO: catch eventual timeout error and log fails
            time_start = time.time()
            print("Reading in project {0}".format(owner))
            cursor = c.find({"owner": owner},
                                         {"_id": 0,
                                          "id": 1,
                                          "user.login": 1,
                                          "owner": 1,
                                          "repo": 1,
                                          "pullreq_id": 1,
                                          "body": 1,
                                          "created_at": 1})

            print("cursor created:                {0:.2f}s".format(time.time() - time_start))

            # TODO: is there a way to speed up iterating the cursor?
            new_list = []
            for doc in cursor:
                new_list.append(doc)

            print("list constructed:                {0:.2f}s".format(time.time() - time_start))

            with open("Export/mdb_exp_{0}_{1}.json".format(cname, owner), "w") as fp:
                json.dump(new_list, fp, indent="\t")

            print("total time required:             {0:.2f}s".format(time.time() - time_start))
            print("comments retrieved:              {0}".format(len(new_list)))
            print()


if __name__ == '__main__':
    # cProfile.run("main()", sort="cumtime")
    main()

    print("------------------------------------------")
    print("Total process time elapsed:      {0:.2f}s".format(time.process_time()))
    print("------------------------------------------")
