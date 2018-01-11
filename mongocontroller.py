# to run this script, establish a ssh connection with the following commands:
# ssh -L 27017:dutihr.st.ewi.tudelft.nl:27017 ghtorrent@dutihr.st.ewi.tudelft.nl
# then in a second terminal window start the mongo shell using
# mongo -u ghtorrentro -p ghtorrentro github

import pymongo
import pandas as pd
import json
import time
import conf


def main():
    mongo_c = pymongo.MongoClient()
    db = mongo_c.github

    coll_issue_c = db.issue_comments
    coll_pullreq_c = db.pull_request_comments
    coll_commit_c = db.commit_comments

    import_repos = pd.read_csv("Input/owners.csv", sep=',', header=0)
    owners = import_repos["owners"].unique()

    for owner in owners:
        time_start = time.time()

        repos = import_repos[import_repos["owners"] == owner]
        repos = repos["repo_names"]

        issue_comments = get_issue_comments(coll_issue_c, repos, owner)
        pullreq_comments = get_pullreq_comments(coll_pullreq_c, repos, owner)
        commit_comments = get_commit_comments(coll_commit_c, get_commit_shas(owner))

        d = {"ic": issue_comments,
             "pc": pullreq_comments,
             "cc": commit_comments}

        with open(conf.get_import_path(owner), "w") as fp:
            json.dump(d, fp, indent="\t")

        print("total time required:             {0:.2f}s".format(time.time() - time_start))
        print("issue comments retrieved:        {0}".format(len(issue_comments)))
        print("pullreq comments retrieved:      {0}".format(len(pullreq_comments)))
        print("commit comments retrieved:       {0}".format(len(commit_comments)))
        print()


def check_consistency(d):
    # TODO: do some consistency checks
    return d


def get_commit_shas(owner):
    # TODO: replace this foo with some proper method
    commit_shas = pd.DataFrame([{"sha": "8640da86801cee1fd5f6d1ad623c685a1f04cc74"},
                                {"sha": "8640da86801cee1fd5f6d1ad623c685a1f04cc74"},
                                {"sha": "0bf56a65edd7d277d49c41a2c4f7c8caa7e1ffd6"},
                                {"sha": "0bf56a65edd7d277d49c41a2c4f7c8caa7e1ffd6"},
                                {"sha": "0bf56a65edd7d277d49c41a2c4f7c8caa7e1ffd6"},
                                {"sha": "0bf56a65edd7d277d49c41a2c4f7c8caa7e1ffd6"},
                                {"sha": "539881f51a69b1f5cf169766d1115c8b7343bd09"},
                                {"sha": "539881f51a69b1f5cf169766d1115c8b7343bd09"},
                                {"sha": "539881f51a69b1f5cf169766d1115c8b7343bd09"},
                                {"sha": "cc22239c9999f4d56f487d4002b1bbe0a2dc0a94"},
                                {"sha": "8ecfab8a598be15e7aa66e334b0ff92700943d1d"},
                                {"sha": "8ecfab8a598be15e7aa66e334b0ff92700943d1d"},
                                {"sha": "8ecfab8a598be15e7aa66e334b0ff92700943d1d"},
                                {"sha": "8ecfab8a598be15e7aa66e334b0ff92700943d1d"},
                                {"sha": "7544a9afc7be23dbeed88f9a6c28da9b7beeaf80"},
                                {"sha": "5785f54f4bd48dca58a7550af48217d4c6372ad2"},
                                {"sha": "8b9ce59ce4b29d2ca761ebdc2acf4dcec3264eef"},
                                {"sha": "2b9a2833bc3c6bc8e7b7344e8178ce98e29ebe4b"},
                                {"sha": "2b9a2833bc3c6bc8e7b7344e8178ce98e29ebe4b"},
                                {"sha": "2b9a2833bc3c6bc8e7b7344e8178ce98e29ebe4b"},
                                {"sha": "2b9a2833bc3c6bc8e7b7344e8178ce98e29ebe4b"},
                                {"sha": "2ad3a87045246f89aa267251315d660f663c42f2"},
                                {"sha": "0532e1e06a35d8ab133f919a69865bddd760f34e"},
                                {"sha": "e9886cac6cd7cffbd39f812d02e9f4c5f308e470"},
                                {"sha": "e9886cac6cd7cffbd39f812d02e9f4c5f308e470"},
                                {"sha": "e9886cac6cd7cffbd39f812d02e9f4c5f308e470"},
                                {"sha": "406fdbb391e844ea604c3eac1075c3f2562d07a9"},
                                {"sha": "7c83d441150580a302b6420edd3a5497a6fa2eb3"},
                                {"sha": "c7c0ad6e612c03084ed367369a11e5322c1ae6ab"},
                                {"sha": "c7c0ad6e612c03084ed367369a11e5322c1ae6ab"},
                                {"sha": "c7c0ad6e612c03084ed367369a11e5322c1ae6ab"},
                                {"sha": "4a7cd160c31969b79979ae9bd20afcf82d9513c0"},
                                {"sha": "4a7cd160c31969b79979ae9bd20afcf82d9513c0"},
                                {"sha": "0c85113053a08c270a8068d4af2013f5758b3a21"},
                                {"sha": "0c85113053a08c270a8068d4af2013f5758b3a21"},
                                {"sha": "0c85113053a08c270a8068d4af2013f5758b3a21"},
                                {"sha": "0c85113053a08c270a8068d4af2013f5758b3a21"},
                                {"sha": "e1f0dec41e17edddc351e2ccd226077aa8f83bb0"},
                                {"sha": "e1f0dec41e17edddc351e2ccd226077aa8f83bb0"},
                                {"sha": "e1f0dec41e17edddc351e2ccd226077aa8f83bb0"},
                                {"sha": "e1f0dec41e17edddc351e2ccd226077aa8f83bb0"},
                                {"sha": "e1f0dec41e17edddc351e2ccd226077aa8f83bb0"},
                                {"sha": "e1f0dec41e17edddc351e2ccd226077aa8f83bb0"},
                                {"sha": "e1f0dec41e17edddc351e2ccd226077aa8f83bb0"},
                                {"sha": "e1f0dec41e17edddc351e2ccd226077aa8f83bb0"},
                                {"sha": "e5c5170bad77c8043b92b2f36a7849bae19946c2"},
                                {"sha": "c20622ade474a1c34a26872e6cc25dd592bc75a8"},
                                {"sha": "1451553188276117a574b2abe8957cb60ef7ced7"},
                                {"sha": "1451553188276117a574b2abe8957cb60ef7ced7"},
                                {"sha": "1451553188276117a574b2abe8957cb60ef7ced7"},
                                {"sha": "1451553188276117a574b2abe8957cb60ef7ced7"},
                                {"sha": "bc98fd37882c64c896dc2243fcc6e129f170a32a"},
                                {"sha": "bc98fd37882c64c896dc2243fcc6e129f170a32a"},
                                {"sha": "4333bce850fb065e36c62f8b1b2b412048887118"},
                                {"sha": "ddb576b582ddc801ac702566bacbc2f231fc86af"},
                                {"sha": "ddb576b582ddc801ac702566bacbc2f231fc86af"},
                                {"sha": "931e292bf12b8e05f6586ac7721255b35f04a389"},
                                {"sha": "931e292bf12b8e05f6586ac7721255b35f04a389"},
                                {"sha": "4059d5fc2634e24a0b5f9f02a416bda1a7016435"},
                                {"sha": "4059d5fc2634e24a0b5f9f02a416bda1a7016435"},
                                {"sha": "93e0f4f9465978460d411f27214e19ae3fc4d294"},
                                {"sha": "d0e15955a45935ff3393ebe619828fcddcf5863a"},
                                {"sha": "b33b1af073979c8a699ed9688dba37fb7e74f0b5"},
                                {"sha": "24f7e671317dfe22f1d8e10426db2e9074674bc9"},
                                {"sha": "24f7e671317dfe22f1d8e10426db2e9074674bc9"},
                                {"sha": "24f7e671317dfe22f1d8e10426db2e9074674bc9"},
                                {"sha": "cc752e97f6dcfb3e58c9e753262926672edeb571"},
                                {"sha": "cc752e97f6dcfb3e58c9e753262926672edeb571"},
                                {"sha": "0c6e307eef57c48d03e1213112d3edd597ce5a7b"},
                                {"sha": "0c6e307eef57c48d03e1213112d3edd597ce5a7b"},
                                {"sha": "bd4e24ae8f6db6ad6f0020352b0b8a15cb1c21e2"},
                                {"sha": "ccb11935f612847145ffe95b3b70f23e621fd4aa"},
                                {"sha": "ccb11935f612847145ffe95b3b70f23e621fd4aa"},
                                {"sha": "ccb11935f612847145ffe95b3b70f23e621fd4aa"},
                                {"sha": "2e747aa910ccfd645388a52517f285047a3276fe"},
                                {"sha": "60cbf5d2beecbbe0279c4180ec505c66ab6ca5e6"},
                                {"sha": "66cda616d1f2a2f5cb6bcece7be9ff29f5d6f863"},
                                {"sha": "66cda616d1f2a2f5cb6bcece7be9ff29f5d6f863"},
                                {"sha": "7c3f1dde59520c0efa3f989b96f6d22230a2b169"},
                                {"sha": "1c1c48c92005b27aba4e17e12b49c4d1cbb0b331"},
                                {"sha": "1c1c48c92005b27aba4e17e12b49c4d1cbb0b331"},
                                {"sha": "1c1c48c92005b27aba4e17e12b49c4d1cbb0b331"},
                                {"sha": "1c1c48c92005b27aba4e17e12b49c4d1cbb0b331"},
                                {"sha": "1c1c48c92005b27aba4e17e12b49c4d1cbb0b331"},
                                {"sha": "1c1c48c92005b27aba4e17e12b49c4d1cbb0b331"},
                                {"sha": "6a82bc49c764a68cf81ef96cae7ba9dbd26a702e"},
                                {"sha": "6a82bc49c764a68cf81ef96cae7ba9dbd26a702e"},
                                {"sha": "ebdb879fe4b0d14bcc92480a3dd193c93f94a23f"},
                                {"sha": "ebdb879fe4b0d14bcc92480a3dd193c93f94a23f"},
                                {"sha": "9b5c45a7df6a9a170389d6045bf06803fe4bc78b"},
                                {"sha": "9b5c45a7df6a9a170389d6045bf06803fe4bc78b"},
                                {"sha": "9b5c45a7df6a9a170389d6045bf06803fe4bc78b"},
                                {"sha": "9b5c45a7df6a9a170389d6045bf06803fe4bc78b"},
                                {"sha": "9b5c45a7df6a9a170389d6045bf06803fe4bc78b"},
                                {"sha": "a09799f25a602f3664ac8c6fcae97b20fc48a685"},
                                {"sha": "a212340cc1c7922b17c9ed35c01ea8bef05a9e9d"},
                                {"sha": "a212340cc1c7922b17c9ed35c01ea8bef05a9e9d"},
                                {"sha": "a212340cc1c7922b17c9ed35c01ea8bef05a9e9d"},
                                {"sha": "a212340cc1c7922b17c9ed35c01ea8bef05a9e9d"},
                                {"sha": "a212340cc1c7922b17c9ed35c01ea8bef05a9e9d"},
                                {"sha": "a212340cc1c7922b17c9ed35c01ea8bef05a9e9d"},
                                {"sha": "a212340cc1c7922b17c9ed35c01ea8bef05a9e9d"},
                                {"sha": "a212340cc1c7922b17c9ed35c01ea8bef05a9e9d"},
                                {"sha": "a212340cc1c7922b17c9ed35c01ea8bef05a9e9d"},
                                {"sha": "a212340cc1c7922b17c9ed35c01ea8bef05a9e9d"},
                                {"sha": "b05a596d570c8e5686fbb45331da8f101179f49a"},
                                {"sha": "6a8f22d3510fc92e2fb67c85bd4429d2aa218eea"},
                                {"sha": "bcf6c6e36d156ea96eb254605f292a99477199ff"},
                                {"sha": "bcf6c6e36d156ea96eb254605f292a99477199ff"},
                                {"sha": "12aad5c65fee39c5f044e39ca1efcbed58aebd39"},
                                {"sha": "12aad5c65fee39c5f044e39ca1efcbed58aebd39"},
                                {"sha": "12aad5c65fee39c5f044e39ca1efcbed58aebd39"},
                                {"sha": "12aad5c65fee39c5f044e39ca1efcbed58aebd39"},
                                {"sha": "12aad5c65fee39c5f044e39ca1efcbed58aebd39"},
                                {"sha": "12aad5c65fee39c5f044e39ca1efcbed58aebd39"},
                                {"sha": "12aad5c65fee39c5f044e39ca1efcbed58aebd39"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "080ddd8804be14f4b18f9558b58270456ff313c2"},
                                {"sha": "a97661a548c077e36956783f39c10746d000959c"},
                                {"sha": "a97661a548c077e36956783f39c10746d000959c"},
                                {"sha": "a97661a548c077e36956783f39c10746d000959c"},
                                {"sha": "310d7067e01952cdcefe8b2c877bc4c792654de2"},
                                {"sha": "ee82827e6d0e3eb3ddc6888035f5b62fb4d6a032"},
                                {"sha": "ee82827e6d0e3eb3ddc6888035f5b62fb4d6a032"},
                                {"sha": "ee82827e6d0e3eb3ddc6888035f5b62fb4d6a032"},
                                {"sha": "4ffe25adc8a3e71706a81e06f2b83aab37765dc2"},
                                {"sha": "4ffe25adc8a3e71706a81e06f2b83aab37765dc2"},
                                {"sha": "b81097cdeef710bc861bfb7be76b2b4d631d89a2"},
                                {"sha": "da34fba151ee33c1a2e14ab21ee0dc4ea451cc0f"},
                                {"sha": "da34fba151ee33c1a2e14ab21ee0dc4ea451cc0f"},
                                {"sha": "da34fba151ee33c1a2e14ab21ee0dc4ea451cc0f"},
                                {"sha": "684c44f356946e14d496040b5babf375c5d3ad08"},
                                {"sha": "684c44f356946e14d496040b5babf375c5d3ad08"},
                                {"sha": "684c44f356946e14d496040b5babf375c5d3ad08"},
                                {"sha": "684c44f356946e14d496040b5babf375c5d3ad08"},
                                {"sha": "2caf7b76bb3e09ffabf8c56d3934ba8190673fdc"},
                                {"sha": "2caf7b76bb3e09ffabf8c56d3934ba8190673fdc"},
                                {"sha": "2caf7b76bb3e09ffabf8c56d3934ba8190673fdc"},
                                {"sha": "2caf7b76bb3e09ffabf8c56d3934ba8190673fdc"},
                                {"sha": "2caf7b76bb3e09ffabf8c56d3934ba8190673fdc"},
                                {"sha": "2caf7b76bb3e09ffabf8c56d3934ba8190673fdc"},
                                {"sha": "2caf7b76bb3e09ffabf8c56d3934ba8190673fdc"},
                                {"sha": "0a33cc591d258e07f2279bc0440d62e38983fd67"},
                                {"sha": "0a33cc591d258e07f2279bc0440d62e38983fd67"},
                                {"sha": "0a33cc591d258e07f2279bc0440d62e38983fd67"},
                                {"sha": "0a33cc591d258e07f2279bc0440d62e38983fd67"},
                                {"sha": "181275c016adb6340553ebceb4042fea7e5c90e0"},
                                {"sha": "181275c016adb6340553ebceb4042fea7e5c90e0"},
                                {"sha": "2d1f2f35eccf366f56c9017ead002780e6dc0ad7"},
                                {"sha": "2d1f2f35eccf366f56c9017ead002780e6dc0ad7"},
                                {"sha": "2d1f2f35eccf366f56c9017ead002780e6dc0ad7"},
                                {"sha": "2d1f2f35eccf366f56c9017ead002780e6dc0ad7"},
                                {"sha": "2d1f2f35eccf366f56c9017ead002780e6dc0ad7"},
                                {"sha": "29d85578e75170a6c0eaebda4d701b46f1acf446"},
                                {"sha": "29d85578e75170a6c0eaebda4d701b46f1acf446"},
                                {"sha": "29d85578e75170a6c0eaebda4d701b46f1acf446"},
                                {"sha": "29d85578e75170a6c0eaebda4d701b46f1acf446"},
                                {"sha": "29d85578e75170a6c0eaebda4d701b46f1acf446"},
                                {"sha": "29d85578e75170a6c0eaebda4d701b46f1acf446"},
                                {"sha": "29d85578e75170a6c0eaebda4d701b46f1acf446"},
                                {"sha": "29d85578e75170a6c0eaebda4d701b46f1acf446"},
                                {"sha": "29d85578e75170a6c0eaebda4d701b46f1acf446"},
                                {"sha": "aa747b915a080633e626cd741fa8f74ae6c5c9d8"},
                                {"sha": "25baaa61acc8743e69032233823a5ac33858d49e"},
                                {"sha": "25baaa61acc8743e69032233823a5ac33858d49e"}], columns=["sha"])
    commit_shas = commit_shas["sha"].tolist()
    return commit_shas


def get_issue_comments(c, repos, owner):
    time_start = time.time()
    print("Reading in >>> {0}".format(owner))

    result = []
    for repo in repos:
        print("\trepo >>> {0}".format(repo))

        try:
            lst = list(c.find({"repo": repo, "owner": owner},
                              {"_id": 0,
                               "id": 1,
                               "user.login": 1,
                               "owner": 1,
                               "repo": 1,
                               "body": 1,
                               "created_at": 1,
                               "issue_id": 1
                               }))

            print("issue comments retrieved:           {0:.2f}s".format(time.time() - time_start))

        except pymongo.errors.OperationFailure:
            lst = []

            print("Operation interrupted. Write to log file")
            print("returning empty list")

            with open('log.txt', 'a') as logfile:
                logfile.write('failure when importing issue comments for >>> {0}'.format(owner))

        result.extend(lst)

    return result


def get_pullreq_comments(c, repos, owner):
    time_start = time.time()
    print("Reading in >>> {0}".format(owner))

    result = []
    for repo in repos:
        print("\trepo >>> {0}".format(repo))

        try:
            lst = list(c.find({"repo": repo, "owner": owner},
                              {"_id": 0,
                               "id": 1,
                               "user.login": 1,
                               "owner": 1,
                               "repo": 1,
                               "body": 1,
                               "created_at": 1,
                               "pullreq_id": 1,
                               "position": 1
                               }))

            print("pc retrieved:        {0:.2f}s".format(time.time() - time_start))

        except pymongo.errors.OperationFailure:
            lst = []

            print("Operation interrupted. Write to log file")
            print("returning empty list")

            with open('log.txt', 'a') as logfile:
                logfile.write('import failure (pc) >>> {0}/{1}'.format(owner, repo))

        result.extend(lst)

    return result


def get_commit_comments(c, commit_shas):
    time_start = time.time()
    print("Reading in commit_comments")

    try:
        lst = list(c.find({"commit_id": {"$in": commit_shas}},
                          {"_id": 0,
                           "id": 1,
                           "user.login": 1,
                           "body": 1,
                           "created_at": 1,
                           "commit_id": 1,
                           "repo": 1,
                           "position": 1
                           }))
        print("cc retrieved:        {0:.2f}s".format(time.time() - time_start))

    except pymongo.errors.OperationFailure:
        print("Operation interrupted. Write to log file")
        print("returning empty list")

        with open('log.txt', 'a') as logfile:
            logfile.write('import failure (cc)')

        lst = []

    return lst


if __name__ == '__main__':
    # cProfile.run("main()", sort="cumtime")
    main()

    print("------------------------------------------")
    print("Total process time elapsed:      {0:.2f}s".format(time.process_time()))
    print("------------------------------------------")
