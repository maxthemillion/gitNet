import pandas as pd
from neocontroller import Neo4jController
from scripts import conf
from threads import Thread


class Project:
    """Serves as data container for the communication data of a single project
    and splits the project communication data into threads."""

    _export_folder = "Export_Network/"

    def __init__(self, pullreq_data, issue_data, commit_data, owner, repo):

        self.owner = owner
        self.repo = repo

        self._pullreq_data = pullreq_data
        self._issue_data = issue_data
        self._commit_data = commit_data

        self._threads = None

        self._references = None

        self._participants = pd.DataFrame(pd.concat([self._pullreq_data["actor_id"],
                                                     self._issue_data["actor_id"],
                                                     self._commit_data["actor_id"]])
                                          .unique(),
                                          columns=["participants"])

        pullreq_actors = self._actor_login_id(pullreq_data)
        commit_actors = self._actor_login_id(commit_data)
        issue_actors = self._actor_login_id(issue_data)

        self._actor_dict = (pd.concat([pullreq_actors,
                                      commit_actors,
                                       issue_actors],
                                      axis="index"))\
            .set_index('actor_login')\
            .to_dict()\
            .get('actor_id')

        self.stats = ProjectStats(self)

    @staticmethod
    def _actor_login_id(comment_df):
        return pd.concat([comment_df["actor_id"], comment_df["actor_login"].str.lower()], axis="columns")

    def get_actor_id(self, actor_login):
        return self._actor_dict.get(actor_login)

    def run(self):
        self._threads = self._split_threads("pullreq") + \
                        self._split_threads("issue") + \
                        self._split_threads("commit")

        self.stats.add_participants(len(self._participants))

        self._references = self._collect_references()

        neo4j = Neo4jController()
        neo4j.import_references(self._references)

        self.stats.print_summary()

    # -------- is ------
    def is_participant(self, name):
        return name in self._participants

    # -------- threads -------
    def _split_threads(self, thread_type, start=None, stop=None):
        """splits the project data into single threads and passes them to new thread objects"""

        if thread_type == "issue":
            data = self._issue_data
        elif thread_type == "pullreq":
            data = self._pullreq_data
        elif thread_type == "commit":
            data = self._commit_data
        else:
            raise ValueError

        thread_ids = data["thread_id"].unique()

        if start is None:
            start = 0
        if stop is None:
            stop = len(thread_ids)

        thread_list = []
        i = start
        while i < stop:
            next_thread = thread_ids.item(i)
            thread_data = data[data["thread_id"] == next_thread]

            if thread_type in ["pullreq", "commit"]:

                positions = thread_data["comment_position"].unique()
                for pos in positions:
                    thread_position_data = thread_data[thread_data["comment_position"] == pos]
                    new_thread = Thread(thread_position_data, thread_type, self.stats, self)
                    new_thread.run()
                    thread_list.append(new_thread)

            else:
                new_thread = Thread(thread_data, thread_type, self.stats, self)
                new_thread.run()
                thread_list.append(new_thread)

            i = i + 1

        return thread_list

    def _collect_references(self):
        refs = []
        for thread in self._threads:
            ref_list = thread.get_references_as_list()
            if refs:
                refs.extend(ref_list)
            else:
                refs = ref_list

        ref_df = pd.DataFrame(refs)
        return ref_df


class ProjectStats:
    def __init__(self, parent_project):
        self._parent_project = parent_project

        self._no_threads = 0
        self._no_comments = 0
        self._no_participants = 0

        self._quotes = []
        self._mentions = []
        self._contextuals = []

        self._quotes_sourced = 0
        self._quotes_not_sourced = 0

        self._mentions_found_total = 0
        self._mentions_found_valid = 0

        self._contextuals_found_total = 0
        self._contextuals_found_valid = 0

        self._contextuals_total = {"issue": 0, "pullreq": 0, "commit": 0}
        self._mentions_total = {"issue": 0, "pullreq": 0, "commit": 0}
        self._quotes_total = {"issue": 0, "pullreq": 0, "commit": 0}

        self._contextuals_valid = {"issue": 0, "pullreq": 0, "commit": 0}
        self._mentions_valid = {"issue": 0, "pullreq": 0, "commit": 0}
        self._quotes_valid = {"issue": 0, "pullreq": 0, "commit": 0}

    def get_no_comments(self):
        return self._no_comments

    def get_no_threads(self):
        return self._no_threads

    def add_thread(self):
        self._no_threads += 1

    def add_comments(self, no_comments):
        self._no_comments += no_comments

    def add_participants(self, no_participants):
        self._no_participants += no_participants

    def add_quote(self, comment_id, sourced, thread_type):
        self._quotes.append([comment_id])
        if sourced:
            self._quotes_sourced += 1
        else:
            self._quotes_not_sourced += 1

        self._quotes_total[thread_type] += 1

    def add_mentions(self, comment_id, valid, thread_type):
        self._mentions.append([comment_id])
        self._mentions_found_total += 1
        if valid:
            self._mentions_found_valid += 1

        self._mentions_total[thread_type] += 1

    def add_contextual(self, comment_id, valid, thread_type):
        self._contextuals.append([comment_id])
        self._contextuals_found_total += 1
        if valid:
            self._contextuals_found_valid += 1

        self._contextuals_total[thread_type] += 1

    def export_summary(self):
        pass
        # TODO: print summary to file instead of to the console

    def print_summary(self):
        if not conf.output_verbose:
            return

        total_no_quotes = self._quotes_not_sourced + self._quotes_sourced
        if not total_no_quotes == 0:
            share_sourced = ((1.00 * self._quotes_sourced) /
                             (1.00 * total_no_quotes)) * 100
        else:
            share_sourced = 0

        if not self._contextuals_found_total == 0:
            share_contextuals_valid = ((1.00 * self._contextuals_found_valid) /
                                       (1.00 * self._contextuals_found_total)) * 100
        else:
            share_contextuals_valid = 0

        if not self._mentions_found_total == 0:
            share_mentions_valid = ((1.00 * self._mentions_found_valid) /
                                    (1.00 * self._mentions_found_total)) * 100
        else:
            share_mentions_valid = 0

        print("### Project Stats Summary ###")
        print()
        print("project name:                    {0}/{1}".format(self._parent_project.owner, self._parent_project.repo))
        print()
        print("no participants:          {0}".format(self._no_participants))
        print("no threads:               {0}".format(self._no_threads))
        print("no comments:              {0}".format(self._no_comments))
        print()

        print('                          total\tvalid\tshare')
        print("quotes:                   {0}\t\t{1}\t\t{2:.2f}%".format(
            total_no_quotes,
            self._quotes_sourced,
            share_sourced))
        print("mentions:                 {0}\t\t{1}\t\t{2:.2f}%".format(
            self._mentions_found_total,
            self._mentions_found_valid,
            share_mentions_valid))
        print("contextuals:              {0}\t\t{1}\t\t{2:.2f}%".format(
            self._contextuals_found_total,
            self._contextuals_found_valid,
            share_contextuals_valid))
        print()
        print("sum:                         \t\t{0}".format(self._quotes_sourced
                                                            + self._mentions_found_valid
                                                            + self._contextuals_found_valid))


