import pandas as pd
from threads import Thread
import time
import conf
from neocontroller import Neo4jController


class Project:
    """Serves as data container for the communication data of a single project
    and splits the project communication data into threads."""

    _export_folder = "Export/"

    def __init__(self, pullreq_data, issue_data):

        # TODO: shift these asserts to unit tests
        assert len(pullreq_data["repo"].unique()) is 1
        assert len(issue_data["repo"].unique()) is 1
        assert issue_data["repo"].at[0] == pullreq_data["repo"].at[0]

        assert len(issue_data["owner"].unique()) is 1
        assert len(pullreq_data["repo"].unique()) is 1
        assert issue_data["owner"].at[0] == pullreq_data["owner"].at[0]

        self.owner = pullreq_data["owner"].at[0]
        self.repo = pullreq_data["repo"].at[0]

        self._pullreq_data = self._clean_input(pullreq_data)
        self._issue_data = self._clean_input(issue_data)
        # self._commit_data =

        self._threads = None

        self._references = None
        self._participants = pd.DataFrame(pd.concat([self._pullreq_data["user"].str.lower(),
                                       self._issue_data["user"].str.lower()]).unique(), columns=["participants"])

        self.stats = ProjectStats(self)

    def run(self):
        self._threads = self._split_threads("pullreq") + \
                        self._split_threads("issue")
        # self._split_threads("commit")

        self.stats.add_participants(len(self._participants))

        self._references = self._collect_references()

        if conf.neo4j_import:
            controller = Neo4jController()
            controller.import_project(self._references, self._participants, self.owner, self.repo)

        self.stats.print_summary()

    # -------- is ------
    def is_participant(self, name):
        return name in self._participants

    # -------- threads -------
    def _split_threads(self, thread_type, start=None, stop=None):
        """splits the project data into single threads and passes them to new thread objects"""
        print("start splitting {0} threads".format(thread_type))
        proc_time_start = time.process_time()

        if thread_type == "issue":
            data = self._issue_data
        elif thread_type == "pullreq":
            data = self._pullreq_data
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
            new_thread = Thread(thread_data, thread_type, self.stats, self)
            new_thread.run()
            if thread_list:
                thread_list.append(new_thread)
            else:
                thread_list = [new_thread]
            i = i + 1

        print("time required:       {0:.2f}s".format(time.process_time()-proc_time_start))
        print()

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

    # -------- data cleaning -------
    @staticmethod
    def _clean_input(data):
        """removes remaining artifacts originating in the MongoDB data structure"""
        if type(data["user"].iloc[1]) is dict:
            for index, row in data.iterrows():
                data.at[index, "user"] = row["user"].get('login')

        data["user"] = data["user"].str.lower()

        # TODO: do this upon data import!
        # TODO: pass the thread type when exporting
        column_names = data.columns
        if "pullreq_id" in column_names:
            data = data.rename(index=str, columns={"pullreq_id": "thread_id"})
        elif "issue_id" in column_names:
            data = data.rename(index=str, columns={"issue_id": "thread_id"})
        elif "commit_id" in column_names:
            data = data.rename(index=str, columns={"commit_id": "thread_id"})

        return data

    # -------- data export --------
    def _export_project(self):
        """exports references from all threads of the project."""

        print("start exporting project")
        proc_time_start = time.process_time()

        ref_df = self._references
        part_df = self._participants

        # export the references
        # this is also a prerequisite for the export to Neo4j
        # filename = "references_export.csv"
        # ref_df.to_csv(path_or_buf=self._export_folder + filename, sep=";", index=False, header=True)

        # export participants
        # filename = "participants_export.csv"
        # part_df["participants"].to_csv(self._export_folder + filename, sep=";", index=False, header=True)

        print("time required:       {0:.2f}s".format(time.process_time()-proc_time_start))
        print()

    def export_raw_data(self, filename=None):
        """exports the raw_data DataFrame for control purposes"""
        if filename is None:
            filename = "raw_data_export.csv"

        self._raw_data.to_csv(path_or_buf=self._export_folder + filename, sep=";")


class ProjectStats:
    def __init__(self, parent_project):
        self._parent_project = parent_project

        self._no_threads_analyzed = 0
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

    def add_thread(self):
        self._no_threads_analyzed += 1

    def add_comments(self, no_comments):
        self._no_comments += no_comments

    def add_participants(self, no_participants):
        self._no_participants += no_participants

    def add_quote(self, comment_id, sourced):
        self._quotes.append([comment_id])
        if sourced:
            self._quotes_sourced += 1
        else:
            self._quotes_not_sourced += 1

    def add_mentions(self, comment_id, valid):
        self._mentions.append([comment_id])
        self._mentions_found_total += 1
        if valid:
            self._mentions_found_valid += 1

    def add_contextual(self, comment_id, valid):
        self._contextuals.append([comment_id])
        self._contextuals_found_total += 1
        if valid:
            self._contextuals_found_valid += 1

    def export_summary(self):
        pass
        # TODO: print summary to file instead of to the console

    def print_summary(self):
        total_no_quotes = self._quotes_not_sourced + self._quotes_sourced
        if not total_no_quotes == 0:
            share_sourced = ((1.00 * self._quotes_sourced) /
                             (1.00 * total_no_quotes)) * 100
        else:
            share_sourced = -999

        if not self._contextuals_found_total == 0:
            share_contextuals_valid = ((1.00 * self._contextuals_found_valid) /
                                       (1.00 * self._contextuals_found_total)) * 100
        else:
            share_contextuals_valid = -999

        if not self._mentions_found_total == 0:
            share_mentions_valid = ((1.00 * self._mentions_found_valid) /
                                    (1.00 * self._mentions_found_total)) * 100
        else:
            share_mentions_valid = -999

        print("---------------------------------")
        print("Project Stats Summary")
        print("---------------------------------")
        print()
        print("project name:                    {0}/{1}".format(self._parent_project.owner, self._parent_project.repo))
        print()
        print("number of participants:          {0}".format(self._no_participants))
        print("number of threads analyzed:      {0}".format(self._no_threads_analyzed))
        print("number of comments:              {0}".format(self._no_comments))
        print()
        print("count sourced quotes:            {0}".format(self._quotes_sourced))
        print("share sourced quotes:            {0:.2f}%".format(share_sourced))
        print("count valid mentions:            {0}".format(self._mentions_found_valid))
        print("share valid mentions:            {0:.2f}%".format(share_mentions_valid))
        print("number valid contextuals:        {0}".format(self._contextuals_found_valid))
        print("share valid contextuals:         {0:.2f}%".format(share_contextuals_valid))
        print()
        print("total count valid references:    {0}".format(self._quotes_sourced
                                                             + self._mentions_found_valid
                                                             + self._contextuals_found_valid))

        print()

