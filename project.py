import pandas as pd
from py2neo import Graph, Path
from threads import Thread


class Project:
    """Serves as data container for the communication data of a single project
    and splits the project communication data into threads."""

    _export_folder = "Export/"
    _filename_lst = ['pullreq_data', 'issue_data', 'commit_data']

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

        self._pullreq_data = self.clean_input(pullreq_data)
        self._issue_data = self.clean_input(issue_data)
        # self._commit_data =       TODO: implement support for _commit_data

        self._threads = self._split_threads("pullreq")
        self._threads = self._threads + self._split_threads("issue")
        # self._threads.append(self._split_threads( "commit"))

        self.no_threads = len(self._threads)

        self._participants = self._collect_participants()
        self._references = None

        self.stats = ProjectStats(self)

    # -------- is ------
    def is_participant(self, name):
        return name in self._participants

    # -------- threads -------
    def analyze_threads(self):
        """starts the analysis process within threads. if more analysis should be performed
        besides the retrieval of references, add method calls in here."""
        for thread in self._threads:
            thread.analyze_references()
        self._references = self._collect_references()
        self._collect_stats()

    def _split_threads(self, thread_type, start=None, stop=None):
        """splits the project data into single threads and passes them to new thread objects"""
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

        i = start
        while i < stop:
            next_thread = thread_ids.item(i)
            thread_data = data[data["thread_id"] == next_thread]
            new_thread = Thread(thread_data, thread_type, self)

            if new_thread is not None:
                if 'thread_list' not in locals():
                    thread_list = [new_thread]
                else:
                    thread_list.append(new_thread)
            i = i + 1

        return thread_list

    def _collect_references(self, weighted=None):
        """returns references that were found in all threads of the project by the ThreadAnalyzer as a DataFrame.
        If parameter 'weighted' is set to False, one row of the df contains one reference found and
        additionally the comment_id is preserved."""

        if weighted is None:
            weighted = True
        assert type(weighted) is bool

        ref_df = pd.DataFrame()
        for thread in self._threads:
            ref_df = ref_df.append(thread.get_references_as_df())
        result = ref_df

        if weighted:
            # drop comment_id column remove duplicate rows and add weights
            # TODO: use formatter "{%s}" % ', '.join(str(x) instead of tuple(x)

            ref_df_weighted = ref_df.groupby(["commenter", "addressee", "ref_type"])\
                .size()\
                .reset_index()

            df_id_strings = ref_df.groupby(["commenter", "addressee", "ref_type"])["comment_id"]\
                .apply(lambda x: tuple(x))\
                .reset_index()

            ref_df_weighted = ref_df_weighted.merge(df_id_strings,
                                                    how="inner",
                                                    on=["commenter", "addressee", "ref_type"])

            ref_df_weighted.columns = ["commenter", "addressee", "ref_type", "weight", "comment_id"]
            result = ref_df_weighted

        print("collected references with weighted parameter set to \n >>>> " + str(weighted))
        print()

        return result

    def _collect_participants(self):
        part_df = pd.DataFrame()
        for thread in self._threads:
            participants = thread.get_participants()
            part_df = part_df.append(pd.DataFrame(participants, columns=["participants"]))
        part_df = part_df.drop_duplicates()

        return part_df

    # -------- data cleaning -------
    @staticmethod
    def clean_input(data):
        """removes remaining artifacts originating in the MongoDB data structure"""
        if type(data["user"].iloc[1]) is dict:
            for index, row in data.iterrows():
                data.at[index, "user"] = row["user"].get('login')

        data["user"] = data["user"].str.lower()

        # TODO: do this upon data import!
        # TODO: is the thread type even important?
        column_names = data.columns
        if "pullreq_id" in column_names:
            data = data.rename(index=str, columns={"pullreq_id": "thread_id"})
        elif "issue_id" in column_names:
            data = data.rename(index=str, columns={"issue_id": "thread_id"})
        elif "commit_id" in column_names:
            data = data.rename(index=str, columns={"commit_id": "thread_id"})

        return data

    # -------- data export --------
    def export_project(self, target_db):
        """exports references from all threads of the project."""
        assert target_db is "Neo4j" or "csv", "target_db option set incorrectly."

        print("begin export with parameter target_db set to \n >>>> " + target_db)
        print()

        if target_db is "csv" or "Neo4j":
            # csv export is prerequisite for importing the network to Neo4j
            self._save_project_to_csv()

        if target_db is "Neo4j":
            self._save_project_to_neo4j()

    def export_raw_data(self, filename=None):
        """exports the raw_data DataFrame for control purposes"""
        if filename is None:
            filename = "raw_data_export.csv"

        self._raw_data.to_csv(path_or_buf=self._export_folder + filename, sep=";")

    def _save_project_to_csv(self):
        ref_df = self._references
        part_df = self._participants

        # export the references
        # this is also a prerequisite for the export to Neo4j
        filename = "references_export.csv"
        ref_df.to_csv(path_or_buf=self._export_folder + filename, sep=";", index=False, header=True)
        print("All references of the project have been exported to: \n >>>> " + self._export_folder + filename)
        print()

        # export participants
        filename = "participants_export.csv"
        part_df["participants"].to_csv(self._export_folder + filename, sep=";", index=False, header=True)
        print("All participants of the project have been exported to: \n >>>> " + self._export_folder + filename)
        print()

    def _save_project_to_neo4j(self):
        # TODO: check if it makes sense to transfer the nodes directly to Neo4j
        # csv export-import could be replaced by something faster.

        graph = Graph(user="max", password="1111")

        # import the CSV files to Neo4j using Cypher
        import_path = \
            "'file:///Users/Max/Desktop/MA/Python/projects/NetworkConstructor/Export/participants_export.csv'"

        # create user nodes if these do not already exist
        # 'MERGE' matches new patterns to existing ones. If it doesn't exist, it creates a new one
        query_part = "LOAD CSV WITH HEADERS FROM " + import_path + \
                     "AS row FIELDTERMINATOR ';'" \
                     "MERGE (:USER{login:row.participants})"
        graph.run(query_part)

        import_path = \
            "'file:///Users/Max/Desktop/MA/Python/projects/NetworkConstructor/Export/references_export.csv'"
        query_ref = '''LOAD CSV WITH HEADERS FROM ''' + import_path + \
                    '''AS row FIELDTERMINATOR ';'
                    MERGE (a:USER{login:row.commenter})
                    MERGE (b:USER{login:row.addressee}) 
                    MERGE (a) -[:REFERS_TO {comment_id:row.comment_id, 
                    ref_type:row.ref_type, 
                    weight:row.weight}]-> (b)'''
        graph.run(query_ref)

        print("Export to Neo4j succeeded!")
        print()

    # ------- statistics ---------
    def _collect_stats(self):
        for thread in self._threads:
            self.stats.add_quotes_sourced(thread.report.get_quotes_sourced())
            self.stats.add_quotes_not_sourced(thread.report.get_quotes_not_sourced())
            self.stats.add_mentions_found_total(thread.report.get_mentions_found_total())
            self.stats.add_mentions_found_valid(thread.report.get_mentions_found_valid())
            self.stats.add_comments(thread.no_comments)


class ProjectStats:
    def __init__(self, parent_project):
        self._parent_project = parent_project
        self._quotes_sourced = 0
        self._quotes_not_sourced = 0
        self._mentions_found_total = 0 # TODO: also show the number of weighted edges drawn
        self._mentions_found_valid = 0
        self._no_threads_analyzed = parent_project.no_threads
        self._no_comments = 0
        self._no_participants = 0 # TODO: get the number of participants from the parent project

    def add_quotes_sourced(self, no):
        self._quotes_sourced += no

    def add_quotes_not_sourced(self, no):
        self._quotes_not_sourced += no

    def add_mentions_found_total(self, no):
        self._mentions_found_total += no

    def add_mentions_found_valid(self, no):
        self._mentions_found_valid += no

    def add_comments(self, no):
        self._no_comments += no

    def print_summary(self):
        total_no_quotes = self._quotes_not_sourced + self._quotes_sourced
        if not total_no_quotes == 0:
            share_sourced = (1.00 * self._quotes_sourced)/(1.00 * total_no_quotes)
        else:
            share_sourced = -999

        print("------ Project Stats Summary -----")
        print("project name:                " + self._parent_project.owner + "/" + self._parent_project.repo)
        print()
        print("number of threads analyzed: {0}".format(self._no_threads_analyzed))
        print("number of comments:         {0}".format(self._no_comments))
        print("total number of quotes:     {0} ".format(total_no_quotes))
        print("percentage quotes sourced:  {0}".format(share_sourced))
        print("number of mentions (total): {0}".format(self._mentions_found_total))
        print("number of valid mentions:   {0}".format(self._mentions_found_valid))
        print()
