import pandas as pd
import warnings
from py2neo import Graph, Path
from Analyzer_classes import ThreadAnalyzer as ta


class Project:
    """Serves as data container for the communication data of a single project,
    handles the data import, cleaning and splits the project communication data into
    threads."""
    _export_folder = "Export/"
    _filename_lst = ['pullreq_data', 'issue_data', 'commit_data']

    def __init__(self, pullreq_data, issue_data):

        assert len(pullreq_data["repo"].unique()) is 1
        assert len(issue_data["repo"].unique()) is 1
        assert issue_data["repo"].at[0] == pullreq_data["repo"].at[0]

        assert len(issue_data["owner"].unique()) is 1
        assert len(pullreq_data["repo"].unique()) is 1
        assert issue_data["owner"].at[0] == pullreq_data["owner"].at[0]

        self.owner = pullreq_data["owner"].at[0]
        self.repo = pullreq_data["repo"].at[0]

        self._pullreq_data = pullreq_data
        self._issue_data = issue_data
        # self._commit_data =       TODO: implement support for _commit_data

        self._threads = self._split_threads(self._pullreq_data, "pullreq")
        self._threads = self._threads + self._split_threads(self._issue_data, "issue")
        # self._threads.append(self._split_threads(self._commit_data, "commit"))

    def collect_references(self, weighted=None):
        """returns references that were found in all threads of the project by the ThreadAnalyzer as a DataFrame.
        If parameter 'weighted' is set to False, one row of the df contains one reference found and
        additionally the comment_id is preserved."""

        if weighted is None:
            weighted = False
        assert type(weighted) is bool

        ref_df = pd.DataFrame()
        for thread in self._threads:
            ref_df = ref_df.append(thread.get_references())

        if weighted:
            # drop comment_id column remove duplicate rows and add weights
            ref_df.drop("comment_id", axis="columns")
            ref_df = ref_df.groupby(["commenter", "addressee", "ref_type"]).size().reset_index()

        print("collected references with weighted parameter set to \n >>>> " + str(weighted))
        print()

        return ref_df

    def collect_participants(self):
        part_df = pd.DataFrame()
        for thread in self._threads:
            participants = thread.get_participants()
            part_df = part_df.append(pd.DataFrame(participants, columns=["participants"]))
        part_df = part_df.drop_duplicates()

        return part_df

    def _split_threads(self, data, thread_type, start=None, stop=None):
        """splits the project data into single threads and passes them to new thread objects"""

        column_names = data.columns
        if thread_type is "pullreq":
            data = data.rename(index=str, columns={"pullreq_id": "thread_id"})
        elif thread_type is "issue":
            data = data.rename(index=str, columns={"issue_id": "thread_id"})
        elif thread_type is "commit":
            data = data.rename(index=str, columns={"commit_id": "thread_id"})
        else:
            raise ValueError("wrong thread type passed to self.split_threads()")

        thread_ids = data["thread_id"].unique()

        if start is None:
            start = 0
        if stop is None:
            stop = len(thread_ids)

        i = start

        while i < stop:
            next_thread = thread_ids.item(i)
            thread_data = data[data["thread_id"] == next_thread]
            new_thread = Thread(thread_data, thread_type)

            if new_thread is not None:
                if 'thread_list' not in locals():
                    thread_list = [new_thread]
                else:
                    thread_list.append(new_thread)
            i = i + 1

        return thread_list

    def export_raw_data(self, filename=None):
        """exports the raw_data DataFrame for control purposes"""
        if filename is None:
            filename = "raw_data_export.csv"

        self._raw_data.to_csv(path_or_buf=self._export_folder + filename, sep=";")

    def export_network(self, target_db=None):
        """exports references from all threads of the project. Defaults to csv export"""
        if target_db is None:
            target_db = "Neo4j"

        assert target_db is "Neo4j" or "SQLite" or "csv", "target_db option set incorrectly."
        print("begin export with parameter target_db set to \n >>>> " + target_db)
        print()

        # get all references
        ref_df = self.collect_references()
        part_df = self.collect_participants()

        if target_db is "csv" or "Neo4j":
            # export the references
            # this is also a prerequisite for the export to Neo4j
            filename = "references_export.csv"
            ref_df.to_csv(path_or_buf=self._export_folder + filename, sep=";", index=False, header=True)
            print("All references of the project have been exported to: \n >>>> " + self._export_folder + filename)
            print()

            # export participants
            filename = "participants_export.csv"
            part_df["participants"].to_csv(self._export_folder + filename, sep =";", index=False, header=True)
            print("All participants of the project have been exported to: \n >>>> " + self._export_folder + filename)
            print()

        if target_db is "Neo4j":
            # TODO: check if it makes sense to transfer the nodes directly to
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
                        MERGE (a) -[:MENTIONS {comment_id:row.comment_id}]-> (b)'''
            graph.run(query_ref)

            print("Export to Neo4j succeeded!")
            print()

        elif target_db is "SQLite":
            # TODO: implement SQLite export option
            warnings.warn("SQLite export option has not been implemented, yet.")


class Thread:
    """contains a pull request communication thread and its thread analytics"""
    # TODO: change this class so that it can handle all types of threads
    def __init__(self, thread_data, thread_type):

        self._thread_data = thread_data.sort_values(by="id", axis="rows", ascending=True)
        self._type = thread_type
        self._references, self._participants = ta.run_analysis(self._thread_data)

        # assert that only one pull request id appears in each thread
        assert len(thread_data["thread_id"].unique()) is 1

    def get_references(self):
        return self._references

    def get_participants(self):
        return self._participants




