import pandas as pd
from Project_classes import Project
import warnings
from py2neo import Graph


def clean_input(data):
    """removes remaining artifacts originating in the MongoDB data structure"""
    if type(data["user"].iloc[1]) is dict:
        for index, row in data.iterrows():
            data.at[index, "user"] = row["user"].get('login')
    return data


def extract_owner_data(owner):
    # TODO: implement MongoDB data extractions
    # run .js scripts in batch job?
    # it's probably possible to run the commands and import the results directly without saving them in json files
    # MongoDB driver: https://docs.mongodb.com/ecosystem/drivers/python/

    warnings.warn("Direct import from MongoDB driver has not been implemented yet!")

    import_folder = "Input/"
    import_path = import_folder

    pullreq_data = pd.DataFrame(pd.read_json(import_path + owner + "_pullreq_data.json"))
    pullreq_data = clean_input(pullreq_data)

    issue_data = pd.DataFrame(pd.read_json(import_path + owner + "_issue_data.json"))
    issue_data = clean_input(issue_data)

    print("Imported pullreq and issue data from owner " + owner)

    # self._commit_data = pd.DataFrame(pd.read_json(import_path + "commit_data.csv"))
    # self._commit_data = self.clean_input(self._commit_data)

    return pullreq_data, issue_data


def run_louvain():
    print("Running louvain algorithm on Neo4j.")

    graph = Graph(user="max", password="1111")

    query_part = "CALL algo.louvain(" \
                 "'MATCH (u:USER) RETURN id(p) as id'," \
                 "'MATCH (u1:USER)-[rel:MENTIONS]-(u2:USER)" \
                 "RETURN id(u1) as source, id(u2) as target'," \
                 "{weightProperty:'weight', write: true, writeProperty:'community', graph:'cypher'})"
    graph.run(query_part)

    print("Louvain algorithm complete")
    print()


def stream_to_gephi():
    print("Running louvain algorithm on Neo4j.")

    graph = Graph(user="max", password="1111")

    query_part = "MATCH path = (:USER)-[:MENTIONS]-(:USER)" \
                 "CALL apoc.gephi.add(null, 'workspace1', path, 'weight', ['community'])" \
                 "YIELD nodes" \
                 "return *"
    graph.run(query_part)

    print("Louvain algorithm complete")
    print()

def build_network():
    owners_list = pd.read_csv("Input/owners.csv")

    for owner in owners_list["owner_name"]:

        owner_pullreq_data, owner_issue_data = extract_owner_data(owner)

        # TODO: clean up the following lines: Is there a better way to do it?
        project_name_list = owner_pullreq_data["repo"].drop_duplicates()
        project_name_list = project_name_list.append(owner_issue_data["repo"].drop_duplicates())
        # project_name_list = project_name_list.append(owner_commit_data["repo"].drop_duplicates())
        project_name_list = project_name_list.drop_duplicates()

        for project_name in project_name_list:
            project_pullreq_data = owner_pullreq_data[owner_pullreq_data["repo"] == project_name]
            project_issue_data = owner_issue_data[owner_issue_data["repo"] == project_name]
            new_project = Project(project_pullreq_data, project_issue_data)
            new_project.export_project()


# build_network()
run_louvain()
