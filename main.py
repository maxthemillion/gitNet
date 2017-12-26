import pandas as pd
from project import Project
from neocontroller import Neo4jController
import warnings


def extract_owner_data(owner):
    # TODO: implement MongoDB data extractions
    # run .js scripts in batch job?
    # it's probably possible to run the commands and import the results directly without saving them in json files
    # MongoDB driver: https://docs.mongodb.com/ecosystem/drivers/python/

    warnings.warn("Direct import from MongoDB driver has not been implemented yet!")

    import_folder = "Input/"
    import_path = import_folder

    pullreq_data = pd.DataFrame(pd.read_json(import_path + owner + "_pullreq_data.json"))
    issue_data = pd.DataFrame(pd.read_json(import_path + owner + "_issue_data.json"))

    # pullreq_data = pd.DataFrame(pd.read_json("TestData/synthetic_data_pullreq.json"))
    # issue_data = pd.DataFrame(pd.read_json("TestData/synthetic_data_issue.json"))


    print("Imported pullreq and issue data from owner " + owner)

    # self._commit_data = pd.DataFrame(pd.read_json(import_path + "commit_data.csv"))
    # self._commit_data = self.clean_input(self._commit_data)

    return pullreq_data, issue_data


def split_projects(owner):

    owner_pullreq_data, owner_issue_data = extract_owner_data(owner)

    # TODO: clean up the following lines: Is there a better way to do it?
    project_name_list = owner_pullreq_data["repo"].drop_duplicates()
    project_name_list = project_name_list.append(owner_issue_data["repo"].drop_duplicates())
    # project_name_list = project_name_list.append(owner_commit_data["repo"].drop_duplicates())
    project_name_list = project_name_list.drop_duplicates()

    project_list = []
    for project_name in project_name_list:
        project_pullreq_data = owner_pullreq_data[owner_pullreq_data["repo"] == project_name]
        project_issue_data = owner_issue_data[owner_issue_data["repo"] == project_name]
        # project_issue_data = owner_issue_data.iloc[0]
        new_project = Project(project_pullreq_data, project_issue_data)
        # new_project.export_project()
        if project_list:
            project_list.append(new_project)
        else:
            project_list = [new_project]

    return project_list


def run_analysis():
    owners_list = pd.read_csv("Input/owners.csv")

    for owner in owners_list["owner_name"]:
        project_list = split_projects(owner)
        for project in project_list:
            project.analyze_threads()
            project.stats.print_summary()
            project.export_project("Neo4j")


analysis = True
neo4j = True

if analysis:
    run_analysis()
if neo4j:
    neo_controller = Neo4jController()
    neo_controller.run_louvain()
    # neo_controller.stream_to_gephi()
    neo_controller.export_graphjson()

