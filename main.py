# takes list of owners
# for each owner in list_of_owners:
#   extract data from MongoDB
#   create projects & threads
#   export relational data to Neo4j
import pandas as pd
from Project_classes import Project
import warnings


def clean_input(data):
    """removes remaining artifacts originating in the MongoDB data structure"""
    if type(data["user"].iloc[1]) is dict:
        for index, row in data.iterrows():
            data.at[index, "user"] = row["user"].get('login')
    return data


def extract_owner_data(owner):
    # TODO: implement running MongoDB data extractions
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


owners_list = pd.read_csv("Input/owners.csv")

for owner in owners_list["owner_name"]:

    # TODO: clean up the following lines
    owner_pullreq_data, owner_issue_data = extract_owner_data(owner)
    project_name_list = owner_pullreq_data["repo"].drop_duplicates()
    project_name_list = project_name_list.append(owner_issue_data["repo"].drop_duplicates())
    project_name_list = project_name_list.drop_duplicates()

    for project_name in project_name_list:
        project_pullreq_data = owner_pullreq_data[owner_pullreq_data["repo"] == project_name]
        project_issue_data = owner_issue_data[owner_issue_data["repo"] == project_name]
        new_project = Project(project_pullreq_data, project_issue_data)
        new_project.export_network()
