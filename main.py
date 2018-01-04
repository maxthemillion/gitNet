import pandas as pd
from project import Project
from neocontroller import Neo4jController
import warnings
import time

# TODO: restructure this code, make it less dependent


def main():
    neo_controller = Neo4jController()

    if False:
        neo_controller.clear_db()

    if True:
        run_analysis()

    if False:
        neo_controller.import_graph()

    if False:
        neo_controller.run_louvain()

    if False:
        neo_controller.stream_to_gephi()

    if False:
        neo_controller.export_graphjson()

    print("------------------------------------------")
    print("Total process time elapsed:        {0:.2f}s".format(time.process_time()))
    print("------------------------------------------")


def run_analysis():
    owners_list = pd.read_csv("Input/owners.csv")

    for owner in owners_list["owner_name"]:
        split_projects(owner)


def split_projects(owner):

    # TODO: implement support for commit data
    owner_pullreq_data, owner_issue_data = import_owner_data(owner)

    project_names = owner_pullreq_data["repo"]
    project_names = project_names.append(owner_issue_data["repo"])
    # project_names = project_names.append(owner_commit_data["repo"])

    project_names = project_names.drop_duplicates()

    for project_name in project_names:
        proc_time_start = time.process_time()
        project_pullreq_data = owner_pullreq_data[owner_pullreq_data["repo"] == project_name]
        project_issue_data = owner_issue_data[owner_issue_data["repo"] == project_name]

        Project(project_pullreq_data, project_issue_data).run()

        print("time required:                {0:.2f}s".format(time.process_time()-proc_time_start))
        print()

def import_owner_data(owner):
    # TODO: implement MongoDB data extractions
    # it's probably possible to run the commands and import the results directly without saving them in json files
    # MongoDB driver: https://docs.mongodb.com/ecosystem/drivers/python/

    warnings.warn("Direct import from MongoDB driver has not been implemented yet!")

    import_path = "Input/"

    pullreq_data = pd.DataFrame(pd.read_json(import_path + owner + "_pullreq_data.json"))
    issue_data = pd.DataFrame(pd.read_json(import_path + owner + "_issue_data.json"))

    # pullreq_data = pd.DataFrame(pd.read_json("TestData/synthetic_pullreq_data.json"))
    # issue_data = pd.DataFrame(pd.read_json("TestData/synthetic_issue_data.json"))

    print("Imported pullreq and issue data from owner " + owner)

    # self._commit_data = pd.DataFrame(pd.read_json(import_path + "commit_data.csv"))
    # self._commit_data = self.clean_input(self._commit_data)

    return pullreq_data, issue_data


if __name__ == '__main__':
    main()
