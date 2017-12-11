# takes list of owners
# for each owner in list_of_owners:
#   extract data from MongoDB
#   create projects & threads
#   export relational data to Neo4j

from Project_classes import Project

# owners_list = pd.csv_import("owners_list.csv")

# for owner in owners_list:
#   data = extract_data(owner)
#   project_name_list = extract_projects(data)
#   for project_name in project_name_list:
#       project_data = get_project_data(project_name)
#       new_project = Project(project_data)
#       new_project.export_network()


def extract_project_names(data):
    project_names = []
    return project_names


def extract_data_from_mongo_db(owner):
    pass

def extract_project_data(project_name):
    pass

def


homebrew_brew = Project()
homebrew_brew.export_network("Neo4j")