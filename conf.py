import pandas as pd

# ---- network construction parameters ----
construct_network = True

# data sources
import_data_folder = "Data/Import_Network"
prep_data_folder = "Data/Export_DataPrep"
_relations_file = "Data/Relations/relations.csv"

# collectors
collect_invalid = False
collect_position_nan = False

# ---- neo4j parameters ----
# neo4j_import_references = construct_network
neo4j_import_references = True

# visualization export
neo4j_export_json = False
neo4j_export_json_pnames = [{"owner": "Homebrew", "repo": "brew"},
                            {"owner": "d3", "repo": "d3"}]

# ---- analysis parameters ---
nx_measures_path = "Export_Network/nx_measures"
plot_path = "Export_Network/plots"
plot_format = "eps"
a_gen_charts = False

# a_resolution = 7  # resolution in days
a_length_timeframe = 30  # length of time period to consider

a_louvain = True
a_modularity = True and a_louvain

a_degree_centrality = False or construct_network  #TODO find better solution: run, if analyzer is called as module
a_betweenness_centrality = True or construct_network
a_eigenvector_centrality = True or construct_network

# ---- output parameters ----
output_verbose = False


def get_ic_data_path(owner):
    return "{0}/IssueCommentEvent/{1}".format(prep_data_folder, owner)


def get_cc_data_path(owner):
    return "{0}/CommitCommentEvent/{1}".format(prep_data_folder, owner)


def get_pc_data_path(owner):
    return "{0}/PullRequestReviewCommentEvent/{1}".format(prep_data_folder, owner)


def get_relations_file_path():
    return _relations_file


def get_nx_path(owner, repo, i):
    return "{0}/nxm_{1}_{2}_{3}.csv".format(nx_measures_path, owner, repo, i)


def get_plot_path(owner, repo, i):
    return "{0}/nxm_{1}_{2}_{3}.{4}".format(plot_path, owner, repo, i, plot_format)
