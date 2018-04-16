import pandas as pd

# ---- network construction parameters ----
construct_network = True

# data sources
_import_data_folder = "Data/Import_Network"
_prep_data_folder = "Data/Export_DataPrep"
_relations_file = "Data/Relations/relations.csv"

# export folders
_viz_data_folder = "Data/Export_Network/viz_data"
_plot_path = "Data/Export_Network/plots"
_nx_measures_path = "Data/Export_Network/nx_measures"

# collectors
collect_invalid = False
collect_position_nan = False

# ---- neo4j parameters ----


# ---- analysis parameters ---
plot_format = "eps"
a_gen_charts = False

# a_resolution = 7  # resolution in days
a_length_timeframe = 30  # length of time period to consider

a_dev_core_min_contributions = 20
a_filter_core = True

a_louvain = True
a_modularity = True and a_louvain

a_degree_centrality = False or construct_network
a_betweenness_centrality = True or construct_network
a_eigenvector_centrality = True or construct_network

# ---- output parameters ----
output_verbose = False


def get_ic_data_path(owner):
    return "{0}/IssueCommentEvent/{1}".format(_prep_data_folder, owner)


def get_cc_data_path(owner):
    return "{0}/CommitCommentEvent/{1}".format(_prep_data_folder, owner)


def get_pc_data_path(owner):
    return "{0}/PullRequestReviewCommentEvent/{1}".format(_prep_data_folder, owner)


def get_relations_file_path():
    return _relations_file


def get_nx_path(owner, i, repo=None):
    if repo is None:
        return "{0}/nxm_{1}_{2}.csv".format(_nx_measures_path, owner, i)
    else:
        return "{0}/nxm_{1}_{2}_{3}.csv".format(_nx_measures_path, owner, repo, i)


def get_plot_path(owner, i, repo=None):
    if repo is None:
        return "{0}/nxm_{1}_{2}.{3}".format(_plot_path, owner, i, plot_format)
    else:
        return "{0}/nxm_{1}_{2}_{3}.{4}".format(_plot_path, owner, repo, i, plot_format)


def get_viz_data_path(owner, repo=None):
    if repo is None:
        return "{0}/viz_{1}.json".format(_viz_data_folder, owner)
    else:
        return "{0}/viz_{1}_{2}.json".format(_viz_data_folder, owner, repo)
