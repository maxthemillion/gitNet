import pandas as pd

# ---- network construction parameters ----
construct_network = True

# data source
use_synthetic_data = False
owner_data_folder = "Input/OwnerData"
synthetic_data_folder = "TestData"

# collectors
collect_invalid = False
collect_position_nan = False

# filter
minDate = pd.Timestamp('2014-01-01 00:00:00.000', tz=None)
maxDate = pd.Timestamp('2017-01-01 00:00:00.000', tz=None)

# ---- neo4j parameters ----
neo4j_import = construct_network
neo4j_clear_on_startup = construct_network

neo4j_stream_to_gephi = False

# visualization export
neo4j_export_json = False
neo4j_export_json_pnames = [{"owner": "Homebrew", "repo": "brew"},
                            {"owner": "d3", "repo": "d3"}]

# ---- analysis parameters ---
nx_measures_path = "Export/nx_measures"
plot_path = "Export/plots"
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


def get_data_path(owner):
    if use_synthetic_data:
        return "{0}/mongo_{1}_syn.json".format(synthetic_data_folder, owner)
    else:
        return "{0}/mongo_{1}.json".format(owner_data_folder, owner)

def get_nx_path(owner, repo, i):
    return "{0}/nxm_{1}_{2}_{3}.csv".format(nx_measures_path, owner, repo, i)

def get_plot_path(owner, repo, i):
    return "{0}/nxm_{1}_{2}_{3}.{4}".format(plot_path, owner, repo, i, plot_format)
