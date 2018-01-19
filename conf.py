import pandas as pd

# ---- network construction parameters ----
construct_network = False

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
neo4j_export_json = True
neo4j_export_json_pnames = [{"owner": "Homebrew", "repo": "brew"},
                            {"owner": "d3", "repo": "d3"}]

# ---- analysis parameters ---

# a_resolution = 7  # resolution in days
a_length_timeframe = 30  # length of time period to consider

a_louvain = True
a_degree_centrality = True
a_betweenness_centrality = True

# ---- output parameters ----
output_verbose = False


def get_import_path(owner):
    if use_synthetic_data:
        return "{0}/mongo_{1}_syn.json".format(synthetic_data_folder, owner)
    else:
        return "{0}/mongo_{1}.json".format(owner_data_folder, owner)
