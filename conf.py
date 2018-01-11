import pandas as pd

run_analysis = True
use_synthetic_data = False
collect_invalid = True
collect_position_nan = True
owner_data_folder = "Input/OwnerData"
synthetic_data_folder = "TestData"

minDate = pd.Timestamp('2014-01-01 00:00:00.000', tz=None)
maxDate = pd.Timestamp('2017-01-01 00:00:00.000', tz=None)

neo4j_clear_on_startup = True
neo4j_import = True
neo4j_run_louvain = False
neo4j_stream_to_gephi = False
neo4j_export_json = False
neo4j_export_json_pnames = ["Homebrew, d3"]


def get_import_path(owner):
    if use_synthetic_data:
        return "{0}/mongo_{1}_syn.json".format(synthetic_data_folder, owner)
    else:
        return "{0}/mongo_{1}.json".format(owner_data_folder, owner)
