run_analysis = True
use_synthetic_data = False
collect_invalid = False
owner_data_folder = "Input/OwnerData"
synthetic_data_folder = "TestData"

neo4j_clear_on_startup = True
neo4j_import = True
neo4j_run_louvain = False
neo4j_stream_to_gephi = False
neo4j_export_json = True
neo4j_export_json_pnames = ["Homebrew, d3"]


def get_import_path(owner):
    if use_synthetic_data:
        return "{0}/mongo_{1}_syn.json".format(synthetic_data_folder, owner)
    else:
        return "{0}/mongo_{1}.json".format(owner_data_folder, owner)
