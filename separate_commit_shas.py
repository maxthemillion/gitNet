import json
import pandas as pd

import_repos = pd.read_csv("Input/owners.csv", sep=',', header=0)
owners = import_repos["owners"].unique()

with open("./Input/commit_shas.json") as json_data:
    d = pd.DataFrame(json.load(json_data))

print("file opened")
#d = d.set_index(["login"])
#print("set index on login")
#u_owners = d["login"].unique()


for o in owners:

    o_d = d[d["login"] == o]
    r_unique = o_d["repo"].unique()
    for r in r_unique:
        r_d = o_d[o_d["repo"] == r]
        shas = r_d["sha"]

        with open("./Input/cc/cc_shas_{0}_{1}.csv"
                          .format(o.lower(), r.lower()), "w") as fp:
            shas.to_csv(fp, index=False)
