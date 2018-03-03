import pandas as pd
from scripts import conf

_invalid_references = []
_position_nan = []


def add_invalid_reference(ref):
    if conf.collect_invalid:
        _invalid_references.append(ref.get_info_as_dict())


def analyze_invalid_refs():

    if not conf.collect_invalid:
        return

    df = pd.DataFrame(_invalid_references)
    a_is_c = df[df["addressee"] == df["commenter"]]
    foo_owner_addressee = df[df["addressee"] == "fooOwner"]
    foo_owner_commenter = df[df["commenter"] == "fooOwner"]

    addressee_vals = df.groupby(by="addressee", axis=1,)

    print("-------------------------------")
    print("### invalid reference statistics ###")
    print()
    print("total count invalid ref:              {0}".format(len(df)))
    print("count ref with a == c:                {0}".format(len(a_is_c)))
    print("count ref with a == 'fooOwner':       {0}".format(len(foo_owner_addressee)))
    print("count ref with c == 'fooOwner':       {0}".format(len(foo_owner_commenter)))
    print()

    print("unique addressee values")
    print(addressee_vals)


def add_position_nan(nan_list):
    if conf.collect_position_nan:
        _position_nan.append(nan_list)


def analyze_position_nan():
    if not conf.collect_position_nan:
        return

    # TODO: implement printing some stats here
    # _position_nan.describe()

    print("-------------------------------")
    print("### nan statistics (position-field) ###")

    print()
    print("number of nan found:                 {0}".format(len(_position_nan)))
    print()
