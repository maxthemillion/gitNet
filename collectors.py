import pandas as pd

invalidReferences = []


def add_invalid_reference(ref):
        invalidReferences.append(ref.get_info_as_dict())


def analyze():
    df = pd.DataFrame(invalidReferences)
    a_is_c = df[df["addressee"] == df["commenter"]]
    foo_owner_addressee = df[df["addressee"] == "fooOwner"]
    foo_owner_commenter = df[df["commenter"] == "fooOwner"]

    addressee_vals = df.groupby(by="addressee", axis=1,)

    print("-------------------------------")
    print("invalid reference statistics")
    print()
    print("total count invalid ref:              {0}".format(len(df)))
    print("count ref with a == c:                {0}".format(len(a_is_c)))
    print("count ref with a == 'fooOwner':       {0}".format(len(foo_owner_addressee)))
    print("count ref with c == 'fooOwner':       {0}".format(len(foo_owner_commenter)))
    print()

    print("unique addressee values")
    print(addressee_vals)