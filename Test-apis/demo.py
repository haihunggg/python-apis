from pprint import pprint

import pandas as pd

df = pd.read_csv("data.csv")

ans = df.to_dict(orient="index")
pprint(list(ans.values()))
