"""
MODULE: event preprocessing

Takes a list of events as input and splits them in separate csv files (owner/repository-wise)
"""






import csv
import pandas as pd

chunk_list=[]
for chunk in pd.read_csv('/input/file.csv', chunksize=10000):
    chunk_list.append(chunk[chunk[   ] == 1]

final_df = pd.concat(chunk_list)