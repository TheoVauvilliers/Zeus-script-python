import os
import dask.dataframe as dd

"""
Create list of all files path from function parameter
param: {string} path - path to directory with files
return: {array} list of all files path
"""
def create_list_of_files(path: str) -> list:
    list_of_files = []

    for file in os.listdir(path):
        if file.endswith(".csv"):
            list_of_files.append(os.path.join(path, file))

    return list_of_files

"""
Read csv file and return dask dataframe
param: {string} file_path - path to csv file
return: {dd.DataFrame} dask dataframe
"""
def read_csv_file(file_path: str) -> dd.DataFrame:
    ddf = dd.read_csv(file_path)

    return ddf
