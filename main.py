from time import time
from pprint import pprint
from helpers.file import create_list_of_files, read_csv_file
from helpers.db import get_database, get_collection, insert_row

if __name__ == "__main__":
    timer_start = time()

    db = get_database()
    collection = get_collection(db, "log")

    list_of_files = create_list_of_files("public/upload")

    for file in list_of_files:
        pprint(f"Processing file: {file}")
        data = read_csv_file(file)

        for index, row in data.iterrows():
            insert_row(collection, row.array)

    timer_end = time()

    pprint(f"Time: {timer_end - timer_start}s")
