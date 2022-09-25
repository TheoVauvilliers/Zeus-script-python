from time import time
from pprint import pprint
from helpers.file import create_list_of_files, read_csv_file
from helpers.db import get_database, get_collection, insert_row

if __name__ == "__main__":
    timer_start = time()

    db = get_database()
    collection = get_collection(db, "log")

    list_of_files = create_list_of_files("public/upload")
    number_rows_inserted = 0

    for file in list_of_files:
        pprint(f"Processing file: {file}")
        data = read_csv_file(file)

        for index, row in data.iterrows():
            insert_row(collection, row.array)
            number_rows_inserted += 1

    timer_end = time()

    pprint(f"Number of rows inserted: {number_rows_inserted}")
    pprint(f"End of script. Time elapsed: {round((timer_end - timer_start) / 60, 2)}min")
