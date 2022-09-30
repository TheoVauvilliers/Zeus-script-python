from time import time
from pprint import pprint
from helpers.file import create_list_of_files, read_csv_file
from helpers.db import get_database, init_and_get_collection, bulk_execute

BULK_SIZE = 1000
LOG_ROWS_NUMBER = 100000

if __name__ == "__main__":
    timer_global_start = time()

    db = get_database()
    collection = init_and_get_collection(db, "log")

    list_of_files = create_list_of_files("public/upload")

    number_rows_inserted = 0
    iteration_number = 1
    timer_start = time()

    for file in list_of_files:
        pprint(f"Processing file: {file}")
        data = read_csv_file(file)
        rows = []

        for index, row in data.iterrows():
            rows.append(row.array)

            # every BULK_SIZE rows, insert them into the database
            if iteration_number % BULK_SIZE == 0:
                bulk_execute(collection, rows)
                rows = []
                iteration_number = 1

            # every LOG_ROWS_NUMBER rows, print the number of rows inserted
            if number_rows_inserted % LOG_ROWS_NUMBER == 0:
                pprint("--- --- --- ---")
                pprint(f"Number of rows inserted: {round(number_rows_inserted / 1000)}K")
                pprint(f"Time elapsed: {round(time() - timer_start, 2)}s")
                timer_start = time()

            number_rows_inserted += 1
            iteration_number += 1
    
    if iteration_number != 1:
        bulk_execute(collection, rows)

    pprint(f"Number of rows inserted: {number_rows_inserted}")
    pprint(f"End of script. Time elapsed: {round((time() - timer_global_start) / 60, 2)}min")
