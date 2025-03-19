# made by pinpin
from __future__ import annotations
from   typing   import Optional

import csv
import sys
import pandas 
import sqlite3
import logging

class Export:
    def __init__(self: Export, csv_file: Optional[str] = None, db_file: Optional[str] = None) -> None:
        """
        Export .csv files to load into SQLite:
            written by pinpin to load the NPD database for the autodoxx-api.
        """
        # check arg vars first
        if csv_file is None:
            self.csv_file = sys.argv[1]
        else:
            self.csv_file = csv_file

        if db_file is None:
            self.db_file = f"{self.csv_file.split('.')[0]}.db"
        else:
            self.db_file = db_file

        self.header: list[str] = []
        self.rows: list[list[Optional[str]]] = []

    def load_csv(self: Export) -> None:
        """
        Load & sort csv file!
        """
        # debugging
        logging.info("[*] starting to load CSV file: %s [*]", self.csv_file)
        # open csv file
        with open(self.csv_file, 'r', encoding='utf-8') as file:
            # iterate thru the file
            reader = csv.reader(file, delimiter="|")
            self.header = next(reader)
            expected_columns = len(self.header)
            row_count = 0
            # dbg
            logging.debug("[*] Header: %s [*]", self.header)
            # iterate thru the rows & sort columns
            for row in reader:
                row_count += 1
                # check row length
                if len(row) < expected_columns:
                    row = row + [None] * (expected_columns - len(row))
                    logging.debug("[*] padded row %d to %d columns [*]", row_count, expected_columns)

                elif len(row) > expected_columns:
                    row = row[:expected_columns]
                    logging.debug("[*] row %d truncated to %d columns [*]", row_count, expected_columns)
                # append to collection of rows
                self.rows.append(row)
                # check if rows were processed
                if row_count % 10000 == 0:
                    logging.info("[*] processed: %d rows [*]", row_count)
            # debugging
            logging.info("[*] finished loading CSV, total rows processed: %d [*]", row_count)

    def clean(self) -> pandas.DataFrame:
        # clean fuckt up data
        return pandas.DataFrame(self.rows, columns=self.header).apply(
            lambda col: col.map(lambda x: None if isinstance(x, str) and x.strip() == "" else x)
        )

    def export(self, df: pandas.DataFrame) -> None:
        """
        export csv!
        """
        # debugging
        logging.info("[*] exporting DataFrame to database: %s [*]", self.db_file)
        # get a connection
        conn = sqlite3.connect(self.db_file)
        # sort info properly
        df.to_sql("cases", conn, if_exists="replace", index=False)
        # close connection essentially
        conn.close()
        # dbgging
        logging.info("[*] export complete. [*]")

def main() -> None:
    """
    entry point/proof of concept:
    """
    # logging settings
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
    # instantiate
    converter = Export()
    # load csv
    converter.load_csv()
    # get a clean dataframe
    df = converter.clean()
    # export dataframe
    converter.export(df)
    # debugging
    logging.info("[*] CSV successfully exported to %s [*]", converter.db_file)

if __name__ == "__main__":
    main()