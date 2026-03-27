import pandas as pd
from pandas.errors import EmptyDataError


class CSVIngestor:
    def __init__(self):

    def ingest_file(self, csv_path: str):
        #TODO: if csv file does not start with https:     / else
        try:
            data_df = pd.read_csv(csv_path)
            if data_df.empty:
                raise EmptyDataError(f"During ingestion, csv file at '{csv_path}' was empty.")

            columns = ', '.join(data_df.columns)
            values_list = []

        except FileNotFoundError:
            raise FileNotFoundError(f"File '{csv_path}' was not found.")
