from metaflow import FlowSpec, step, IncludeFile

RAW_URL = "https://github.com/CapitalOneRecruiting/DS/raw/master/transactions.zip"
FILENAME = "transactions.csv"


class TransactionStatsFlow(FlowSpec):
    """
    A flow to generate some statistics about the transaction data.

    This flow performs the following steps:
    1) Download data from remote url
    2) ...
    """

    @step
    def start(self):
        """
        The start step:
        1) Loads the transaction data into dataframe.

        """
        import os
        import pandas as pd

        if not os.path.isfile(FILENAME):
            raw_df = pd.read_json(RAW_URL, lines=True)
            raw_df.to_csv(FILENAME, index=False)

        self.df = pd.read_csv(FILENAME)
        self.next(self.end)

    @step
    def end(self):
        """
        This step simply prints out the shape of the dataframe.

        """
        print(f" The data has {self.df.shape[0]} records (rows)" + \
        f" and {self.df.shape[1]} fields (columns).")


if __name__ == "__main__":
    TransactionStatsFlow()
