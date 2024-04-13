from metaflow import Flow, FlowSpec, step


class TransactionDeduplicationFlow(FlowSpec):
    """
    A flow to identify duplicate (multi-swipe/reversal) transactions.

    The flow performs the following steps:
        1. Find duplicate transactions within short time intervals
        2. Find transaction reversals.
    """

    @step
    def start(self):
        """
        The start step:
        1. Loads the data into dataframe
        2. Calculate time difference (in seconds) between grouped
        transactions.
        """
        import pandas as pd

        run = Flow('TransactionStatsFlow').latest_successful_run

        raw_df = run.data.df
        raw_df["transactionDateTime"] = raw_df["transactionDateTime"] \
        .apply(lambda _: pd.to_datetime(_, format="%Y-%m-%dT%H:%M:%S"))
        # To remove ["accountNumber","merchantName"] groups of only one occurance
        duplicate_count_df = raw_df.sort_values(["accountNumber","merchantName","transactionDateTime"]) \
                                .groupby(["accountNumber","merchantName"]).size()
        more_than_1_group_occurance = duplicate_count_df[duplicate_count_df>1].reset_index()
        more_than_1_group_occurance.columns = ["accountNumber","merchantName","groupCount"]

        #raw_df["groupedTransactionDifferenceInSeconds"] = raw_df \
        #.sort_values(["accountNumber","merchantName", \
        #              "parsedTransactionDT"]) \
        #.groupby(["accountNumber","merchantName"]) \
        #["parsedTransactionDT"].diff().fillna(pd.Timedelta(seconds=0)).dt.total_seconds()
        #self.df = raw_df 
        self.next(self.end)

    @step
    def end(self):
        print("The End")

if __name__ == "__main__":
    TransactionDeduplicationFlow()
