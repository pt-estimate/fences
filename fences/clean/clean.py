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
        # To remove ["accountNumber","merchantName"] groups of only one
        # occurance so that when we leverage the time diff later, there is
        # a difference between a single event and the first event
        recur_count_df = raw_df.sort_values(["accountNumber",
                                            "merchantName",
                                            "transactionAmount",
                                            "transactionDateTime"]) \
                                .groupby(["accountNumber",
                                          "merchantName",
                                          "transactionAmount"]).size() \
                                .reset_index()
        #gt1_grp_count = dupe_count_df[dupe_count_df>1].reset_index()
        recur_count_df.columns = ["accountNumber","merchantName",
                                 "transactionAmount","merchantRecurrenceCount"]

        dupe_df = raw_df.merge(recur_count_df, how="left",
                               on=["accountNumber","merchantName",
                                   "transactionAmount"])
        dupe_df["multiSwipeDiffSeconds"] = dupe_df.sort_values(
                                           ["accountNumber","merchantName", \
                                            "transactionAmount", 
                                            "transactionDateTime"]) \
                                .groupby(["accountNumber","merchantName",
                                          "transactionAmount"]) \
                                ["transactionDateTime"].diff() \
                                .fillna(pd.Timedelta(seconds=0)) \
                                .dt.total_seconds()
        dupe_df["merchantRecurrenceDiffSeconds"] = None
        cond = (dupe_df["merchantRecurrenceCount"]>1)
        dupe_df["merchantRecurrenceDiffSeconds"] = dupe_df[ \
                "merchantRecurrenceDiffSeconds"].case_when( \
                        [(cond, dupe_df["multiSwipeDiffSeconds"])])

        print(dupe_df.shape)
        deduped_df = dupe_df[(dupe_df["transactionType"]!="REVERSAL") & \
                             ((dupe_df["merchantRecurrenceDiffSeconds"]==0) | \
                             (dupe_df["merchantRecurrenceDiffSeconds"]>180) | \
                             (dupe_df["merchantRecurrenceDiffSeconds"] \
                              .isnull()))]
        print(deduped_df.shape)
        self.df = deduped_df
        self.next(self.screen_account_abuse)


    @step
    def screen_account_abuse(self):
        df = self.df
        df["paymentInstrumentId"] = df["cardLast4Digits"].astype(str) + ":" + \
                                    df["cardCVV"].astype(str) + ":" + \
                                    df["currentExpDate"]
        # Test to ensure no accounts are using the same payment instrument
        print("Test for payment instrument abuse")
        print(df.groupby("paymentInstrumentId")["accountNumber"].nunique())
        self.next(self.end)

    @step
    def end(self):
        print("Ending TransactionDeduplicationFlow")

if __name__ == "__main__":
    TransactionDeduplicationFlow()
