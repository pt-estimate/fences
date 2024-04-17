from metaflow import Flow, FlowSpec, step


class FeatureExtractionFlow(FlowSpec):
    """
    A flow to prepare the data for the modeling step 

    The flow performs the following steps:
        1. Identify typology of features
        2. Train embeddings on sparse categoricals
    """

    @step
    def start(self):
        """
        The start step:
        1. Loads the data into dataframe
        """
        import pandas as pd

        run = Flow('TransactionDeduplicationFlow').latest_successful_run

        cleaned_df = run.data.df

        leakage_data = ["accountNumber","customerId"]
        print(f"Dropping leakage features:")
        print(leakage_data)
        cleaned_df.drop(leakage_data, axis=1, inplace=True)

        nunique = cleaned_df.nunique()
        cols_to_drop = nunique[nunique == 0].index
        print(f"Dropping one value features:") 
        print(f"{cols_to_drop.values}.") 
        cleaned_df.drop(cols_to_drop, axis=1, inplace=True)

        date_map = {"accountOpenDate":"%Y-%m-%d","dateOfLastAddressChange":
                    "%Y-%m-%d","currentExpDate":"%m/%Y"}
        for dt_feature in date_map:
            cleaned_df[dt_feature] = cleaned_df[dt_feature].apply(lambda _: \
                    pd.to_datetime(_, format=date_map[dt_feature]))
        print("Numeric features:")
        print(cleaned_df.select_dtypes(include='number').columns.values)
        print("Object feature cardinality:")
        cardinal_df = cleaned_df.select_dtypes(include="O")
        print(cardinal_df.nunique())
        print("Features prepped to one-hot encode:")
        print(cardinal_df.loc[:, cardinal_df.nunique() <= 20].columns)

        self.next(self.end)

    @step
    def end(self):
        print("Ending FeatureExtractionFlow")

if __name__ == "__main__":
    FeatureExtractionFlow()
