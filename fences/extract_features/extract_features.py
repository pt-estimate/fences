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

        leakage_data = ["accountNumber","customerId","cardLast4Digits"]
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
        self.df = cleaned_df
        self.next(self.prepare_features)

    @step
    def prepare_features(self):
        from category_encoders import OneHotEncoder
        feature_df = self.df
        feature_df["enteredCVVMatch"] = feature_df["enteredCVV"] \
                                        .eq(feature_df["cardCVV"])
        print("Numeric features:")
        print(feature_df.select_dtypes(include='number').columns.values)
        print("Object feature cardinality:")
        cardinal_df = feature_df.select_dtypes(include="O")
        print(cardinal_df.nunique())
        print("Features to one-hot encode:")
        ohe_features = cardinal_df.loc[:, cardinal_df.nunique() <= 5].columns
        # TODO: embed these high-cardinality features
        embedding_features = cardinal_df.loc[:, cardinal_df.nunique() \
                                            > 5].columns
        print(embedding_features)
        encoder = OneHotEncoder(cols=ohe_features)
        extracted_df = encoder.fit_transform(feature_df)
        self.next(self.end)

    @step
    def end(self):
        print("Ending FeatureExtractionFlow")

if __name__ == "__main__":
    FeatureExtractionFlow()
