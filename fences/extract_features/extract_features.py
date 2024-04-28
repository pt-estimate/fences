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
        import math
        import numpy as np
        import pandas as pd

        feature_df = self.df
        feature_df["enteredCVVMatch"] = feature_df["enteredCVV"] \
                                        .eq(feature_df["cardCVV"])
        #encode cyclical datetime
        feature_df['txn_day_of_month'] = feature_df['transactionDateTime'].apply(lambda _:_.day)
        feature_df['txn_day_of_week'] = feature_df['transactionDateTime'].apply(lambda _:_.dayofweek)
        feature_df['txn_month_of_year'] = feature_df['transactionDateTime'].apply(lambda _:_.month)
        feature_df['tdom_norm'] = 2 * math.pi *feature_df['txn_day_of_month'] / feature_df['txn_day_of_month'].max()
        feature_df['tdow_norm'] = 2 * math.pi *feature_df['txn_day_of_week'] /feature_df['txn_day_of_week'].max()
        feature_df['tmoy_norm'] = 2 * math.pi *feature_df['txn_month_of_year'] /feature_df['txn_month_of_year'].max()
        feature_df['cos_tdom'] = np.cos(feature_df['tdom_norm'])
        feature_df['cos_tdow'] = np.cos(feature_df['tdow_norm'])
        feature_df['cos_tmoy'] = np.cos(feature_df['tmoy_norm'])
        feature_df['sin_tdom'] = np.sin(feature_df['tdom_norm'])
        feature_df['sin_tdow'] = np.sin(feature_df['tdow_norm'])
        feature_df['sin_tmoy'] = np.sin(feature_df['tmoy_norm'])
        feature_df.drop(['txn_day_of_month','txn_day_of_week',
                         'txn_month_of_year','tdom_norm','tdow_norm',
                         'tmoy_norm'], axis=1)
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
        self.df = extracted_df
        self.next(self.end)

    @step
    def end(self):
        print("Ending FeatureExtractionFlow")

if __name__ == "__main__":
    FeatureExtractionFlow()
