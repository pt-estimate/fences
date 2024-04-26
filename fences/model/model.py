from metaflow import Flow, FlowSpec, step


class ModelTrainFlow(FlowSpec):
    """
    A flow to train a supervised model that predicts if a transaction is
    fraudulent.

    The flow performs the following steps:
        1. Split data in preparation for training/validation.
        2. Train model.
    """

    @step
    def start(self):
        """
        The start step:
        1. Split data into train/validation.
        2. Train model.
        """
        import pandas as pd
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import precision_recall_curve,roc_auc_score, \
                                    roc_curve, auc, confusion_matrix

        from xgboost import XGBClassifier

        run = Flow("FeatureExtractionFlow").latest_successful_run
        X = run.data.df
        X.drop(columns=["transactionDateTime","merchantName","merchantCategoryCode","currentExpDate","accountOpenDate","dateOfLastAddressChange"], inplace=True)
        y = X.pop("isFraud")
        X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=2021, test_size=0.30)
 
        xgb = XGBClassifier(enable_categorical=True)
        xgb.fit(X_train, y_train)
        y_pred = xgb.predict(X_test)
        precision, recall, thresholds = precision_recall_curve(y_test, y_pred)
        area = auc(recall, precision)

        print('------------ Results for XGBClassifier ---------------')
        print('cm:', confusion_matrix(y_test,y_pred))
        print('roc_auc_score:',roc_auc_score(y_test,y_pred))
        print("Area Under P-R Curve: ", area)

        self.next(self.end)

    @step
    def end(self):
        print("Ending ModelTrainFlow")

if __name__ == "__main__":
    ModelTrainFlow()
