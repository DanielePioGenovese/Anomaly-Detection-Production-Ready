from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import IsolationForest

class ModelFactory:
    @staticmethod
    def build_pipeline(num_cols, cat_cols, settings): # cat_cols = Cycle_Phase_ID
        pre = ColumnTransformer(
            transformers=[
                ("num", Pipeline([
                    ("imp", SimpleImputer(strategy="median")),
                    ("scaler", StandardScaler())
                ]), num_cols),
                ("cat", Pipeline([
                    ("imp", SimpleImputer(strategy="most_frequent")),
                    ("ohe", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                ]), cat_cols),
            ],
            remainder="drop",
            verbose_feature_names_out=False,
        )

        model = IsolationForest(
            n_estimators=settings.training.if_n_estimators,
            contamination=settings.training.contamination,
            random_state=settings.training.random_state,
            n_jobs=-1,
        )

        return Pipeline([("pre", pre), ("model", model)])