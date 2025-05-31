"""
This is a boilerplate pipeline 'data_science'
generated using Kedro 0.19.13
"""
import logging

import pandas as pd
from sklearn.metrics import recall_score
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from xgboost import XGBClassifier


def split_data(data: pd.DataFrame, parameters: dict) -> tuple:
    X = data[parameters["features"]]
    y = data["cancelacion"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=parameters["test_size"], random_state=parameters["random_state"]
    )
    return X_train, X_test, y_train, y_test

# MODELO INICIAL
def train_xgb_model(X_train: pd.DataFrame, y_train: pd.Series) -> XGBClassifier:
    weight = len(y_train[y_train == 0]) / len(y_train[y_train == 1])
    model = XGBClassifier(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        scale_pos_weight=weight,
        use_label_encoder=False,
        eval_metric='logloss',
        random_state=42
    )
    model.fit(X_train, y_train)
    return model



# TUNNING HIPPER PARAMS
def get_best_hip_params(X_train, y_train):
    weight = len(y_train[y_train == 0]) / len(y_train[y_train == 1])
    xgb = XGBClassifier(
        use_label_encoder=False,
        eval_metric='logloss',
        random_state=42,
        scale_pos_weight=weight
    )
    param_grid = {
        'n_estimators': [100, 200, 300],
        'max_depth': [3, 6, 9],
        'learning_rate': [0.01, 0.1, 0.3],
        'subsample': [0.8, 1.0],
        'colsample_bytree': [0.6, 0.8, 1.0]
    }
    random_search = RandomizedSearchCV(
        estimator=xgb,
        param_distributions=param_grid,
        n_iter=20,
        scoring='recall',
        cv=3,
        verbose=2,
        n_jobs=1
    )
    random_search.fit(X_train, y_train)
    # best_model = random_search.best_estimator_
    best_params = random_search.best_params_

    # Crear el diccionario
    params_dict = {}
    params_dict['best_params'] = best_params

    # return best_model, best_params
    return best_params

# TRAIN WITH BEST HIPER PARAMS
def train_with_best_param(X_train, y_train, best_params: dict) -> XGBClassifier:
    scale_weight = len(y_train[y_train == 0]) / len(y_train[y_train == 1])
    params_actualizados = {
        **best_params,
        "scale_pos_weight": scale_weight,
        "use_label_encoder": False,
        "eval_metric": "logloss",
        "random_state": 42
    }
    modelo = XGBClassifier(**params_actualizados)
    modelo.fit(X_train, y_train)
    return modelo


def evaluate_model(
    classifier: XGBClassifier, X_test: pd.DataFrame, y_test: pd.Series
) -> dict[str, float]:
    """Calculates and logs the coefficient of determination.

    Args:
        regressor: Trained model.
        X_test: Testing data of independent features.
        y_test: Testing data for price.
    """
    y_pred = classifier.predict(X_test)
    recall = recall_score(y_test, y_pred)
    logger = logging.getLogger(__name__)
    logger.info("Model has a recall of %.3f on test data.")
    return {"recall": recall}
