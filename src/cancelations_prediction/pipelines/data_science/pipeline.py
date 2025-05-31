"""
This is a boilerplate pipeline 'data_science'
generated using Kedro 0.19.13
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa

from .nodes import split_data, train_xgb_model, get_best_hip_params, train_with_best_param, evaluate_model


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=split_data,
                inputs=["model_input_table", "params:model_options"],
                outputs=["X_train", "X_test", "y_train", "y_test"],
                name="split_data_node",
            ),
            node(
                func= train_xgb_model,
                inputs= ["X_train", "y_train"],
                outputs= "classifier",
                name= "train_model_node",
            ),
            node(
                func= get_best_hip_params,
                inputs= ["X_train", "y_train"],
                outputs= "best_hyper_params",
                name= "tunning_params_node",
            ),
            node(
                func= train_with_best_param,
                inputs= ["X_train", "y_train", "best_hyper_params"],
                outputs= "best_classifier",
                name = "train_final_model_node",
            ),
            node(
                func= evaluate_model,
                inputs= ["best_classifier","X_test","y_test"],
                outputs= None,
                name= "evaluate_model_node",
            )
        ]
    )
