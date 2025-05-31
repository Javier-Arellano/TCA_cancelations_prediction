"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.19.13
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa

from .nodes import procesar_reservas, get_pesos


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func = procesar_reservas,
                inputs = ["iar_Reservaciones", "id_Agencias","id_Canales","id_Empresa",
                          "id_Estados", "id_Habitacion","id_Paises","id_Paquete",
                          "id_Programa","id_Reservaciones","id_Segmento"],
                outputs = "processed_reservas",
                name = "processed_reservas_node"
            ),
            node(
                func=get_pesos,
                inputs="processed_reservas",
                outputs="features_table",
                name="features_table_node"
            )
        ]
    )
