import kfp.dsl as dsl
from kfp.compiler import Compiler
from kfp.components import create_component_from_func
import sys

sys.path.append("../../")

from component.data_drift.src.component import drift_detection


@dsl.pipeline(
    name="data-drift-pipeline",
    description="An example to demonstrate data drift",
)
def data_drift_pipeline(
    reference_dataset: str = "gs//evidently_kubeflow_datasets/reference_data.csv",
    inference_dataset: str = "gs//evidently_kubeflow_datasets/inference_data.csv",
):

    data_drift_op = create_component_from_func(
        drift_detection,
        base_image="python:3.8",
        packages_to_install=[
            "pandas==1.3.5",
            "evidently==0.1.48.dev0",
            "google-cloud-storage==1.40.0",
        ],
    )

    data_drift_task = data_drift_op(
        reference_data_path=reference_dataset,
        current_data_path=inference_dataset,
    )


Compiler().compile(data_drift_pipeline, "data-drift-pipeline.yaml")
