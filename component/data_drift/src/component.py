from threading import local
from kfp.components import OutputPath
from typing import NamedTuple


def drift_detection(
    reference_data_path: str, current_data_path: str
) -> NamedTuple("DataDriftOutput", [("mlpipeline_ui_metadata", "UI_metadata")]):
    from evidently.model_profile import Profile
    from evidently.model_profile.sections import DataDriftProfileSection
    from evidently.dashboard import Dashboard
    from evidently.dashboard.tabs import DataDriftTab
    from evidently.pipeline.column_mapping import ColumnMapping
    import json
    import os
    import pickle
    import pandas as pd
    from evidently.dashboard import Dashboard
    from evidently.dashboard.tabs import DataDriftTab
    import os

    def _detect_dataset_drift(
        reference,
        production,
        column_mapping,
        confidence=0.95,
        threshold=0.5,
        get_ratio=False,
    ) -> bool:
        """
        Returns True if Data Drift is detected, else returns False.
        If get_ratio is True, returns ration of drifted features.
        The Data Drift detection depends on the confidence level and the threshold.
        For each individual feature Data Drift is detected with the selected confidence (default value is 0.95).
        Data Drift for the dataset is detected if share of the drifted features is above the selected threshold (default value is 0.5).
        """

        data_drift_profile = Profile(sections=[DataDriftProfileSection()])
        data_drift_profile.calculate(
            reference, production, column_mapping=column_mapping
        )
        report = data_drift_profile.json()
        json_report = json.loads(report)
        #     return json_report
        drifts = []
        num_features = (
            column_mapping.numerical_features
            if column_mapping.numerical_features
            else []
        )
        cat_features = (
            column_mapping.categorical_features
            if column_mapping.categorical_features
            else []
        )
        for feature in num_features + cat_features:
            drifts.append(
                json_report["data_drift"]["data"]["metrics"][feature]["drift_score"]
            )

        n_features = len(drifts)
        n_drifted_features = sum([1 if x < (1.0 - confidence) else 0 for x in drifts])

        if get_ratio:
            return n_drifted_features / n_features
        else:
            if n_drifted_features / n_features >= threshold:
                return True
            else:
                return False

    def get_file_URL(gcs_uri: str):

        bucket = gcs_uri.split("/")[2]
        object_name = "/".join(gcs_uri.split("/")[3:])
        URL = f"https://storage.googleapis.com/{bucket}/{object_name}"
        return URL

    reference_data_local_path = get_file_URL(gcs_uri=reference_data_path)
    current_data_local_path = get_file_URL(gcs_uri=current_data_path)
    current_data = pd.read_csv(current_data_local_path)
    reference_data = pd.read_csv(reference_data_local_path)
    current_feature_data = current_data[reference_data.columns]
    data_columns = ColumnMapping()
    data_columns.numerical_features = reference_data.columns.tolist()
    drift_detected = _detect_dataset_drift(
        reference_data, current_feature_data, data_columns
    )
    if drift_detected:
        print("drift_detected")
        # can also perform trigger an alert if required

    #################################
    # Generate data drift dashboard #
    #################################

    data_drift_dashboard = Dashboard(tabs=[DataDriftTab(verbose_level=1)])

    data_drift_dashboard.calculate(
        reference_data, current_feature_data, column_mapping=data_columns
    )
    data_drift_dashboard_filename = "data_drift.html"
    local_dir = "/tmp/artifact_downloads"
    if not os.path.exists(local_dir):
        os.mkdir(local_dir)
    static_html_path = os.path.join(local_dir, data_drift_dashboard_filename)
    data_drift_dashboard.save(static_html_path)
    with open(static_html_path, "r") as f:
        inline_report = f.read()
    metadata = {
        "outputs": [
            {
                "storage": "inline",
                "source": inline_report,
                "type": "web-app",
            },
        ]
    }
    from collections import namedtuple

    output = namedtuple("DataDriftOutput", ["mlpipeline_ui_metadata"])
    return output(json.dumps(metadata))
