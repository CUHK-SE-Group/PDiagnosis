import csv
import json
import logging
import os
import signal
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta
from multiprocessing import Pool
from pathlib import Path
from trace.trace_entrance import trace_based_anomaly_detection

import pandas as pd
import requests
import toml
from flask import Flask, request

import decision_maker as dm
from decision_maker import app
from log.log import log_based_anomaly_detection
from metrics.metrics import metric_monitor

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)


def evaluate(case_dir: Path):
    with open('/home/nn/workspace/PDiagnosis/src/config/train-ticket.json') as f:
        config = json.load(f)
        config["base_dir"] = case_dir
        # log_cache = log_based_anomaly_detection(config)

        anomalous = metric_monitor(
            config,
        )
        if len(anomalous) < config["anomaly_num_threshold"]:
            anomalous = pd.DataFrame()

        return case_dir.name, anomalous


def calculate_overlap(start1, end1, start2, end2):
    if start1 is None or end1 is None or start2 is None or end2 is None:
        return False  # No overlap if any input is None
    return max(start1, start2) < min(end1, end2)


def f1_score(predicted_range, ground_truth_range):
    """
    Calculate the F1 score based on predicted and ground truth time ranges.

    Args:
        predicted_range (tuple): A tuple with predicted start and end times (datetime, datetime).
        ground_truth_range (tuple): A tuple with ground truth start and end times (datetime, datetime).

    Returns:
        float: precision, recall, F1 score.
    """
    predicted_start, predicted_end = predicted_range
    truth_start, truth_end = ground_truth_range
    truth_start, truth_end = map(
        lambda x: int(pd.to_datetime(x).as_unit("s").timestamp()),
        [truth_start, truth_end],
    )
    # Calculate overlap (True Positive)
    if calculate_overlap(predicted_start, predicted_end, truth_start, truth_end):
        tp = 1
        fp = 0
        fn = 0
    else:
        tp = 0
        fp = 1  # No overlap, predicted range is considered as false positive
        fn = 1  # No overlap, ground truth range is considered as false negative

    # Calculate Precision and Recall
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0

    # Calculate F1 score
    if (precision + recall) > 0:
        f1 = 2 * (precision * recall) / (precision + recall)
    else:
        f1 = 0  # If precision and recall are both 0, F1 is 0

    return precision, recall, f1


def calculate_metrics(prediction_data, gt_list):
    AC_at_1 = 0
    AC_at_3 = 0
    AC_at_5 = 0
    avg_at_5 = 0
    total_cases = len(gt_list)
    total_precision = 0
    total_recall = 0
    total_f1 = 0

    for gt in gt_list:
        case_name = gt['case']
        gt_service = gt['service']
        # Calculate gt_abnormal_time_range
        gt_start_time = gt['timestamp']
        gt_end_time = gt_start_time + timedelta(minutes=5)
        gt_abnormal_time_range = (gt_start_time, gt_end_time)

        # Find the corresponding case in prediction_data
        for case, df in prediction_data:
            if case == case_name:
                # Calculate F1 score using predict_abnormal_time_range and gt_abnormal_time_range
                if df.empty:
                    # No anomlies predicted
                    precision, recall, f1 = 0, 0, 0
                    continue
                else:
                    predicted_start, predicted_end = df.iloc[0]['timestamp_min'], df.iloc[0]['timestamp_max']
                    precision, recall, f1 = f1_score((predicted_start, predicted_end), gt_abnormal_time_range)

                # Update total precision, recall, and f1
                total_precision += precision
                total_recall += recall
                total_f1 += f1

                # Find ground truth service index in the combined_ranking dataframe
                ranking_df = df
                if gt_service in ranking_df['cmdb_id'].values:
                    service_index = ranking_df[ranking_df['cmdb_id'] == gt_service]['rank'].values[0]

                    # AC@1
                    if service_index == 1:
                        AC_at_1 += 1

                    # AC@3
                    if service_index <= 3:
                        AC_at_3 += 1

                    # AC@5
                    if service_index <= 5:
                        AC_at_5 += 1

                    # Avg@5
                    if service_index <= 5:
                        avg_at_5 += (5 - service_index + 1) / 5.0

                break

    # Ranking metrics
    AC_at_1 /= total_cases
    AC_at_3 /= total_cases
    AC_at_5 /= total_cases
    avg_at_5 /= total_cases
    precision_avg = total_precision / total_cases
    recall_avg = total_recall / total_cases
    f1_avg = total_f1 / total_cases

    return {
        "AD Precision": precision_avg,
        "AD Recall": recall_avg,
        "AD F1": f1_avg,
        "AC@1": AC_at_1,
        "AC@3": AC_at_3,
        "AC@5": AC_at_5,
        "Avg@5": avg_at_5,
    }


def main():
    root_base_dir = Path("/home/nn/workspace/Metis-DataSet/ts-all")
    fault_injection_file = root_base_dir / "fault_injection.toml"
    data = toml.load(fault_injection_file)
    gt_list = data["chaos_injection"]

    case_dirs = [p for p in root_base_dir.iterdir() if p.is_dir()]

    with Pool() as pool:
        prediction_data = list(pool.map(evaluate, case_dirs))

        result = calculate_metrics(prediction_data, gt_list)
        # save as result.csv
        result_df = pd.DataFrame([result])
        result_df.to_csv("result.csv", index=False)


if __name__ == "__main__":
    main()
