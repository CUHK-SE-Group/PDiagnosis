import json

import requests

from consumer import CSVConsumer

from .trace_based_anomaly_detection import TraceModel
from .trace_parser import TraceParser


def trace_based_anomaly_detection_entrance(config):
    # initiation
    CONSUMER = CSVConsumer(config["base_dir"] / "normal" / "traces.csv")

    trace_parser = TraceParser()
    trace_model = TraceModel()
    print('Trace Monitor Running')
    # "Timestamp", "TraceId", "SpanId", "ParentSpanId", "SpanName", "ServiceName", "Duration", "ParentServiceName"
    CONSUMER.data.rename(
        columns={
            'Timestamp': 'timestamp',
            'TraceId': 'trace_id',
            'SpanId': 'span_id',
            'ParentSpanId': 'parent_id',
            'SpanName': 'span_name',
            'ServiceName': 'cmdb_id',
            'Duration': 'duration',
            'ParentServiceName': 'parent_cmdb_id',
        },
        inplace=True,
    )
    for index, data in CONSUMER.data.iterrows():
        complete_flag = trace_parser.update_trace_info(data)
        if complete_flag == 1:

            trace_model.update_model_pattern(trace_parser)
            trace_model.add_to_q()
            # print(json.dumps(trace_model.model))
            if trace_model.fixed_trace_q.full():
                main_key, anomaly_time = trace_model.anomaly_detection_with_queue(data['timestamp'])
                if main_key != 'null':
                    send_dict = {'cmdb_id': main_key, 'timestamp': anomaly_time}

                    requests.post(
                        'http://127.0.0.1:' + str(config['decision_port']) + '/trace',
                        json.dumps(send_dict),
                    )


def trace_based_anomaly_detection(config):
    # initiation
    CONSUMER = CSVConsumer(config["trace_path"])
    trace_cache = []
    trace_parser = TraceParser()
    trace_model = TraceModel()

    # "Timestamp", "TraceId", "SpanId", "ParentSpanId", "SpanName", "ServiceName", "Duration", "ParentServiceName"
    CONSUMER.data.rename(
        columns={
            'Timestamp': 'timestamp',
            'TraceId': 'trace_id',
            'SpanId': 'span_id',
            'ParentSpanId': 'parent_id',
            'SpanName': 'span_name',
            'ServiceName': 'cmdb_id',
            'Duration': 'duration',
            'ParentServiceName': 'parent_cmdb_id',
        },
        inplace=True,
    )
    for index, data in CONSUMER.data.iterrows():
        complete_flag = trace_parser.update_trace_info(data)
        if complete_flag == 1:

            trace_model.update_model_pattern(trace_parser)
            trace_model.add_to_q()
            # print(json.dumps(trace_model.model))
            if trace_model.fixed_trace_q.full():
                main_key, anomaly_time = trace_model.anomaly_detection_with_queue(data['timestamp'])
                if main_key != 'null':
                    trace_cache.append({'cmdb_id': main_key, 'timestamp': anomaly_time})
    print('Trace Process Done')
    return trace_cache
