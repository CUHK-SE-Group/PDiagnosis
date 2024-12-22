import json

import requests

from consumer import CSVConsumer

template_dict = {}


def log_based_anomaly_detection_entrance(config):
    # initiation
    CONSUMER = CSVConsumer(config["log_path"])
    for index, data in CONSUMER.data.iterrows():

        if config['log_keyword'] in data["Body"].lower():
            send_dict = {
                "cmdb_id": data["ServiceName"],
                "timestamp": data["Timestamp"],
                "logname": data["Body"],
            }
            requests.post(
                'http://127.0.0.1:' + str(config['decision_port']) + '/log',
                json.dumps(send_dict),
            )


def log_based_anomaly_detection(config):
    CONSUMER = CSVConsumer(config["log_path"])
    CONSUMER.data = CONSUMER.data[CONSUMER.data["SeverityNumber"] == 17]
    CONSUMER.data.rename(
        columns={
            "Body": "logname",
            "ServiceName": "cmdb_id",
            "Timestamp": "timestamp",
        },
        inplace=True,
    )
    ret = CONSUMER.data[["cmdb_id", "timestamp", "logname"]]
    return ret
