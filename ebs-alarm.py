import boto3
import logging
import os

from config_reader import read_alarm_config, read_config, CONFIG_KEYS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler("ebs-alarm.log")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

PAGINATION_COUNT = 50
EBS_PREFIX = "EBS_"

MODE_CREATE = "CREATE"  # create/update alarms
MODE_DELETE = "DELETE"  # delete alarms
MODE_DISABLE = "DISABLE"  # disable alarms


class AlarmClient:
    def __init__(self, config):
        self.read_latency_alarm_config = self.get_config(config["ReadLatency"])
        self.write_latency_alarm_config = self.get_config(config["WriteLatency"])
        self.impaired_alarm_config = self.get_config(config["ImpairedVolume"])
        self.sns_arn = config["default"]["sns_arn"]
        aws_region = config["default"]["aws_region"]
        self.ec2 = boto3.client("ec2", region_name=aws_region)
        self.cloudwatch = boto3.client("cloudwatch", region_name=aws_region)
        self.sns = boto3.client("sns", region_name=aws_region)

    def get_config(self, input_config):
        config = {}
        config["EvaluationPeriods"] = int(
            read_config(input_config, CONFIG_KEYS["EvaluationPeriods"])
        )
        config["DatapointsToAlarm"] = int(
            read_config(input_config, CONFIG_KEYS["DatapointsToAlarm"])
        )
        config["Threshold"] = int(read_config(input_config, CONFIG_KEYS["Threshold"]))
        config["ComparisonOperator"] = read_config(
            input_config, CONFIG_KEYS["ComparisonOperator"]
        )
        config["TreatMissingData"] = read_config(
            input_config, CONFIG_KEYS["TreatMissingData"]
        )
        return config

    def create(self):
        volumes = self.get_ebs_volumes()
        alarms = self.get_alarms()
        for volume in volumes:
            logger.info("Working on alarms for volume: %s", volume)
            impaired_alarm = alarms.pop(
                self.create_alarm_name(volume, "ImpairedAlarm"), None
            )
            self.handle_impaired_alarm(impaired_alarm, volume)
            read_latency_alarm = alarms.pop(
                self.create_alarm_name(volume, "ReadLatency"), None
            )
            self.handle_read_latency_alarm(read_latency_alarm, volume)
            write_latency_alarm = alarms.pop(
                self.create_alarm_name(volume, "WriteLatency"), None
            )
            self.handle_write_latency_alarm(write_latency_alarm, volume)
        self.delete_alarms(
            list(alarms.keys())
        )  # delete alarms for non existent EBS volumes

    def get_ebs_volumes(self):
        paginator_vols = self.ec2.get_paginator("describe_volumes")
        volume_ids = []
        for page in paginator_vols.paginate(MaxResults=PAGINATION_COUNT):
            for volume in page["Volumes"]:
                volume_ids.append(volume["VolumeId"])
        return volume_ids

    def get_alarms(self):
        paginator_alarms = self.cloudwatch.get_paginator("describe_alarms")
        paginator_parameters = {
            "AlarmNamePrefix": EBS_PREFIX,
            "MaxRecords": PAGINATION_COUNT,
        }
        alarms = {}
        for page in paginator_alarms.paginate(**paginator_parameters):
            for alarm in page["MetricAlarms"]:
                alarms[alarm["AlarmName"]] = alarm
        return alarms

    def create_alarm_name(self, volume, alarmType):
        return EBS_PREFIX + volume + "_" + alarmType

    def handle_impaired_alarm(self, alarm, volume_id):
        if alarm is None:
            self.create_impaired_alarm(volume_id)
        else:
            self.handle_existing_alarm(alarm, self.impaired_alarm_config)

    def handle_read_latency_alarm(self, alarm, volume_id):
        if alarm is None:
            self.create_read_latency_alarm(volume_id)
        else:
            self.handle_existing_alarm(alarm, self.read_latency_alarm_config)

    def handle_write_latency_alarm(self, alarm, volume_id):
        if alarm is None:
            self.create_write_latency_alarm(volume_id)
        else:
            self.handle_existing_alarm(alarm, self.write_latency_alarm_config)

    def create_impaired_alarm(self, volume_id):
        alarm_name = self.create_alarm_name(volume_id, "ImpairedAlarm")
        alarm_details = {
            "AlarmName": alarm_name,
            "AlarmActions": [self.sns_arn],
            "EvaluationPeriods": self.impaired_alarm_config["EvaluationPeriods"],
            "DatapointsToAlarm": self.impaired_alarm_config["DatapointsToAlarm"],
            "Threshold": self.impaired_alarm_config["Threshold"],
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": self.impaired_alarm_config["TreatMissingData"],
            "Metrics": [
                {
                    "Id": "e1",
                    "Expression": "IF(m3>0 AND m1+m2==0, 1, 0)",
                    "Label": "ImpairedVolume",
                    "ReturnData": True,
                },
                {
                    "Id": "m3",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeQueueLength",
                            "Dimensions": [{"Name": "VolumeId", "Value": volume_id}],
                        },
                        "Period": 60,
                        "Stat": "Average",
                    },
                    "ReturnData": False,
                },
                {
                    "Id": "m1",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeReadOps",
                            "Dimensions": [{"Name": "VolumeId", "Value": volume_id}],
                        },
                        "Period": 60,
                        "Stat": "Average",
                    },
                    "ReturnData": False,
                },
                {
                    "Id": "m2",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeWriteBytes",
                            "Dimensions": [{"Name": "VolumeId", "Value": volume_id}],
                        },
                        "Period": 60,
                        "Stat": "Average",
                    },
                    "ReturnData": False,
                },
            ],
        }
        self.put_metric_alarm(alarm_details)

    def create_read_latency_alarm(self, volume_id):
        alarm_name = self.create_alarm_name(volume_id, "ReadLatency")
        alarm_details = {
            "AlarmName": alarm_name,
            "AlarmActions": [self.sns_arn],
            "EvaluationPeriods": self.read_latency_alarm_config["EvaluationPeriods"],
            "DatapointsToAlarm": self.read_latency_alarm_config["DatapointsToAlarm"],
            "Threshold": self.read_latency_alarm_config["Threshold"],
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": self.read_latency_alarm_config["TreatMissingData"],
            "Metrics": [
                {
                    "Id": "e1",
                    "Expression": "(m1/m2)*1000",
                    "Label": "ReadLatency",
                    "ReturnData": True,
                },
                {
                    "Id": "m1",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeTotalReadTime",
                            "Dimensions": [{"Name": "VolumeId", "Value": volume_id}],
                        },
                        "Period": 60,
                        "Stat": "Average",
                    },
                    "ReturnData": False,
                },
                {
                    "Id": "m2",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeReadOps",
                            "Dimensions": [{"Name": "VolumeId", "Value": volume_id}],
                        },
                        "Period": 60,
                        "Stat": "Average",
                    },
                    "ReturnData": False,
                },
            ],
        }
        self.put_metric_alarm(alarm_details)

    def create_write_latency_alarm(self, volume_id):
        alarm_name = self.create_alarm_name(volume_id, "WriteLatency")
        alarm_details = {
            "AlarmName": alarm_name,
            "AlarmActions": [self.sns_arn],
            "EvaluationPeriods": self.write_latency_alarm_config["EvaluationPeriods"],
            "DatapointsToAlarm": self.write_latency_alarm_config["DatapointsToAlarm"],
            "Threshold": self.write_latency_alarm_config["Threshold"],
            "ComparisonOperator": "GreaterThanOrEqualToThreshold",
            "TreatMissingData": self.write_latency_alarm_config["TreatMissingData"],
            "Metrics": [
                {
                    "Id": "e1",
                    "Expression": "(m1/m2)*1000",
                    "Label": "WriteLatency",
                    "ReturnData": True,
                },
                {
                    "Id": "m1",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeTotalWriteTime",
                            "Dimensions": [{"Name": "VolumeId", "Value": volume_id}],
                        },
                        "Period": 60,
                        "Stat": "Average",
                    },
                    "ReturnData": False,
                },
                {
                    "Id": "m2",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EBS",
                            "MetricName": "VolumeWriteOps",
                            "Dimensions": [{"Name": "VolumeId", "Value": volume_id}],
                        },
                        "Period": 60,
                        "Stat": "Average",
                    },
                    "ReturnData": False,
                },
            ],
        }
        self.put_metric_alarm(alarm_details)

    def handle_existing_alarm(self, alarm, config):
        if self.update_alarm(alarm, config):
            alarm["ActionsEnabled"] = True
            if config["EvaluationPeriods"] is not None:
                alarm["EvaluationPeriods"] = config["EvaluationPeriods"]
            if config["DatapointsToAlarm"] is not None:
                alarm["DatapointsToAlarm"] = config["DatapointsToAlarm"]
            if config["Threshold"] is not None:
                alarm["Threshold"] = config["Threshold"]
            if config["ComparisonOperator"] is not None:
                alarm["ComparisonOperator"] = config["ComparisonOperator"]
            if config["TreatMissingData"] is not None:
                alarm["TreatMissingData"] = config["TreatMissingData"]
            self.put_metric_alarm(alarm)
        else:
            logger.info("Alarm for volume is upto date")

    def update_alarm(self, alarm, config):
        alarm_name = alarm["AlarmName"]
        if alarm["ActionsEnabled"] == False:
            logger.info(
                "Change in ActionsEnabled for alert %s. Enabling alarm", alarm_name
            )
            return True
        if (
            "EvaluationPeriods" in config
            and alarm["EvaluationPeriods"] != config["EvaluationPeriods"]
        ):
            logger.info(
                "Change in EvaluationPeriods for alert %s. old value %s, new value %s",
                alarm_name,
                alarm["EvaluationPeriods"],
                config["EvaluationPeriods"],
            )
            return True
        if (
            "DatapointsToAlarm" in config
            and alarm["DatapointsToAlarm"] != config["DatapointsToAlarm"]
        ):
            logger.info(
                "Change in DatapointsToAlarm for alert %s. old value %s, new value %s",
                alarm_name,
                alarm["DatapointsToAlarm"],
                config["DatapointsToAlarm"],
            )
            return True
        if "Threshold" in config and alarm["Threshold"] != config["Threshold"]:
            logger.info(
                "Change in Threshold for alert %s. old value %s, new value %s",
                alarm_name,
                alarm["Threshold"],
                config["Threshold"],
            )
            return True
        if (
            "ComparisonOperator" in config
            and alarm["ComparisonOperator"] != config["ComparisonOperator"]
        ):
            logger.info(
                "Change in ComparisonOperator for alert %s. old value %s, new value %s",
                alarm_name,
                alarm["ComparisonOperator"],
                config["ComparisonOperator"],
            )
            return True
        if (
            "TreatMissingData" in config
            and alarm["TreatMissingData"] != config["TreatMissingData"]
        ):
            logger.info(
                "Change in TreatMissingData for alert %s. old value %s, new value %s",
                alarm_name,
                alarm["TreatMissingData"],
                config["TreatMissingData"],
            )
            return True
        return False

    def put_metric_alarm(self, alarm_details):
        try:
            self.cloudwatch.put_metric_alarm(
                AlarmName=alarm_details["AlarmName"],
                AlarmActions=alarm_details["AlarmActions"],
                EvaluationPeriods=alarm_details["EvaluationPeriods"],
                DatapointsToAlarm=alarm_details["DatapointsToAlarm"],
                Threshold=alarm_details["Threshold"],
                ComparisonOperator=alarm_details["ComparisonOperator"],
                TreatMissingData=alarm_details["TreatMissingData"],
                Metrics=alarm_details["Metrics"],
            )
            logger.info(f"Alarm '{alarm_details['AlarmName']}' updated/created")
        except Exception as e:
            logger.exception(f"Error creating alarm: '{alarm_details['AlarmName']}'", e)

    def delete_alarms(self, alarm_names):
        if len(alarm_names) > 0:
            alarm_chunks = [
                alarm_names[x : x + PAGINATION_COUNT]
                for x in range(0, len(alarm_names), PAGINATION_COUNT)
            ]
            for alarms_to_delete in alarm_chunks:
                logger.info("Deleting alarms %s", alarms_to_delete)
                self.cloudwatch.delete_alarms(AlarmNames=alarms_to_delete)

    def disable(self):
        alarms = self.get_alarms()
        enabled_alarms = {
            alarm_name: alarm
            for alarm_name, alarm in alarms.items()
            if alarm["ActionsEnabled"]
        }
        logger.info("Disabling alarms")
        alarm_names = list(enabled_alarms.keys())
        if len(alarm_names) > 0:
            alarm_chunks = [
                alarm_names[x : x + PAGINATION_COUNT]
                for x in range(0, len(alarm_names), PAGINATION_COUNT)
            ]
            for alarms_to_disable in alarm_chunks:
                logger.info("Disabling alarms %s", alarms_to_disable)
                self.cloudwatch.disable_alarm_actions(AlarmNames=alarms_to_disable)

    def delete(self):
        alarms = self.get_alarms()
        self.delete_alarms(list(alarms.keys()))


def main():
    config = read_alarm_config()
    client = AlarmClient(config)
    run_mode = os.getenv("RUN_MODE", MODE_CREATE)
    if run_mode == MODE_CREATE:
        logger.info("Running script in create mode")
        client.create()
    elif run_mode == MODE_DISABLE:
        logger.info("Running script in disable mode")
        client.disable()
    elif run_mode == MODE_DELETE:
        logger.info("Running script in delete mode")
        client.delete()
    else:
        logger.error("Invalid run mode %s", run_mode)


if __name__ == "__main__":
    main()
