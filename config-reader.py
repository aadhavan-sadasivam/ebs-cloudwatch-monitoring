import configparser

ALARM_CONFIG_PATH = "ebs-alarm.config"

SECTIONS = ["default", "ReadLatency", "WriteLatency", "ImpairedVolume"]

CONFIG_KEYS = {
    "EvaluationPeriods": "evaluation_periods",
    "DatapointsToAlarm": "datapoint_to_alarm",
    "Threshold": "threshold",
    "ComparisonOperator": "comparision_operator",
    "TreatMissingData": "treat_missing_data",
}

DEFAULTS = {
    "evaluation_periods": 5,
    "datapoint_to_alarm": 5,
    "threshold": 100,
    "comparision_operator": "GreaterThanOrEqualToThreshold",
    "treat_missing_data": "missing",
}


def read_alarm_config():
    config = configparser.ConfigParser()
    config.read(ALARM_CONFIG_PATH)
    config_dict = {}
    for section in SECTIONS:
        config_dict[section] = {}
        for key, value in config[section].items():
            config_dict[section][key] = value
    return config_dict


def read_config(config, config_name):
    if config_name in config:
        return config.get(config_name)
    else:
        return DEFAULTS.get(config_name)
