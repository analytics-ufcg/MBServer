import ConfigParser
import json
import os
import requests
import sys
import uuid

class BrokerClient(object):
    def __init__(self, config):
        self.app_id = None
        self.plugin = config.get('manager', 'plugin')
        self.ip = config.get('manager', 'ip')
        self.port = config.get('manager', 'port')
        self.cluster_size = config.getint('manager', 'cluster_size')
        self.flavor_id = config.get('manager', 'flavor_id')
        self.image_id = config.get('manager', 'image_id')
        self.bigsea_username = config.get('manager', 'bigsea_username')
        self.bigsea_password = config.get('manager', 'bigsea_password')
        self.opportunistic = config.get('plugin', 'opportunistic')
        self.dependencies = config.get('plugin', 'dependencies')
        self.args = config.get('plugin', 'args').split()
        self.main_class = config.get('plugin', 'main_class')
        self.job_template_name = config.get('plugin', 'job_template_name')
        self.job_binary_name = config.get('plugin', 'job_binary_name')
        self.job_binary_url = config.get('plugin', 'job_binary_url')
        self.input_ds_id = ''
        self.output_ds_id = ''
        self.plugin_app = config.get('plugin', 'plugin_app')
        self.expected_time = config.getint('plugin', 'expected_time')
        self.number_of_jobs = config.getint('plugin', 'number_of_jobs')
        self.collect_period = config.getint('plugin', 'collect_period')
        self.openstack_plugin = config.get('plugin', 'openstack_plugin')
        self.job_type = config.get('plugin', 'job_type')
        self.version = '2.1.0'
        self.cluster_id = config.get('plugin', 'cluster_id')
        self.slave_ng = config.get('plugin', 'slave_ng')
        self.opportunistic_slave_ng = config.get('plugin', 'opportunistic_slave_ng')
        self.master_ng = config.get('plugin', 'master_ng')
        self.net_id = config.get('plugin', 'net_id')
        self.actuator = config.get('scaler', 'actuator')
        self.starting_cap = config.get('scaler', 'starting_cap')
        self.scaler_plugin = config.get('scaler', 'scaler_plugin')
        self.scaling_parameters = {}
        self.scaling_parameters['actuator'] = config.get('scaler', 'actuator')
        self.scaling_parameters['metric_source'] = config.get('scaler', 'metric_source')
        self.scaling_parameters['application_type'] = config.get('scaler', 'application_type')
        self.scaling_parameters['check_interval'] = config.getint('scaler', 'check_interval')
        self.scaling_parameters['trigger_down'] = config.getint('scaler', 'trigger_down')
        self.scaling_parameters['trigger_up'] = config.getint('scaler', 'trigger_up')
        self.scaling_parameters['min_cap'] = config.getint('scaler', 'min_cap')
        self.scaling_parameters['max_cap'] = config.getint('scaler', 'max_cap')
        self.scaling_parameters['actuation_size'] = config.getint('scaler', 'actuation_size')
        self.scaling_parameters['metric_rounding'] = config.getint('scaler', 'metric_rounding')

    def execute_application(self):
        headers = {'Content-Type': 'application/json'}
        body = dict(plugin=self.plugin, scaler_plugin=self.scaler_plugin, scaling_parameters=self.scaling_parameters, cluster_size=self.cluster_size, starting_cap=self.starting_cap, actuator=self.actuator, flavor_id=self.flavor_id, image_id=self.image_id, opportunistic=self.opportunistic, args=self.args, main_class=self.main_class, job_template_name=self.job_template_name, job_binary_name=self.job_binary_name, job_binary_url=self.job_binary_url, input_ds_id=self.input_ds_id, output_ds_id=self.output_ds_id, plugin_app=self.plugin_app, expected_time=self.expected_time, number_of_jobs=self.number_of_jobs, collect_period=self.collect_period, bigsea_username=self.bigsea_username, bigsea_password=self.bigsea_password, openstack_plugin=self.openstack_plugin, job_type=self.job_type, version=self.version, opportunistic_slave_ng=self.opportunistic_slave_ng, slave_ng=self.slave_ng, master_ng=self.master_ng, net_id=self.net_id, dependencies=self.dependencies)

        url = "http://%s:%s/manager/execute" % (self.ip, self.port)
        r = requests.post(url, headers=headers, data=json.dumps(body))
        self.app_id =  r.content

        return self.app_id

    def get_execution_log(self):
        if self.app_id == None:
            return None

        url_execution_log = "http://%s:%s/manager/logs/execution/%s" % (self.ip,
                                                                        self.port,
                                                                        self.app_id)
        r = requests.get(url_execution_log).json()
        return r

    def get_std_log(self):
        if self.app_id == None:
            return None

        url_std_log = "http://%s:%s/manager/logs/std/%s" % (self.ip,
                                                            self.port,
                                                            self.app_id)
        r = requests.get(url_std_log).json()
        return r


if __name__ == "__main__":
    config = ConfigParser.RawConfigParser()
    __file__ = os.path.join(sys.path[0], sys.argv[1])
    config.read(__file__)

    client = BrokerClient(config)
    client.execute_application()