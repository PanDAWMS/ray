#!/usr/bin/env python
import yaml


class Config(object):
    def __init__(self, configFile):
        with open(configFile, 'r') as stream:
            try:
                self.config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    def get_property(self, property_name):
        if property_name not in self.config.keys():  # we don't want KeyError
            return None  # just return None if not found
        return self.config[property_name]


class EventServiceConfig(Config):

    @property
    def config_name(self):
        return self.get_property('config_name')

    @property
    def number_of_actors(self):
        return self.get_property('number_of_actors')

    @property
    def extra_nodes(self):
        return self.get_property('extra_nodes')

    @property
    def cores_per_actor(self):
        return self.get_property('cores_per_actor')

    @property
    def merge_event_size(self):
        return self.get_property('merge_event_size')

    @property
    def max_retries(self):
        return self.get_property('max_retries')

    @property
    def max_ranges(self):
        return self.get_property('max_ranges')

    @property
    def yampl_communication_channel(self):
        return self.get_property('yampl_communication_channel')

    @property
    def run_dir(self):
        return self.get_property('run_dir')

    @property
    def merge_dir(self):
        return self.get_property('merge_dir')

    @property
    def asetup(self):
        return self.get_property('asetup')

    @property
    def geometry_version(self):
        return self.get_property('geometry_version')

    @property
    def physics_list(self):
        return self.get_property('physics_list')

    @property
    def conditions_tag(self):
        return self.get_property('conditions_tag')

    @property
    def proxy_evnt_file(self):
        return self.get_property('proxy_evnt_file')

    @property
    def extra_setup_commands(self):
        return self.get_property('extra_setup_commands')

    @property
    def extra_pre_exec(self):
        return self.get_property('extra_pre_exec')
