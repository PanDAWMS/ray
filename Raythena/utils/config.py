
import yaml
import os


class Config:

    conf_template = {
        'payload': {
            'plugin': str,
            "bindir": str,
            "pandaqueue": str,
            "logfilename": str,
            "extrasetup": str,
            "hpcresource": str,
            "extrapostpayload": str,
            "containerengine": str,
            "containerextraargs": str
        },
        'harvester': {
            'endpoint': str,
            'communicator': str,
            'harvesterconf': str,
            'min_nevents': int
        },
        'ray': {
            'workdir': str,
            'headip': str,
            'redisport': int,
            'redispassword': str,
            'driver': str
        },
        'resources': {
            'corepernode': int,
            'workerpernode': int,
        },
        'logging': {
            'level': str,
            'logfile': str
        }
    }

    def __init__(self, configpath, *args, **kwargs):

        self.configpath = configpath
        # parse.config file
        if not self.configpath or not os.path.isfile(self.configpath):
            raise Exception(f"Could not find config file {self.configpath}")

        with open(self.configpath) as f:
            file_conf = yaml.safe_load(f)
            for k, v in file_conf.items():
                setattr(self, k, v)
        self._validate()
        self._parse_cli_args(**kwargs)

    def __str__(self):
        return str(self.__dict__)

    def _parse_cli_args(self, config, debug, payload_bindir, ray_driver, ray_head_ip,
                        ray_redis_password, ray_redis_port, ray_workdir, harvester_endpoint, panda_queue, core_per_node):

        if debug:
            self.logging['level'] = 'debug'
        if payload_bindir:
            self.payload['bindir'] = payload_bindir
        if ray_driver:
            self.ray['driver'] = ray_driver
        if ray_head_ip:
            self.ray['headip'] = ray_head_ip
        if ray_redis_port:
            self.ray['redispassword'] = ray_redis_password
        if ray_redis_port:
            self.ray['redisport'] = ray_redis_port
        if ray_workdir:
            self.ray['workdir'] = ray_workdir
        if harvester_endpoint:
            self.harvester['endpoint'] = harvester_endpoint
        if panda_queue:
            self.payload['pandaqueue'] = panda_queue
        if core_per_node:
            self.resources['corepernode'] = int(core_per_node)

    def _validate_section(self, template_section_name, section_params, template_params):
        for name, value in template_params.items():
            if name not in section_params.keys():
                raise Exception(f"Param '{name}' not found in conf section '{template_section_name}'")
            if isinstance(value, dict):
                self._validate_section(f"{template_section_name}.{name}", section_params.get(name), value)

    def _validate(self):
        # validate pilot section
        for template_section, template_params in Config.conf_template.items():
            section_params = getattr(self, template_section, None)
            if section_params is None:
                raise Exception(f"Malformed configuration file: section '{template_section}' not found")
            self._validate_section(template_section, section_params, template_params)
