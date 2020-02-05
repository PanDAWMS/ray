import os

import yaml


class Config(object):
    """Class storing app configuration.

    This class will store configuration by prioritizing in the following order:
    cli arguments > environment variables > configuration file
    Note that not all arguments can be specified using cli or env variable, some of them can only be specified from
    the conf file. See the file <raythena.py> for more information about which settings can be specified using cli. Any
    parameter can be specified in the config file, the only constraint checked being that
    attributes in Config.required_conf_settings should be present in the config file. This allows to specify
    custom settings for plugins if necessary.
    """

    required_conf_settings = {
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
            'harvesterconf': str
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

    def __init__(self, config_path: str, *args, **kwargs) -> None:
        """Parse the config file to an object

        Read the yaml configuration file specified by 'config_path', **kwargs will be used to override settings
        present in the configuration files and should contains cli / environment variables arguements,
        with cli arguments already overriding environment variable arguments.

        Args:
            config_path: path to the configuration file
            *args: unused
            **kwargs: config from cli / environment variables
        """

        del args
        self.config_path = config_path
        # parse.config file
        if not self.config_path or not os.path.isfile(self.config_path):
            raise Exception(f"Could not find config file {self.config_path}")

        with open(self.config_path) as f:
            file_conf = yaml.safe_load(f)
            for k, v in file_conf.items():
                setattr(self, k, v)
        self._validate()
        self._parse_cli_args(**kwargs)

    def __str__(self):
        """String repr of config object

        Returns:
            string repr of config object
        """
        return str(self.__dict__)

    def _parse_cli_args(self, config: str, debug: bool, payload_bindir: str,
                        ray_driver: str, ray_head_ip: str,
                        ray_redis_password: str, ray_redis_port: str,
                        ray_workdir: str, harvester_endpoint: str,
                        panda_queue: str, core_per_node: int) -> None:
        """
        Overrides config settings with settings specified via cli / env vars

        Args:
            config: path to config file
            debug: debug log level. Overrides logging.level
            payload_bindir: directory to the payload used by worker. Overrides payload.bindir
            ray_driver: driver class using form path.to.module:DriverClass. Overrides ray.driver
            ray_head_ip: ray cluster head ip. Overrides ray.headip
            ray_redis_password: ray cluster password. Overrides ray.redispassword
            ray_redis_port: ray cluster port. Overrides ray.redisport
            ray_workdir: Base raythena workdir. Overrides ray.workdir
            harvester_endpoint: Directory used by harvester shared file messaging. Overrides harvester.endpoint
            panda_queue: Panda queue from which harvester is retrieving jobs. Overrides payload.pandaqueue
            core_per_node: Number of cores used by the payload. This is only user to determine event ranges cache size
            on each ray actor. The actual number of processes used by AthenaMP is defined in the job specification.
            Overrides resources.corepernode

        Returns:
            None
        """
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

    def _validate_section(self, template_section_name: str,
                          section_params: dict, template_params: dict) -> None:
        """
        Validate one section of the config file

        Args:
            template_section_name: current config section being validated
            section_params: keys-values of current section from the config file
            template_params: key-values of current section used to match section_params

        Returns:
            None

        Raises:
            Exception: Invalid configuration file
        """
        for name, value in template_params.items():
            if name not in section_params.keys():
                raise Exception(
                    f"Param '{name}' not found in conf section '{template_section_name}'"
                )
            if isinstance(value, dict):
                self._validate_section(f"{template_section_name}.{name}",
                                       section_params.get(name), value)

    def _validate(self) -> None:
        """
        Validate the config file by checking that all attributes in required_conf_settings exist in the config file.

        Returns:
            None

        Raises:
            Exception: config file is invalid
        """
        # validate pilot section
        for template_section, template_params in Config.required_conf_settings.items(
        ):
            section_params = getattr(self, template_section, None)
            if section_params is None:
                raise Exception(
                    f"Malformed configuration file: section '{template_section}' not found"
                )
            self._validate_section(template_section, section_params,
                                   template_params)
