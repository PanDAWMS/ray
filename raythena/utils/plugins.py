import importlib
import pkgutil
from typing import Callable

import raythena.drivers
import raythena.actors.payloads
import raythena.drivers.communicators
from raythena.utils.exception import PluginNotFound


class PluginsRegistry(object):
    """
    Perform plugin discovery using the 'namespace packages' convention allowing plugins to be installed using pip.
    See https://packaging.python.org/guides/creating-and-discovering-plugins/#using-namespace-packages for more details.

    """

    def __init__(self) -> None:
        """
        Initialize plugin search space with default packages
        """
        self.plugins = dict()
        # adds preconfigured plugins packages
        self.add_plugin_namespace(raythena.drivers)
        self.add_plugin_namespace(raythena.actors.payloads)
        self.add_plugin_namespace(raythena.drivers.communicators)

    def add_plugin_namespace(self,
                             plugin_package: object,
                             recursive: bool = True) -> None:
        """
        Add python modules present in the specified package to the plugin search space

        Args:
            plugin_package: the python package from which modules should be added
            recursive: If True, will also add modules from sub-packages, otherwise only add modules in plugin_package

        Returns:
            None
        """
        walk = pkgutil.walk_packages
        if not recursive:
            walk = pkgutil.iter_modules

        self.plugins.update({
            name: importlib.import_module(name) for finder, name, ispkg in walk(
                plugin_package.__path__, plugin_package.__name__ + ".") if not ispkg
        })

    def get_plugin(self, plugin_name: str) -> Callable:
        """
        Tries to load the class specified by 'plugin_name'. The package is specified
        using the form: path.to.module:PluginClass
        Only the end of plugin module path needs to be specified, the previous example can also be specified using
        module:PluginClass

        Args:
            plugin_name: Plugin class to load

        Returns:
            The class specified by plugin_name

        Raises:
            PluginNotFound if the specified plugin was not found in the search space
        """
        plugin_module, _, plugin_class = plugin_name.partition(":")

        if not plugin_class:
            raise ValueError(
                "plugin name should be formatted as <plugin.module:PluginClass>"
            )

        for name, plugin in self.plugins.items():
            if name.endswith(plugin_module):
                if not hasattr(plugin, plugin_class):
                    raise PluginNotFound(
                        worker_id="tests",
                        message=f"Can't import plugin {plugin_class} from {plugin.__name__}"
                    )
                return getattr(plugin, plugin_class)

        raise PluginNotFound(worker_id="tests",
                             message=f"Module {plugin_module} not found")

if __name__ == "__main__":
    p = PluginsRegistry()
    print(p.plugins)