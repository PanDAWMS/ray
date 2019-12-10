import importlib
import pkgutil

import Raythena.actors.payloads


class PluginsRegistry:

    def __init__(self):

        self.plugins = dict()
        # adds preconfigured plugins packages
        self.add_plugin_namespace(Raythena.actors.payloads)

    def add_plugin_namespace(self, namespace, recursive=True):

        walk = pkgutil.walk_packages
        if not recursive:
            walk = pkgutil.iter_modules

        self.plugins.update(
            {
                name: importlib.import_module(name) for finder, name, ispkg in walk(namespace.__path__, namespace.__name__ + ".") if not ispkg
            }
        )

    def get_plugin(self, plugin_name):
        plugin_module, _, plugin_class = plugin_name.partition(":")

        if not plugin_class:
            raise ValueError("plugin name should be formatted as <plugin.module:PluginClass>")

        for name, plugin in self.plugins.items():
            if name.endswith(plugin_module):
                if not hasattr(plugin, plugin_class):
                    raise ImportError(f"Can't import plugin {plugin_class} from {plugin.__name__}")
                return getattr(plugin, plugin_class)

        raise ImportError(f"Module {plugin_module} not found")
