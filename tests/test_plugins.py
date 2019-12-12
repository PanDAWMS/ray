import pytest
from Raythena.utils.plugins import PluginsRegistry
from Raythena.utils.exception import PluginNotFound


class TestPlugins:

    def test_package_namespace(self):
        import Raythena.actors.payloads as payloads
        import Raythena.actors.payloads.eventservice as payloads_es

        plugins = PluginsRegistry()

        # Checks that plugin module was registered after init
        assert payloads.basePayload.__name__ in plugins.plugins
        assert payloads_es.pilot2http.__name__ in plugins.plugins
        assert payloads.__name__ not in plugins.plugins
        assert payloads_es.__name__ not in plugins.plugins

    def test_add_namespace(self):
        import Raythena.drivers as drivers
        plugins = PluginsRegistry()
        plugins.add_plugin_namespace(drivers)
        assert drivers.__name__ not in plugins.plugins
        assert drivers.esdriver.__name__ in plugins.plugins
        assert drivers.communicators.__name__ not in plugins.plugins
        assert drivers.communicators.harvesterFileMessenger.__name__ in plugins.plugins

        plugins = PluginsRegistry()
        plugins.add_plugin_namespace(drivers, recursive=False)
        assert drivers.__name__ not in plugins.plugins
        assert drivers.esdriver.__name__ in plugins.plugins
        assert drivers.communicators.__name__ not in plugins.plugins
        assert drivers.communicators.harvesterFileMessenger.__name__ not in plugins.plugins

    def test_get_plugins(self, config):
        import Raythena.actors.payloads.eventservice as payloads_es
        plugins = PluginsRegistry()

        with pytest.raises(ValueError):
            assert plugins.get_plugin("unknown")

        plugin_str = config.payload['plugin']
        assert plugin_str
        module, sep, plugin_class = plugin_str.partition(":")
        print(f"Module: {module}, class: {plugin_class}")
        with pytest.raises(ValueError):
            plugins.get_plugin(module)

        with pytest.raises(ValueError):
            plugins.get_plugin(plugin_class)

        with pytest.raises(PluginNotFound):
            plugins.get_plugin(f"{plugin_str}_donotexists")

        with pytest.raises(PluginNotFound):
            plugins.get_plugin(f"donotexists.{plugin_str}")

        with pytest.raises(PluginNotFound):
            plugins.get_plugin(f"{module}.donotexists:{plugin_class}")

        plugin_str_full = f"{payloads_es.__name__}.{plugin_str}"
        pilot_http_plugin = plugins.get_plugin(plugin_str_full)

        assert pilot_http_plugin is not None
        assert issubclass(pilot_http_plugin, payloads_es.pilot2http.Pilot2HttpPayload)
        assert issubclass(pilot_http_plugin, payloads_es.esPayload.ESPayload)

        pilot_http_plugin = plugins.get_plugin(plugin_str)
        assert pilot_http_plugin is not None
        assert issubclass(pilot_http_plugin, payloads_es.pilot2http.Pilot2HttpPayload)
        assert issubclass(pilot_http_plugin, payloads_es.esPayload.ESPayload)
