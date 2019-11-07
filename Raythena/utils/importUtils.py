import importlib


def import_from_string(modulepath: str):
    module, _, instance = modulepath.partition(':')
    module = importlib.import_module(module)

    for elt in instance.split('.'):
        if not hasattr(module, elt):
            raise ImportError(f"Can't import {elt} from {module}")
        module = getattr(module, elt)
    return module
