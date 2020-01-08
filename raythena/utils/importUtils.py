import importlib
from typing import Callable


def import_from_string(module_path: str) -> Callable:
    module, _, instance = module_path.partition(':')
    module = importlib.import_module(module)

    for elt in instance.split('.'):
        if not hasattr(module, elt):
            raise ImportError(f"Can't import {elt} from {module}")
        module = getattr(module, elt)
    return module
