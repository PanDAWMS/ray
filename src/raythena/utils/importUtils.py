import importlib
from typing import Callable


def import_from_string(module_path: str) -> Callable:
    """
    Tries to import the class specified in argument and returns it. Class should be specified using the form:
    full.path.to.module:ModuleClass

    Args:
        module_path: The class to load

    Returns:
        The class specified by the module_path

    Raises:
        ImportError if the specified class couldn't be found
    """
    module, _, instance = module_path.partition(":")
    module = importlib.import_module(module)

    for elt in instance.split("."):
        if not hasattr(module, elt):
            raise ImportError(f"Can't import {elt} from {module}")
        module = getattr(module, elt)
    return module
