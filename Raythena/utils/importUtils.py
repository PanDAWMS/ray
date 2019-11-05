import importlib
import logging

logger = logging.getLogger(__name__)


def import_from_string(modulepath: str):
    module, _, instance = modulepath.partition(':')
    module = importlib.import_module(module)

    for elt in instance.split('.'):
        if not hasattr(module, elt):
            logger.critical(f"Could not import module {modulepath}. {module.__name__} has not elt {elt}")
        module = getattr(module, elt)
    return module
