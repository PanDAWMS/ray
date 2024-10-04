import pytest
from raythena.utils.importUtils import import_from_string


def test_importutils():
    errors_string = [
        "unknown", "unknown:unknown", "unknown:",
        "raythena.drivers.esdriver:ESDriver.unknown"
    ]
    for s in errors_string:
        with pytest.raises(ImportError):
            import_from_string(s)

    with pytest.raises(ValueError):
        import_from_string(":unknown")
    from raythena.drivers.esdriver import ESDriver
    assert import_from_string("raythena.drivers.esdriver:ESDriver") == ESDriver
