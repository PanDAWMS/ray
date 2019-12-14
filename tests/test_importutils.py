import pytest
from Raythena.utils.importUtils import import_from_string


def test_importutils():

    errors_string = [
        "unknown",
        "unknown:unknown",
        "unknown:",
        "Raythena.drivers.esdriver:ESDriver.unknown"
    ]
    for s in errors_string:
        with pytest.raises(ImportError):
            import_from_string(s)

    with pytest.raises(ValueError):
        import_from_string(":unknown")
    from Raythena.drivers.esdriver import ESDriver
    assert import_from_string("Raythena.drivers.esdriver:ESDriver") == ESDriver
