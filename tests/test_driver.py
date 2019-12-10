from Raythena.drivers.esdriver import ESDriver


class TestDriver:

    def test_one(self, tmpdir):
        print(f"Temp dir: {tmpdir}")
        assert True

    def test_two(self):
        assert not False
