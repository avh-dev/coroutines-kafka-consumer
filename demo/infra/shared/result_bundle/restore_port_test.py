from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SOURCE = Path(__file__).resolve().parent / "restore/select_port.py"
SPEC = importlib.util.spec_from_file_location("restore_select_port", SOURCE)
assert SPEC and SPEC.loader
SELECT_PORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SELECT_PORT)


class RestorePortSelectionTest(unittest.TestCase):
    def test_automatic_selection_skips_busy_and_reserved_ports(self) -> None:
        available = lambda _address, port: port not in {3002, 3004}

        self.assertEqual(
            3005,
            SELECT_PORT.select_port(
                "127.0.0.1",
                3002,
                explicit=False,
                excluded={3003},
                available=available,
            ),
        )

    def test_explicit_free_port_is_preserved(self) -> None:
        self.assertEqual(
            3200,
            SELECT_PORT.select_port(
                "127.0.0.1",
                3200,
                explicit=True,
                available=lambda _address, _port: True,
            ),
        )

    def test_explicit_busy_port_fails(self) -> None:
        with self.assertRaisesRegex(SELECT_PORT.PortSelectionError, "already in use"):
            SELECT_PORT.select_port(
                "127.0.0.1",
                3200,
                explicit=True,
                available=lambda _address, _port: False,
            )

    def test_port_range_is_validated(self) -> None:
        for value in ("not-a-port", "0", "65536"):
            with self.subTest(value=value):
                with self.assertRaises(SELECT_PORT.PortSelectionError):
                    SELECT_PORT.parse_port(value)


if __name__ == "__main__":
    unittest.main()
