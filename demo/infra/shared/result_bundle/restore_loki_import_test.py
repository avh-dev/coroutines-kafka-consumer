from __future__ import annotations

import importlib.util
import io
import unittest
import urllib.error
from pathlib import Path
from unittest import mock


SOURCE = Path(__file__).resolve().parent / "restore/import-loki.py"
SPEC = importlib.util.spec_from_file_location("restore_import_loki", SOURCE)
assert SPEC and SPEC.loader
IMPORT_LOKI = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(IMPORT_LOKI)


class Response:
    status = 204

    def __enter__(self) -> "Response":
        return self

    def __exit__(self, *_args: object) -> None:
        pass


class RestoreLokiImportTest(unittest.TestCase):
    @mock.patch.object(IMPORT_LOKI.time, "sleep")
    @mock.patch.object(IMPORT_LOKI.urllib.request, "urlopen")
    def test_retries_rate_limited_batch(self, urlopen: mock.Mock, sleep: mock.Mock) -> None:
        rate_limited = urllib.error.HTTPError(
            "http://loki/loki/api/v1/push",
            429,
            "Too Many Requests",
            {"Retry-After": "0.25"},
            io.BytesIO(b"rate limited"),
        )
        urlopen.side_effect = [rate_limited, Response()]

        IMPORT_LOKI.push(
            "http://loki",
            {(("app", "load-test"),): [["123", "message"]]},
        )

        self.assertEqual(2, urlopen.call_count)
        sleep.assert_called_once_with(0.25)

    @mock.patch.object(IMPORT_LOKI.time, "sleep")
    @mock.patch.object(IMPORT_LOKI.urllib.request, "urlopen")
    def test_stops_after_bounded_rate_limit_retries(
        self,
        urlopen: mock.Mock,
        sleep: mock.Mock,
    ) -> None:
        def rate_limited(*_args: object, **_kwargs: object) -> None:
            raise urllib.error.HTTPError(
                "http://loki/loki/api/v1/push",
                429,
                "Too Many Requests",
                {},
                None,
            )

        urlopen.side_effect = rate_limited

        with self.assertRaises(urllib.error.HTTPError):
            IMPORT_LOKI.push(
                "http://loki",
                {(("app", "load-test"),): [["123", "message"]]},
                max_attempts=3,
            )

        self.assertEqual(3, urlopen.call_count)
        self.assertEqual([mock.call(1.0), mock.call(2.0)], sleep.call_args_list)


if __name__ == "__main__":
    unittest.main()
