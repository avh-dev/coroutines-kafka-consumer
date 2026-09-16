from __future__ import annotations

import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
AUDIT_CONFIGS = (
    REPO_ROOT / "demo/ckc-demo/src/main/resources/logback-spring.xml",
    REPO_ROOT / "demo/ckc-demo-load-test/src/main/resources/logback.xml",
)


class AuditAppenderConfigTest(unittest.TestCase):
    def test_producer_and_consumer_use_the_same_expanded_audit_buffers(self) -> None:
        settings = []
        for path in AUDIT_CONFIGS:
            root = ET.parse(path).getroot()
            appender = root.find(".//appender[@name='AUDIT_TCP']")
            self.assertIsNotNone(appender, path)
            settings.append({
                "ring_buffer_events": int(appender.findtext("ringBufferSize", "0")),
                "write_buffer_bytes": int(appender.findtext("writeBufferSize", "0")),
            })

        self.assertEqual(settings[0], settings[1])
        self.assertEqual(
            {"ring_buffer_events": 65536, "write_buffer_bytes": 65536},
            settings[0],
        )


if __name__ == "__main__":
    unittest.main()
