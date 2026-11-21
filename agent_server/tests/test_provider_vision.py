import base64
import struct
import tempfile
import unittest
import zlib
from pathlib import Path
from unittest.mock import patch

import httpx

from app import main
from app.model_gateway import ModelConfig, ModelResult


class FakeVisionGateway:
    def __init__(self, response):
        self.response = response
        self.messages = None

    async def complete_messages(self, messages, purpose="general"):
        self.messages = messages
        return ModelResult(
            content=self.response,
            provider_id="user-anymodel",
            model="vision-test-model",
            usage={},
        )


class ProviderVisionTests(unittest.IsolatedAsyncioTestCase):
    def test_vision_probe_is_a_valid_solid_green_png(self):
        data_url = main._vision_test_data_url()
        self.assertTrue(data_url.startswith("data:image/png;base64,"))
        png = base64.b64decode(data_url.split(",", 1)[1])
        self.assertEqual(png[:8], b"\x89PNG\r\n\x1a\n")

        offset = 8
        image_data = b""
        while offset < len(png):
            length = struct.unpack(">I", png[offset:offset + 4])[0]
            kind = png[offset + 4:offset + 8]
            payload = png[offset + 8:offset + 8 + length]
            self.assertEqual(
                zlib.crc32(kind + payload) & 0xFFFFFFFF,
                struct.unpack(">I", png[offset + 8 + length:offset + 12 + length])[0],
            )
            if kind == b"IHDR":
                width, height, bit_depth, color_type = struct.unpack(">IIBB", payload[:10])
                self.assertEqual((width, height, bit_depth, color_type), (16, 16, 8, 2))
            elif kind == b"IDAT":
                image_data += payload
            offset += 12 + length

        decoded = zlib.decompress(image_data)
        self.assertEqual(len(decoded), 16 * (1 + 16 * 3))
        for row in range(16):
            start = row * 49
            self.assertEqual(decoded[start], 0)
            self.assertEqual(decoded[start + 1:start + 49], b"\x00\xff\x00" * 16)

    async def _call_probe(self, response_text):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        db_path = Path(temp.name) / "nexus.sqlite3"
        model = ModelConfig(
            id="user-anymodel",
            base_url="https://provider.example/v1",
            model="vision-test-model",
            api_key="not-a-real-key",
        )
        fake_gateway = FakeVisionGateway(response_text)
        with patch.object(main, "DB_PATH", db_path), \
             patch.object(main, "API_KEY", "test-key"), \
             patch.object(main, "provider_models_for_owner", return_value=[model]), \
             patch.object(main, "ModelGateway", return_value=fake_gateway):
            main.init_db()
            with main.db() as conn:
                conn.execute(
                    """INSERT INTO provider_credentials(
                        owner_id,provider_id,base_url,model,encrypted_api_key,
                        created_at,updated_at,supports_vision
                    ) VALUES(?,?,?,?,?,?,?,0)""",
                    ("personal", "anymodel", model.base_url, model.model, b"encrypted", 1.0, 1.0),
                )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=main.app),
                base_url="http://nexus.test",
            ) as client:
                response = await client.post(
                    "/v1/providers/anymodel/test-vision",
                    headers={"Authorization": "Bearer test-key"},
                )
            with main.db() as conn:
                enabled = conn.execute(
                    "SELECT supports_vision FROM provider_credentials WHERE owner_id='personal' AND provider_id='anymodel'"
                ).fetchone()["supports_vision"]
        return response, enabled, fake_gateway

    async def test_probe_confirms_and_enables_vision_only_after_green_response(self):
        response, enabled, gateway = await self._call_probe("Green")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["supports_vision"])
        self.assertEqual(enabled, 1)
        self.assertIn("image_url", [part["type"] for part in gateway.messages[0]["content"]])
        self.assertEqual(gateway.messages[0]["content"][1]["image_url"]["url"][:22], "data:image/png;base64,")

    async def test_probe_does_not_enable_vision_when_color_is_not_identified(self):
        response, enabled, _ = await self._call_probe("I cannot identify the color.")
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["supports_vision"])
        self.assertEqual(response.json()["status"], "vision_not_confirmed")
        self.assertEqual(enabled, 0)


if __name__ == "__main__":
    unittest.main()
