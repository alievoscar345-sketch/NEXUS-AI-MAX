import base64
import unittest

from fastapi import HTTPException

from app.main import validate_chat_messages


class ChatValidationTests(unittest.TestCase):
    def test_accepts_text_only_chat(self):
        self.assertFalse(validate_chat_messages([
            {"role": "user", "content": "Summarize this document"}
        ]))

    def test_accepts_supported_image_data_url(self):
        encoded = base64.b64encode(b"test-image-bytes").decode("ascii")
        self.assertTrue(validate_chat_messages([
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe this image"},
                    {"type": "image_url", "image_url": {
                        "url": "data:image/jpeg;base64," + encoded
                    }},
                ],
            }
        ]))

    def test_rejects_unsupported_image_mime(self):
        encoded = base64.b64encode(b"test-image-bytes").decode("ascii")
        with self.assertRaises(HTTPException) as raised:
            validate_chat_messages([
                {"role": "user", "content": [
                    {"type": "image_url", "image_url": {
                        "url": "data:image/heic;base64," + encoded
                    }}
                ]}
            ])
        self.assertEqual(422, raised.exception.status_code)

    def test_rejects_invalid_image_base64(self):
        with self.assertRaises(HTTPException) as raised:
            validate_chat_messages([
                {"role": "user", "content": [
                    {"type": "image_url", "image_url": {
                        "url": "data:image/png;base64,not-valid-base64!"
                    }}
                ]}
            ])
        self.assertEqual(422, raised.exception.status_code)

    def test_rejects_oversized_multimodal_text_part(self):
        with self.assertRaises(HTTPException) as raised:
            validate_chat_messages([
                {"role": "user", "content": [
                    {"type": "text", "text": "x" * 200_001}
                ]}
            ])
        self.assertEqual(422, raised.exception.status_code)


if __name__ == "__main__":
    unittest.main()
