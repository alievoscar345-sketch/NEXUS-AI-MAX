import base64
import unittest

from fastapi import HTTPException

from app.main import ChatCompletionInput, ProviderCredentialInput, validate_chat_messages


class ChatValidationTests(unittest.TestCase):
    def test_missing_vision_provider_error_is_actionable(self):
        from app.main import _chat_gateway_error_message
        from app.model_gateway import ModelGatewayError

        message = _chat_gateway_error_message(
            ModelGatewayError("No configured model supports purpose: general with capability vision"),
            requires_vision=True,
        )

        self.assertIn("provider is configured for image input", message)
        self.assertIn("image-understanding test", message)

    def test_text_chat_keeps_original_provider_error(self):
        from app.main import _chat_gateway_error_message
        from app.model_gateway import ModelGatewayError

        error = ModelGatewayError("All eligible model providers failed (HTTP 401)")
        self.assertEqual(_chat_gateway_error_message(error, requires_vision=False), str(error))

    def test_provider_context_window_has_safe_default_and_bounds(self):
        base = {
            "provider_id": "test-provider",
            "base_url": "https://anymodel.org/v1",
            "model": "test-model",
        }
        self.assertEqual(ProviderCredentialInput(**base).context_window_tokens, 32768)
        with self.assertRaises(Exception):
            ProviderCredentialInput(**{**base, "context_window_tokens": 2048})
        with self.assertRaises(Exception):
            ProviderCredentialInput(**{**base, "context_window_tokens": 3000000})

    def test_chat_request_accepts_extended_history(self):
        request = ChatCompletionInput(messages=[
            {"role": "user", "content": f"message-{index}"} for index in range(2000)
        ])
        self.assertEqual(len(request.messages), 2000)

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
