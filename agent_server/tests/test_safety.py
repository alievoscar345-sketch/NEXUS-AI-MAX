import ipaddress
import unittest
from app.main import safe_url

class SafetyTests(unittest.TestCase):
    def test_blocks_http(self):
        with self.assertRaises(ValueError):
            safe_url("http://example.com")

    def test_blocks_localhost(self):
        with self.assertRaises(ValueError):
            safe_url("https://localhost/")

    def test_blocks_private_ip(self):
        with self.assertRaises(ValueError):
            safe_url("https://127.0.0.1/")

if __name__ == "__main__":
    unittest.main()
