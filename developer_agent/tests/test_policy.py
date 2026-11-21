import unittest
from agent import validate_repo_path, MAX_CHANGED_FILES, MAX_FILE_BYTES

class PathPolicyTests(unittest.TestCase):
    def test_accepts_relative_path(self):
        self.assertEqual(validate_repo_path('app/src/Main.kt'), 'app/src/Main.kt')
    def test_rejects_traversal(self):
        for path in ('/etc/passwd', '../secret', 'a/../secret', '.'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                validate_repo_path(path)
    def test_rejects_sensitive_paths(self):
        for path in ('.github/workflows/ci.yml', '.env', 'config/.env.production', '.git/config'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                validate_repo_path(path)
    def test_limits(self):
        self.assertEqual(MAX_CHANGED_FILES, 12)
        self.assertEqual(MAX_FILE_BYTES, 180000)

if __name__ == '__main__':
    unittest.main()
