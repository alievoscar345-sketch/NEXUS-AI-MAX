import unittest
from agent import parse_tool_arguments

class ToolArgumentParsingTests(unittest.TestCase):
    def test_parses_normal_json(self):
        self.assertEqual(parse_tool_arguments('{"path":"src"}'), {"path":"src"})

    def test_parses_dict_from_provider(self):
        self.assertEqual(parse_tool_arguments({"path":"src"}), {"path":"src"})

    def test_parses_json_with_surrounding_text(self):
        self.assertEqual(parse_tool_arguments('arguments: {"path":"src"} trailing metadata'), {"path":"src"})

    def test_rejects_non_object(self):
        for value in ("not json", "[]", "null", 42):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_tool_arguments(value)

if __name__ == "__main__":
    unittest.main()
