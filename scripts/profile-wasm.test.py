import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("profile_wasm", Path(__file__).with_name("profile-wasm.py"))
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


class ProfileTests(unittest.TestCase):
    def test_boundaries_duplicate_images_and_unknown_frames(self):
        sidecar = {"guest_sha256": "one", "functions": {"2": "JS_Call", "3": "malloc"}}
        record = {"guest_sha256": "one", "pid": 1, "text_base": 4096, "text_length": 128,
                  "functions": [{"index": 2, "offset": 16, "length": 16},
                                {"index": 3, "offset": 48, "length": 8}]}
        symbols = profile.Symbols(sidecar, [record, record])
        self.assertEqual(symbols.resolve(4112), ("JS_Call", 0))
        self.assertEqual(symbols.resolve(4127), ("JS_Call", 15))
        for address in [0, 4096, 4128, 4224]:
            self.assertIsNone(symbols.resolve(address))
        text = "Sort by top of stack, same collapsed (when >= 5):\n??? (in <unknown binary>) [0x1011] 7\n??? (in <unknown binary>) [0x1012] 8\n??? (in <unknown binary>) [0x1020] 9\n"
        output, counts, matched = profile.annotate(text, symbols)
        self.assertIn("JS_Call (in Flower QuickJS Wasm) + 1 [0x1011]", output)
        self.assertIn("??? (in <unknown binary>) [0x1020]", output)
        self.assertEqual((counts, matched), ({"JS_Call": 15}, 2))
        replacement = dict(record, functions=[{"index": 3, "offset": 16, "length": 16}])
        self.assertIsNone(profile.Symbols(sidecar, [record, replacement]).resolve(4112))
        with self.assertRaises(ValueError):
            profile.Symbols(sidecar, [dict(record, guest_sha256="different")])
        with self.assertRaises(ValueError):
            profile.Symbols(sidecar, [record, dict(record, pid=2)])


if __name__ == "__main__":
    unittest.main()
