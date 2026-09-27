#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import tempfile
import json
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("cpu_profile", Path(__file__).with_name("profile-cpu.py"))
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)

SCHEMA = '<schema name="time-profile"><col><mnemonic>thread-state</mnemonic><engineering-type>thread-state</engineering-type></col><col><mnemonic>weight</mnemonic><engineering-type>weight</engineering-type></col><col><mnemonic>stack</mnemonic><engineering-type>tagged-backtrace</engineering-type></col></schema>'


def exported(rows):
    return ('<trace-query-result><node>' + SCHEMA + rows + '</node></trace-query-result>').encode()


def sample(stack='', state='Running', weight='1000000', extra=''):
    return f'<row><thread-state>{state}</thread-state><weight>{weight}</weight>{stack}{extra}</row>'


def no_demangle(names):
    return {}, "disabled in test"


class CpuProfileTests(unittest.TestCase):
    def analyze(self, rows, **kwargs):
        return profile.analyze(exported(rows), demangler=no_demangle, **kwargs)

    def test_references_weights_recursive_inclusive_and_ownership(self):
        rows = '''<row><thread-state id="s">Running</thread-state><weight id="w">2000000</weight>
        <tagged-backtrace id="stack"><frame id="leaf" name="_platform_memmove"/>
        <frame name="wasmtime::call&lt;flower::evaluator::Host&gt;"/><frame id="owner" name="&lt;flower::evaluator::Tracker&gt;::restore"/><frame ref="owner"/></tagged-backtrace></row>
        <row><thread-state ref="s"/><weight ref="w"/><tagged-backtrace ref="stack"/></row>'''
        result = self.analyze(rows)
        self.assertEqual(result['sampledRunningMs'], 4)
        self.assertEqual(result['self'][0]['sampledRunningMs'], 4)
        owner = '<flower::evaluator::Tracker>::restore'
        self.assertEqual(next(row for row in result['inclusive'] if row['frame'] == owner)['sampledRunningMs'], 4)
        self.assertEqual(result['nearestFlower'][0]['frame'], owner)
        self.assertEqual(result['copyNearestFlower'][0]['sampledRunningMs'], 4)
        self.assertEqual(result['stackCoverage'], 1)

    def test_nonrunning_unknown_states_and_wait_named_running_frames(self):
        stack = '<tagged-backtrace><frame name="__psynch_mutexwait"/></tagged-backtrace>'
        rows = sample(stack) + sample(stack, 'Waiting', '4000000') + sample(stack, 'NewState', '2000000')
        rows += '<row><weight>3000000</weight></row>'
        result = self.analyze(rows)
        self.assertEqual(result['sampledRunningMs'], 1)
        self.assertEqual(result['recordedStateWeightMs'], {'Running': 1, 'Waiting': 4, 'NewState': 2, '[missing state]': 3})
        self.assertEqual(result['self'][0]['frame'], '__psynch_mutexwait')
        self.assertEqual(result['runningRows'], 1)

    def test_missing_partial_and_unresolved_symbol_weights_are_visible(self):
        rows = sample('<tagged-backtrace><frame name="leaf"/></tagged-backtrace>')
        rows += sample(weight='2000000')
        rows += sample('<tagged-backtrace><frame name="known"/><frame ref="absent"/></tagged-backtrace>', weight='3000000')
        rows += sample('<tagged-backtrace><frame name="0x1234" addr="0x1234"/></tagged-backtrace>', weight='4000000')
        result = self.analyze(rows)
        self.assertEqual(result['sampledRunningMs'], 10)
        self.assertEqual(result['runningWithCompleteExportedStackMs'], 5)
        self.assertEqual(result['runningWithMissingStackMs'], 2)
        self.assertEqual(result['runningWithPartialExportedStackMs'], 3)
        self.assertEqual(result['runningWithUnresolvedSymbolsMs'], 4)
        self.assertEqual(result['stackCoverage'], 0.5)
        self.assertEqual(sum(row['percentOfAllRunningWeight'] for row in result['self']), 50)
        self.assertEqual(result['issues']['runningRowsWithUnresolvedFrameReferences'], 1)

    def test_forward_references_and_absent_stack_reference(self):
        rows = '<row><thread-state ref="s"/><weight ref="w"/><tagged-backtrace ref="stack"/></row>'
        rows += '<row><thread-state id="s">Running</thread-state><weight id="w">1000000</weight><tagged-backtrace id="stack"><frame name="f"/></tagged-backtrace></row>'
        rows += sample('<tagged-backtrace ref="missing"/>')
        result = self.analyze(rows)
        self.assertEqual(result['runningWithCompleteExportedStackMs'], 2)
        self.assertEqual(result['runningWithMissingStackMs'], 1)

    def test_guest_names_only_apply_to_matching_pid(self):
        class Symbols:
            def resolve(self, address):
                return ('JS_CallInternal', 0) if address == 0x1000 else None
        stack = '<tagged-backtrace><frame name="0x1000" addr="0x1000"/></tagged-backtrace>'
        rows = sample(stack, extra='<process><pid>42</pid></process>')
        rows += sample(stack, extra='<process><pid>43</pid></process>')
        result = self.analyze(rows, symbols=Symbols(), symbol_pid=42)
        self.assertEqual({row['frame'] for row in result['self']}, {'JS_CallInternal', '0x1000'})
        self.assertEqual(result['runningWithUnresolvedSymbolsMs'], 1)
        self.assertEqual(result['recordedRunningWeightMsByPid'], {'42': 1, '43': 1})
        self.assertEqual(result['issues']['runningRowsWithoutMatchingMapPid'], 1)

    def test_invalid_weights_references_and_duplicate_ids_fail(self):
        for weight in ('-1', 'NaN', '1.5', str(1 << 63), ''):
            with self.assertRaisesRegex(ValueError, 'weight'):
                self.analyze(sample(weight=weight))
        for rows in [
            '<row><weight id="a" ref="b"/><weight id="b" ref="a"/></row>',
            '<row><weight id="same">1</weight><weight id="same">2</weight></row>',
            '<row><thread-state id="s">Running</thread-state><weight ref="s"/></row>',
        ]:
            with self.assertRaises(ValueError):
                self.analyze(rows)

    def test_bounded_rows_elements_frames_and_bytes(self):
        with patch.object(profile, 'MAX_ROWS', 1), self.assertRaisesRegex(ValueError, 'row limit'):
            self.analyze(sample() + sample())
        with patch.object(profile, 'MAX_ELEMENTS', 1), self.assertRaisesRegex(ValueError, 'element limit'):
            self.analyze(sample())
        with patch.object(profile, 'MAX_STACK_FRAMES', 1), self.assertRaisesRegex(ValueError, 'frame limit'):
            self.analyze(sample('<tagged-backtrace><frame name="a"/><frame name="b"/></tagged-backtrace>'))
        with patch.object(profile, 'MAX_XML_BYTES', 1), self.assertRaisesRegex(ValueError, 'byte limit'):
            self.analyze(sample())

    def test_rejects_toc_other_tables_and_entity_declarations(self):
        for xml in [b'<trace-toc><environment>private</environment></trace-toc>', exported(sample()).replace(b'time-profile', b'process-info'), b'<!DOCTYPE foo [<!ENTITY e "bad">]>' + exported(sample())]:
            with self.assertRaises(ValueError):
                profile.analyze(xml, demangler=no_demangle)

    def test_demangler_failure_keeps_raw_symbols(self):
        def fail(*args, **kwargs):
            raise OSError('not installed')
        mapping, status = profile.demangle({'_RNvExample'}, execute=fail)
        self.assertEqual(mapping, {})
        self.assertIn('raw Rust symbols retained', status)

    def test_guest_map_artifact_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            sidecar, native = Path(directory) / 'symbols.json', Path(directory) / 'map.jsonl'
            sidecar.write_text(json.dumps({'guest_sha256': 'one', 'functions': {'1': 'JS_CallInternal'}}))
            native.write_text(json.dumps({'pid': 42, 'guest_sha256': 'two', 'text_base': 1, 'text_length': 100, 'functions': []}))
            with self.assertRaisesRegex(ValueError, 'different guest'):
                profile.load_symbols(sidecar, native)

    def test_no_running_samples_has_no_fabricated_percentages(self):
        result = self.analyze(sample(state='Waiting'))
        self.assertEqual(result['sampledRunningMs'], 0)
        self.assertEqual(result['self'], [])
        self.assertIsNone(result['stackCoverage'])


if __name__ == '__main__':
    unittest.main()
