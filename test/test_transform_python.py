"""Python binding parity tests for prepared numeric transforms."""

from array import array
import ctypes
import importlib
import json
import math
import os
from pathlib import Path
import struct
import sys
import threading
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'py'))
if (ROOT / 'build' / 'libtranfi.so').exists():
    os.environ['TRANFI_LIB_PATH'] = str(ROOT / 'build' / 'libtranfi.so')

import tranfi
from tranfi import _ffi


VECTORS = json.loads(
    (ROOT / 'test' / 'vectors' / 'prepared_transform_v1.json').read_text())
SCHEMA64 = [{'id': 'x0', 'name': 'x0', 'dtype': 'float64'}]
SUPPORTED_CASES = [
    case for case in VECTORS['semanticCases']
    if case['recipe'] in {
        'numeric_mean_standard', 'numeric_median_none',
        'numeric_median_zero_none',
        'numeric_none_none', 'numeric_zero_minmax'
    } and 'expectedError' not in case
]


def recipe_text(name):
    return json.dumps(VECTORS['recipes'][name], separators=(',', ':'))


def double_from_bits(value):
    if value is None:
        return float('nan')
    return struct.unpack('>d', bytes.fromhex(value))[0]


def double_bits(value):
    return struct.pack('>d', value).hex()


def make_table(values, dtype='d', validity=None):
    column = {'data': array(dtype, values)}
    if validity is not None:
        column['validity'] = validity
    return {'rows': len(values), 'columns': [column]}


def large_cancellation_fixture():
    identifier = 'x' + 'a' * (4 * 1024 * 1024)
    recipe = json.loads(recipe_text('numeric_none_none'))
    recipe['columns'][0]['sourceId'] = identifier
    return (
        json.dumps(recipe, separators=(',', ':')),
        [{'id': identifier, 'name': 'large feature', 'dtype': 'float64'}],
        tranfi.TransformLimits(
            max_recipe_bytes=8 * 1024 * 1024,
            max_string_bytes=8 * 1024 * 1024),
    )


def assert_in_call_cancelled(token, action, *, after_polls=1):
    baseline = token._poll_count
    ready = threading.Event()
    observed = []

    def request_after_entry():
        ready.set()
        deadline = time.monotonic() + 5
        while (token._poll_count < baseline + after_polls
               and time.monotonic() < deadline):
            time.sleep(0)
        if token._poll_count >= baseline + after_polls:
            observed.append(token._poll_count)
            token.request()

    canceller = threading.Thread(target=request_after_entry)
    canceller.start()
    assert ready.wait(timeout=5)
    started = time.monotonic()
    try:
        with pytest.raises(tranfi.TranfiTransformError) as caught:
            action()
    finally:
        canceller.join(timeout=5)
    assert not canceller.is_alive()
    assert observed
    assert token.requested
    assert token._poll_count >= baseline + after_polls
    assert time.monotonic() - started < 5
    assert caught.value.code == 109


def test_ctypes_layout_and_safe_limits():
    assert ctypes.sizeof(_ffi._TfTransformLimitsV1) == 184
    assert ctypes.sizeof(_ffi._TfTransformRuntimeV1) == 64
    assert ctypes.sizeof(_ffi._TfFieldViewV1) == 48
    assert ctypes.sizeof(_ffi._TfSchemaViewV1) == 32
    assert ctypes.sizeof(_ffi._TfColumnViewV1) == 64
    assert ctypes.sizeof(_ffi._TfTableViewV1) == 40
    assert ctypes.sizeof(_ffi._TfOwnedDenseV1) == 48
    limits = tranfi.safe_transform_limits()
    assert limits.max_recipe_bytes == 1_048_576
    assert limits.max_plan_bytes == 67_108_864
    assert limits.max_output_elements_per_call == 134_217_728
    limit_values = limits.to_dict()
    assert limit_values['max_live_handles'] == 65_535
    assert len(limit_values) == 22
    for name in limit_values:
        with pytest.raises(tranfi.TranfiTransformError) as caught:
            tranfi.TransformLimits(**{name: 0})
        assert caught.value.code == 100
    with pytest.raises(TypeError):
        tranfi.TransformLimits(unknown_limit=1)


@pytest.mark.parametrize('case', SUPPORTED_CASES, ids=lambda case: case['id'])
def test_semantic_vectors_chunk_plan_and_apply_parity(case):
    values = [double_from_bits(row[0]) for row in case['analyze']['rows']]
    apply_values = [double_from_bits(row[0]) for row in case['apply']['rows']]
    expected = [row[0] for row in case['apply']['expectedRows']]
    reference_blob = None
    for split in case['analyze']['chunkSplits']:
        with tranfi.TransformRecipe.from_json(recipe_text(case['recipe'])) as recipe:
            with recipe.analyzer(SCHEMA64) as analyzer:
                offset = 0
                for count in split:
                    analyzer.push(make_table(values[offset:offset + count]))
                    offset += count
                assert offset == len(values)
                with analyzer.finalize() as plan:
                    blob = plan.to_bytes()
                    if reference_blob is None:
                        reference_blob = blob
                    else:
                        assert blob == reference_blob
                    assert plan.schema_json('input') == (
                        b'[{"dtype":"float64","id":"x0","name":"x0"}]')
                    with plan.apply(SCHEMA64) as apply:
                        result = apply.run(make_table(apply_values))
                        assert result.rows == len(apply_values)
                        assert result.columns == 1
                        actual = [double_bits(value) for value in result.data]
                        assert actual == expected
    with tranfi.TransformPlan.from_bytes(reference_blob) as loaded:
        assert loaded.to_bytes() == reference_blob


def test_empty_analysis_preserves_insufficient_data_code():
    case = next(
        item for item in VECTORS['semanticCases']
        if item['id'] == 'empty-analysis-insufficient')
    with tranfi.TransformRecipe.from_json(recipe_text(case['recipe'])) as recipe:
        with recipe.analyzer(SCHEMA64) as analyzer:
            analyzer.push(make_table([]))
            with pytest.raises(tranfi.TranfiTransformError) as caught:
                analyzer.finalize()
            assert caught.value.code == case['expectedError']['code']


def test_median_errors_and_retained_state_limits():
    case = next(
        item for item in VECTORS['semanticCases']
        if item['id'] == 'numeric-all-missing-error-median')
    values = [double_from_bits(row[0]) for row in case['analyze']['rows']]
    with tranfi.TransformRecipe.from_json(
            recipe_text(case['recipe'])) as recipe:
        with recipe.analyzer(SCHEMA64) as analyzer:
            analyzer.push(make_table(values))
            with pytest.raises(tranfi.TranfiTransformError) as caught:
                analyzer.finalize()
            assert caught.value.code == case['expectedError']['code']

        limits = tranfi.TransformLimits(max_allocations_per_session=9)
        with recipe.analyzer(SCHEMA64, limits=limits) as analyzer:
            with pytest.raises(tranfi.TranfiTransformError) as caught:
                analyzer.push(make_table([1.0]))
            assert caught.value.code == 104
            with pytest.raises(tranfi.TranfiTransformError) as terminal:
                analyzer.push(make_table([1.0]))
            assert terminal.value.code == 112


def test_f32_validity_readonly_and_parent_lifetimes():
    schema = [{'id': 'x0', 'name': 'feature', 'dtype': 'float32'}]
    config = json.loads(recipe_text('numeric_median_none'))
    recipe = tranfi.TransformRecipe.from_json(
        json.dumps(config, separators=(',', ':')))
    analyzer = recipe.analyzer(schema)
    recipe.close()
    source = array('f', [1.0, 99.0, 3.0])
    analyzer.push({
        'rows': 3,
        'columns': [{
            'data': memoryview(source).toreadonly(),
            'validity': b'\x05',
        }],
    })
    plan = analyzer.finalize()
    analyzer.close()
    apply = plan.apply(schema)
    plan.close()
    result = apply.run({
        'rows': 3,
        'columns': [{'data': array('f', [1.0, float('nan'), 3.0])}],
    })
    assert [double_bits(value) for value in result.data] == [
        '3ff0000000000000', '4000000000000000', '4008000000000000']
    apply.close()
    apply.close()


def test_python_buffers_require_native_endianness():
    foreign_type = (ctypes.c_double.__ctype_be__
                    if sys.byteorder == 'little'
                    else ctypes.c_double.__ctype_le__)
    foreign_values = (foreign_type * 2)(1.0, 2.0)
    native_values = (ctypes.c_double * 2)(1.0, 2.0)
    with tranfi.TransformRecipe.from_json(
            recipe_text('numeric_none_none')) as recipe:
        with recipe.analyzer(SCHEMA64) as analyzer:
            with pytest.raises(TypeError, match='native-endian'):
                analyzer.push({'rows': 2, 'columns': [foreign_values]})
            analyzer.push({'rows': 2, 'columns': [native_values]})
            with analyzer.finalize() as plan:
                with plan.apply(SCHEMA64) as apply:
                    with pytest.raises(TypeError, match='native-endian'):
                        apply.run({'rows': 2, 'columns': [foreign_values]})
                    result = apply.run({
                        'rows': 2, 'columns': [native_values]})
                    assert list(result.data) == [1.0, 2.0]


def test_zero_rows_and_operational_limit():
    with tranfi.TransformRecipe.from_json(
            recipe_text('numeric_none_none')) as recipe:
        with recipe.analyzer(SCHEMA64) as analyzer:
            analyzer.push(make_table([1.0]))
            with analyzer.finalize() as plan:
                with plan.apply(SCHEMA64) as apply:
                    source = array('d', [7.0, 8.0])
                    for view in (
                            memoryview(source)[:0],
                            memoryview(source)[1:1]):
                        result = apply.run({'rows': 0, 'columns': [view]})
                        assert result.rows == 0 and result.columns == 1
                        assert list(result.data) == []
                    result = apply.run(make_table([2.0]))
                    assert list(result.data) == [2.0]
                    result = apply.run(make_table([]))
                    assert result.rows == 0 and result.columns == 1
                    assert list(result.data) == []
                limits = tranfi.TransformLimits(max_apply_rows=1)
                with plan.apply(SCHEMA64, limits=limits) as apply:
                    with pytest.raises(tranfi.TranfiTransformError) as caught:
                        apply.run(make_table([1.0, 2.0]))
                    assert caught.value.code == 104

                with plan.apply(SCHEMA64) as apply:
                    with pytest.raises(tranfi.TranfiTransformError) as caught:
                        apply.run({
                            'rows': 0,
                            'columns': [array('d', [1.0])],
                        })
                    assert caught.value.code == 100
                    result = apply.run(make_table([3.0]))
                    assert list(result.data) == [3.0]


def test_python_unicode_schema_parity():
    for invalid in ('\ud800', '\udc00'):
        with pytest.raises(UnicodeEncodeError):
            tranfi.TransformRecipe.from_json(invalid)
        with tranfi.TransformRecipe.from_json(
                recipe_text('numeric_none_none')) as recipe:
            with pytest.raises(UnicodeEncodeError):
                recipe.analyzer([{
                    'id': 'x0', 'name': invalid, 'dtype': 'float64'}])
            with pytest.raises(UnicodeEncodeError):
                recipe.analyzer([{
                    'id': invalid, 'name': 'x0', 'dtype': 'float64'}])

    astral_id = 'x\U0001f600'
    astral_name = 'feature\U0001f680'
    config = json.loads(recipe_text('numeric_none_none'))
    config['columns'][0]['sourceId'] = astral_id
    text = json.dumps(config, ensure_ascii=False, separators=(',', ':'))
    schema = [{'id': astral_id, 'name': astral_name, 'dtype': 'float64'}]
    with tranfi.TransformRecipe.from_json(text) as recipe:
        with recipe.analyzer(schema) as analyzer:
            analyzer.push(make_table([1.0]))
            with analyzer.finalize() as plan:
                expected = json.dumps([{
                    'dtype': 'float64', 'id': astral_id, 'name': astral_name,
                }], ensure_ascii=False, separators=(',', ':')).encode('utf-8')
                assert plan.schema_json('input') == expected


def test_python_transferred_handles_are_destroyed_on_wrapper_failure(monkeypatch):
    module = importlib.import_module('tranfi.transform')
    lib = _ffi._load_lib()

    def count_destroy(name):
        original = getattr(lib, name)
        calls = []

        def counting(pointer):
            calls.append(1)
            original(pointer)

        monkeypatch.setattr(lib, name, counting)
        return original, calls

    original, calls = count_destroy('tf_transform_recipe_destroy')

    class FailingRecipe(module.TransformRecipe):
        def __init__(self, *args):
            del args
            raise MemoryError('injected recipe wrapper failure')

    with pytest.raises(MemoryError):
        FailingRecipe.from_json(recipe_text('numeric_none_none'))
    assert calls == [1]
    monkeypatch.setattr(lib, 'tf_transform_recipe_destroy', original)

    with tranfi.TransformRecipe.from_json(
            recipe_text('numeric_none_none')) as recipe:
        original_constructor = module.TransformAnalyzer
        original, calls = count_destroy('tf_transform_analyzer_destroy')
        monkeypatch.setattr(
            module, 'TransformAnalyzer',
            lambda *args: (_ for _ in ()).throw(
                MemoryError('injected analyzer wrapper failure')))
        with pytest.raises(MemoryError):
            recipe.analyzer(SCHEMA64)
        assert calls == [1]
        monkeypatch.setattr(module, 'TransformAnalyzer', original_constructor)
        monkeypatch.setattr(lib, 'tf_transform_analyzer_destroy', original)

        with recipe.analyzer(SCHEMA64) as analyzer:
            analyzer.push(make_table([1.0]))
            original_constructor = module.TransformPlan
            original, calls = count_destroy('tf_transform_plan_destroy')
            monkeypatch.setattr(
                module, 'TransformPlan',
                lambda *args: (_ for _ in ()).throw(
                    MemoryError('injected plan wrapper failure')))
            with pytest.raises(MemoryError):
                analyzer.finalize()
            assert calls == [1]
            monkeypatch.setattr(module, 'TransformPlan', original_constructor)
            monkeypatch.setattr(lib, 'tf_transform_plan_destroy', original)
            with pytest.raises(tranfi.TranfiTransformError) as terminal:
                analyzer.finalize()
            assert terminal.value.code == 112

    with tranfi.TransformRecipe.from_json(
            recipe_text('numeric_none_none')) as recipe:
        with recipe.analyzer(SCHEMA64) as analyzer:
            analyzer.push(make_table([1.0]))
            with analyzer.finalize() as plan:
                plan_bytes = plan.to_bytes()
                original, calls = count_destroy('tf_transform_plan_destroy')

                class FailingPlan(module.TransformPlan):
                    def __init__(self, *args):
                        del args
                        raise MemoryError('injected imported-plan wrapper failure')

                with pytest.raises(MemoryError):
                    FailingPlan.from_bytes(plan_bytes)
                assert calls == [1]
                monkeypatch.setattr(lib, 'tf_transform_plan_destroy', original)

                original_constructor = module.TransformApply
                original, calls = count_destroy('tf_transform_apply_destroy')
                monkeypatch.setattr(
                    module, 'TransformApply',
                    lambda *args: (_ for _ in ()).throw(
                        MemoryError('injected apply wrapper failure')))
                with pytest.raises(MemoryError):
                    plan.apply(SCHEMA64)
                assert calls == [1]
                monkeypatch.setattr(module, 'TransformApply', original_constructor)
                monkeypatch.setattr(lib, 'tf_transform_apply_destroy', original)


def test_python_post_c_output_failure_closes_apply(monkeypatch):
    module = importlib.import_module('tranfi.transform')
    with tranfi.TransformRecipe.from_json(
            recipe_text('numeric_none_none')) as recipe:
        with recipe.analyzer(SCHEMA64) as analyzer:
            analyzer.push(make_table([1.0]))
            with analyzer.finalize() as plan:
                limits = tranfi.TransformLimits(max_apply_rows=1)
                with plan.apply(SCHEMA64, limits=limits) as apply:
                    input_table = make_table([2.0])
                    original_array = module.array

                    def fail_array(*args, **kwargs):
                        del args, kwargs
                        raise MemoryError('injected output materialization failure')

                    monkeypatch.setattr(module, 'array', fail_array)
                    with pytest.raises(MemoryError):
                        apply.run(input_table)
                    monkeypatch.setattr(module, 'array', original_array)
                    with pytest.raises(tranfi.TranfiTransformError) as terminal:
                        apply.run(input_table)
                    assert terminal.value.code == 112


def test_stable_error_codes_and_closed_state():
    with pytest.raises(tranfi.TranfiTransformError) as caught:
        tranfi.TransformRecipe.from_json('{}')
    assert caught.value.code == 101

    recipe = tranfi.TransformRecipe.from_json(recipe_text('numeric_none_none'))
    with pytest.raises(tranfi.TranfiTransformError) as caught:
        recipe.analyzer([{'id': 'wrong', 'name': 'wrong', 'dtype': 'float64'}])
    assert caught.value.code == 102
    recipe.close()
    with pytest.raises(tranfi.TranfiTransformError) as caught:
        recipe.analyzer(SCHEMA64)
    assert caught.value.code == 112

    with pytest.raises(tranfi.TranfiTransformError) as caught:
        tranfi.TransformPlan.from_bytes(b'not-a-plan')
    assert caught.value.code == 106

    recipe = tranfi.TransformRecipe.from_json(recipe_text('numeric_none_none'))
    analyzer = recipe.analyzer(SCHEMA64)
    with pytest.raises(tranfi.TranfiTransformError) as caught:
        analyzer.push(make_table([math.inf]))
    assert caught.value.code == 107
    analyzer.close()
    recipe.close()


def test_cross_thread_in_call_cancellation_and_retention():
    token = tranfi.TransformCancelToken()
    assert not token.requested
    recipe = tranfi.TransformRecipe.from_json(
        recipe_text('numeric_none_none'))
    analyzer = recipe.analyzer(SCHEMA64, cancel_token=token)
    rows = 8 * 1024 * 1024
    values = array('d', [0.0]) * rows
    start = threading.Event()

    def request_after_entry():
        start.wait()
        time.sleep(0.01)
        token.request()

    canceller = threading.Thread(target=request_after_entry)
    canceller.start()
    start.set()
    with pytest.raises(tranfi.TranfiTransformError) as caught:
        analyzer.push({'rows': rows, 'columns': [values]})
    canceller.join()
    assert caught.value.code == 109
    assert token.requested
    with pytest.raises(tranfi.TranfiTransformError) as terminal:
        analyzer.push(make_table([1.0]))
    assert terminal.value.code == 112
    analyzer.close()
    recipe.close()

    with tranfi.TransformRecipe.from_json(
            recipe_text('numeric_none_none')) as checked_recipe:
        with pytest.raises(TypeError):
            checked_recipe.analyzer(SCHEMA64, cancel_token=object())


def test_cancellation_reaches_import_finalize_and_apply():
    recipe_json, schema, limits = large_cancellation_fixture()
    with tranfi.TransformRecipe.from_json(
            recipe_json, limits=limits) as recipe:
        with recipe.analyzer(schema, limits=limits) as analyzer:
            analyzer.push(make_table([1.0]))
            with analyzer.finalize() as plan:
                plan_bytes = plan.to_bytes(limits=limits)
                exact_string_bytes = len(schema[0]['id'].encode('utf-8'))
                restrictive = tranfi.TransformLimits(
                    max_recipe_bytes=limits.max_recipe_bytes,
                    max_string_bytes=exact_string_bytes - 1)
                with pytest.raises(tranfi.TranfiTransformError) as caught:
                    plan.to_bytes(limits=restrictive)
                assert caught.value.code == 104
                with pytest.raises(tranfi.TranfiTransformError) as caught:
                    plan.schema_json('input', limits=restrictive)
                assert caught.value.code == 104
                exact = tranfi.TransformLimits(
                    max_recipe_bytes=limits.max_recipe_bytes,
                    max_string_bytes=exact_string_bytes)
                assert plan.to_bytes(limits=exact) == plan_bytes

    finalize_token = tranfi.TransformCancelToken()
    with tranfi.TransformRecipe.from_json(
            recipe_json, limits=limits) as recipe:
        with recipe.analyzer(
                schema, limits=limits,
                cancel_token=finalize_token) as analyzer:
            analyzer.push(make_table([1.0]))
            assert_in_call_cancelled(finalize_token, analyzer.finalize)
            with pytest.raises(tranfi.TranfiTransformError) as terminal:
                analyzer.finalize()
            assert terminal.value.code == 112

    import_token = tranfi.TransformCancelToken()
    assert_in_call_cancelled(
        import_token,
        lambda: tranfi.TransformPlan.from_bytes(
            plan_bytes, limits=limits, cancel_token=import_token))

    with tranfi.TransformPlan.from_bytes(
            plan_bytes, limits=limits) as plan:
        apply_token = tranfi.TransformCancelToken()
        with plan.apply(
                schema, limits=limits, cancel_token=apply_token) as apply:
            rows = 4 * 1024 * 1024
            values = array('d', [0.0]) * rows
            assert_in_call_cancelled(
                apply_token,
                lambda: apply.run({'rows': rows, 'columns': [values]}))
            with pytest.raises(tranfi.TranfiTransformError) as terminal:
                apply.run(make_table([1.0]))
            assert terminal.value.code == 112


def test_python_preflights_limits_before_c_transport():
    lib = _ffi._load_lib()

    def assert_not_called(name, action):
        original = getattr(lib, name)
        calls = []

        def unexpected(*args):
            calls.append(args)
            return 199

        setattr(lib, name, unexpected)
        try:
            with pytest.raises(tranfi.TranfiTransformError) as caught:
                action()
            assert caught.value.code == 104
            assert not calls
        finally:
            setattr(lib, name, original)

    assert_not_called(
        'tf_transform_recipe_from_json',
        lambda: tranfi.TransformRecipe.from_json(
            b'{}', limits=tranfi.TransformLimits(max_recipe_bytes=1)))

    with tranfi.TransformRecipe.from_json(
            recipe_text('numeric_none_none')) as recipe:
        assert_not_called(
            'tf_transform_analyzer_create',
            lambda: recipe.analyzer(
                [*SCHEMA64, {'id': 'x1', 'dtype': 'float64'}],
                limits=tranfi.TransformLimits(max_input_columns=1)))
        limits = tranfi.TransformLimits(
            max_analyzer_rows=1, max_analyzer_input_bytes=8)
        with recipe.analyzer(SCHEMA64, limits=limits) as analyzer:
            assert_not_called(
                'tf_transform_analyzer_push',
                lambda: analyzer.push(make_table([1.0, 2.0])))
            analyzer.push(make_table([1.0]))
            with analyzer.finalize() as plan:
                plan_bytes = plan.to_bytes()
                assert_not_called(
                    'tf_transform_plan_import',
                    lambda: tranfi.TransformPlan.from_bytes(
                        plan_bytes,
                        limits=tranfi.TransformLimits(
                            max_plan_bytes=len(plan_bytes) - 1)))


def test_python_cancels_during_readonly_transport_copy():
    token = tranfi.TransformCancelToken()
    values = memoryview(
        array('d', [0.0]) * (4 * 1024 * 1024)).toreadonly()
    with tranfi.TransformRecipe.from_json(
            recipe_text('numeric_none_none')) as recipe:
        analyzer = recipe.analyzer(SCHEMA64, cancel_token=token)
        assert_in_call_cancelled(
            token,
            lambda: analyzer.push({
                'rows': len(values),
                'columns': [values],
            }),
            after_polls=6)
        with pytest.raises(tranfi.TranfiTransformError) as terminal:
            analyzer.push(make_table([1.0]))
        assert terminal.value.code == 112
        analyzer.close()


def test_python_cancels_during_schema_encoding():
    recipe_json, schema, limits = large_cancellation_fixture()
    token = tranfi.TransformCancelToken()
    with tranfi.TransformRecipe.from_json(
            recipe_json, limits=limits) as recipe:
        assert_in_call_cancelled(
            token,
            lambda: recipe.analyzer(
                schema, limits=limits, cancel_token=token),
            after_polls=6)
        with recipe.analyzer(schema, limits=limits) as analyzer:
            analyzer.push(make_table([1.0]))


def test_python_size_t_boundaries_do_not_wrap():
    size_max = ctypes.c_size_t(-1).value
    aligned_max = size_max - size_max % ctypes.sizeof(ctypes.c_double)
    limits = tranfi.TransformLimits(max_analyzer_rows=size_max)
    with tranfi.TransformRecipe.from_json(
            recipe_text('numeric_none_none')) as recipe:
        with recipe.analyzer(SCHEMA64, limits=limits) as analyzer:
            with pytest.raises(OverflowError, match='table rows'):
                analyzer.push({
                    'rows': size_max + 1,
                    'columns': [array('d')],
                })
            with pytest.raises(OverflowError, match='data span'):
                analyzer.push({
                    'rows': size_max,
                    'columns': [array('d')],
                })
            with pytest.raises(OverflowError, match='stride_bytes'):
                analyzer.push({
                    'rows': 1,
                    'columns': [{
                        'data': array('d', [1.0]),
                        'stride_bytes': size_max + 1,
                    }],
                })
            analyzer.push({
                'rows': 1,
                'columns': [{
                    'data': array('d', [1.0]),
                    'stride_bytes': aligned_max,
                }],
            })

    with tranfi.TransformRecipe.from_json(
            recipe_text('numeric_none_none')) as recipe:
        with recipe.analyzer(SCHEMA64) as analyzer:
            with pytest.raises(OverflowError, match='validity_bit_offset'):
                analyzer.push({
                    'rows': 1,
                    'columns': [{
                        'data': array('d', [1.0]),
                        'validity': b'\x01',
                        'validity_bit_offset': size_max + 1,
                    }],
                })
            with pytest.raises(OverflowError, match='validity_bit_stride'):
                analyzer.push({
                    'rows': 1,
                    'columns': [{
                        'data': array('d', [1.0]),
                        'validity': b'\x01',
                        'validity_bit_stride': size_max + 1,
                    }],
                })
            analyzer.push({
                'rows': 1,
                'columns': [{
                    'data': array('d', [1.0]),
                    'validity': b'\x01',
                    'validity_bit_stride': size_max,
                }],
            })
