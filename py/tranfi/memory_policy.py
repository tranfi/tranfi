"""Host-side memory policy checks for native Tranfi execution."""

from __future__ import annotations

import json
import math
from typing import Any, Dict, Iterable, Tuple

BLOCKING_OPS = {
    'sort',
    'pivot',
    'stack',
    'interpolate',
    'normalize',
    'acf',
    'codec.table.encode',
}

KEY_STATE_OPS = {
    'rowid',
    'unique',
    'dedup',
    'group-agg',
    'frequency',
    'join',
    'semi-join',
    'anti-join',
    'onehot',
    'label-encode',
    'intersect',
    'setdiff',
    'intersect-all',
    'setdiff-all',
    'union',
}

_SIZE_UNITS = {
    '': 1,
    'b': 1,
    'k': 1024,
    'kb': 1024,
    'kib': 1024,
    'm': 1024 ** 2,
    'mb': 1024 ** 2,
    'mib': 1024 ** 2,
    'g': 1024 ** 3,
    'gb': 1024 ** 3,
    'gib': 1024 ** 3,
    't': 1024 ** 4,
    'tb': 1024 ** 4,
    'tib': 1024 ** 4,
}


def parse_memory_size(value: Any) -> int | None:
    """Parse a byte size. Accepts None, positive ints, or strings like max:64MB."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError('memory must be a positive byte count or size string')
    if isinstance(value, int):
        if value <= 0:
            raise ValueError('memory must be positive')
        return value
    if isinstance(value, float):
        if not math.isfinite(value) or value <= 0 or int(value) != value:
            raise ValueError('memory must be a positive integer byte count')
        return int(value)
    if not isinstance(value, str):
        raise TypeError('memory must be a positive byte count or size string')

    text = value.strip()
    lower = text.lower()
    if lower.startswith('max:') or lower.startswith('max='):
        text = text[4:].strip()
    if not text:
        raise ValueError('memory size is empty')

    i = 0
    while i < len(text) and text[i].isdigit():
        i += 1
    if i == 0:
        raise ValueError('memory must look like 64MB or max:64MB')
    amount = int(text[:i])
    suffix = text[i:].strip().lower()
    if amount <= 0:
        raise ValueError('memory must be positive')
    if suffix not in _SIZE_UNITS:
        raise ValueError(f'unsupported memory size suffix {suffix!r}')
    return amount * _SIZE_UNITS[suffix]


def memory_for_engine(value: Any) -> str | None:
    """Return a DuckDB-compatible memory size string, or None."""
    n = parse_memory_size(value)
    return None if n is None else f'{n}B'


def format_bytes(n: int) -> str:
    if n < 1024:
        return f'{n}B'
    if n < 1024 ** 2:
        return f'{n / 1024:.1f}KB'
    if n < 1024 ** 3:
        return f'{n / (1024 ** 2):.1f}MB'
    return f'{n / (1024 ** 3):.1f}GB'


def _steps_from_plan(plan_json: str | Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    plan = json.loads(plan_json) if isinstance(plan_json, str) else plan_json
    steps = plan.get('steps') if isinstance(plan, dict) else None
    if not isinstance(steps, list):
        raise ValueError('plan must contain a steps array')
    return steps


def _is_filtering_join(step: Dict[str, Any]) -> bool:
    op = step.get('op')
    if op in ('semi-join', 'anti-join'):
        return True
    if op != 'join':
        return False
    return _arg(step, 'how') in ('semi', 'anti')


def _memory_class(step: Dict[str, Any]) -> str | None:
    cls = step.get('memory_class')
    if isinstance(cls, str):
        return cls
    op = step.get('op')
    args = step.get('args') if isinstance(step.get('args'), dict) else {}
    if op == 'pivot':
        if isinstance(args.get('spill_dir'), str) and args.get('spill_dir'):
            return 'external'
        if args.get('sorted') is True and isinstance(args.get('categories'), list) and len(args.get('categories')) > 0:
            return 'bounded_state'
    if op in ('unique', 'dedup'):
        if isinstance(args.get('spill_dir'), str) and args.get('spill_dir'):
            return 'external'
        if args.get('sorted') is True:
            return 'bounded_state'
    if op == 'group-agg':
        if isinstance(args.get('spill_dir'), str) and args.get('spill_dir'):
            return 'external'
        if args.get('sorted') is True:
            return 'bounded_state'
    if op in ('join', 'semi-join', 'anti-join'):
        if isinstance(args.get('spill_dir'), str) and args.get('spill_dir'):
            return 'external'
        if args.get('sorted') is True:
            return 'bounded_state'
    if op in ('intersect', 'setdiff', 'intersect-all', 'setdiff-all'):
        if isinstance(args.get('spill_dir'), str) and args.get('spill_dir'):
            return 'external'
        if args.get('sorted') is True:
            return 'bounded_state'
    if op == 'union':
        if isinstance(args.get('spill_dir'), str) and args.get('spill_dir'):
            return 'external'
        if args.get('sorted') is True:
            return 'bounded_state'
    if op in BLOCKING_OPS:
        return 'blocking'
    if op in KEY_STATE_OPS:
        return 'key_state'
    return None


def _arg(step: Dict[str, Any], name: str) -> Any:
    args = step.get('args')
    if not isinstance(args, dict):
        return None
    return args.get(name)


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and value > 0 and int(value) == value:
        return int(value)
    return None


def _array_len(value: Any) -> int:
    return len(value) if isinstance(value, list) else 0


def _string_array_bytes(value: Any) -> int:
    if not isinstance(value, list):
        return 0
    total = 0
    for item in value:
        if isinstance(item, str):
            total += len(item.encode('utf-8')) + 1
    return total


def _category_cap(step: Dict[str, Any]) -> Tuple[int | None, int]:
    max_categories = _positive_int(_arg(step, 'max_categories'))
    categories = _arg(step, 'categories')
    n_categories = _array_len(categories)
    literal_bytes = _string_array_bytes(categories)
    if _arg(step, 'unknown') == 'other':
        n_categories += 1
        literal_bytes += len('other') + 1
    if max_categories is not None:
        return max_categories, literal_bytes
    if n_categories > 0:
        return n_categories, literal_bytes
    return None, literal_bytes


def estimate_key_state_step_bytes(step: Dict[str, Any]) -> int:
    op = step.get('op')
    if op == 'rowid':
        n_cols = _array_len(_arg(step, 'columns'))
        if n_cols == 0 or _arg(step, 'sorted') is True:
            return 2048 + n_cols * 128
        max_state_bytes = _positive_int(_arg(step, 'max_state_bytes'))
        if max_state_bytes is not None:
            return max_state_bytes
        max_keys = _positive_int(_arg(step, 'max_keys'))
        if max_keys is None:
            raise RuntimeError("step 'rowid' needs max_keys, max_state_bytes, or sorted=true")
        return 1024 + max_keys * 224 + n_cols * 128

    if op in ('unique', 'dedup'):
        n_cols = _array_len(_arg(step, 'columns'))
        if _arg(step, 'sorted') is True:
            return 2048 + n_cols * 128
        max_state_bytes = _positive_int(_arg(step, 'max_state_bytes'))
        if max_state_bytes is not None:
            return max_state_bytes
        max_keys = _positive_int(_arg(step, 'max_keys'))
        if max_keys is None:
            raise RuntimeError(f"step '{op}' needs max_keys, max_state_bytes, or sorted=true for byte-bounded native execution")
        return 1024 + max_keys * (256 + n_cols * 32)

    if op == 'group-agg':
        n_group = _array_len(_arg(step, 'group_by'))
        n_aggs = _array_len(_arg(step, 'aggs'))
        if isinstance(_arg(step, 'spill_dir'), str) and _arg(step, 'spill_dir'):
            spill_bytes = _positive_int(_arg(step, 'spill_memory_bytes'))
            if spill_bytes is not None:
                return spill_bytes
            raise RuntimeError("step 'group-agg' spill mode needs spill_memory_bytes for a host byte estimate")
        max_state_bytes = _positive_int(_arg(step, 'max_state_bytes'))
        if max_state_bytes is not None:
            return max_state_bytes
        if _arg(step, 'sorted') is True:
            return 4096 + n_group * 128 + n_aggs * 128
        max_groups = _positive_int(_arg(step, 'max_groups'))
        if max_groups is None:
            raise RuntimeError("step 'group-agg' needs max_groups, max_state_bytes, or sorted=true for byte-bounded native execution")
        return 2048 + max_groups * (384 + n_group * 64 + n_aggs * 96)

    if op == 'frequency':
        max_state_bytes = _positive_int(_arg(step, 'max_state_bytes'))
        if max_state_bytes is not None:
            return max_state_bytes
        max_values = _positive_int(_arg(step, 'max_values'))
        if max_values is None:
            raise RuntimeError("step 'frequency' needs max_values or max_state_bytes for byte-bounded native execution")
        n_cols = _array_len(_arg(step, 'columns'))
        if _arg(step, 'overflow') == 'other':
            max_values += 1
        return 1024 + max_values * (224 + n_cols * 32)

    if op in ('onehot', 'label-encode'):
        max_state_bytes = _positive_int(_arg(step, 'max_state_bytes'))
        if max_state_bytes is not None:
            return max_state_bytes
        cap, literal_bytes = _category_cap(step)
        if cap is None:
            raise RuntimeError(f"step '{op}' needs max_categories, max_state_bytes, or declared categories")
        return 1024 + literal_bytes + cap * (320 if op == 'onehot' else 224)

    if op == 'union-all':
        return 4096

    if op in ('intersect', 'setdiff', 'intersect-all', 'setdiff-all'):
        n_cols = _array_len(_arg(step, 'columns'))
        bag_set_op = op in ('intersect-all', 'setdiff-all')
        if isinstance(_arg(step, 'spill_dir'), str) and _arg(step, 'spill_dir'):
            spill_bytes = _positive_int(_arg(step, 'spill_memory_bytes'))
            if spill_bytes is not None:
                return spill_bytes
            raise RuntimeError(f"step '{op}' spill mode needs spill_memory_bytes for a host byte estimate")
        if _arg(step, 'sorted') is True:
            return 4096 + n_cols * 256
        max_lookup_bytes = _positive_int(_arg(step, 'max_lookup_bytes'))
        if max_lookup_bytes is None:
            raise RuntimeError(f"step '{op}' needs max_lookup_bytes for byte-bounded native execution")
        max_state_bytes = _positive_int(_arg(step, 'max_state_bytes'))
        if max_state_bytes is not None:
            return 4096 + max_lookup_bytes * 3 + max_state_bytes
        est = 4096 + max_lookup_bytes * 3
        max_lookup_keys = _positive_int(_arg(step, 'max_lookup_keys'))
        if bag_set_op:
            if max_lookup_keys is None:
                raise RuntimeError(f"step '{op}' needs max_lookup_keys or max_state_bytes for bag-count set semantics")
            return est + max_lookup_keys * (224 + n_cols * 32)
        max_output_keys = _positive_int(_arg(step, 'max_output_keys'))
        if max_output_keys is None:
            raise RuntimeError(f"step '{op}' needs max_output_keys or max_state_bytes for duplicate-eliminating set semantics")
        if max_lookup_keys is not None:
            est += max_lookup_keys * (192 + n_cols * 32)
        est += max_output_keys * (192 + n_cols * 32)
        return est

    if op == 'union':
        if _arg(step, 'sorted') is True:
            n_cols = _array_len(_arg(step, 'columns'))
            return 4096 + n_cols * 256
        if isinstance(_arg(step, 'spill_dir'), str) and _arg(step, 'spill_dir'):
            spill_memory_bytes = _positive_int(_arg(step, 'spill_memory_bytes'))
            if spill_memory_bytes is None:
                raise RuntimeError("step 'union' spill needs spill_memory_bytes for byte-bounded native execution")
            return spill_memory_bytes
        max_state_bytes = _positive_int(_arg(step, 'max_state_bytes'))
        if max_state_bytes is not None:
            return max_state_bytes
        n_cols = _array_len(_arg(step, 'columns'))
        max_output_keys = _positive_int(_arg(step, 'max_output_keys'))
        if max_output_keys is None:
            raise RuntimeError("step 'union' needs max_output_keys or max_state_bytes for duplicate-eliminating set semantics")
        return 4096 + max_output_keys * (192 + n_cols * 32)

    if op in ('join', 'semi-join', 'anti-join'):
        filtering = _is_filtering_join(step)
        if isinstance(_arg(step, 'spill_dir'), str) and _arg(step, 'spill_dir'):
            if not filtering:
                max_matches = _positive_int(_arg(step, 'max_matches_per_row'))
                if max_matches is None:
                    raise RuntimeError(f"step '{op}' spill mode needs max_matches_per_row for byte-bounded mutating join")
            spill_bytes = _positive_int(_arg(step, 'spill_memory_bytes'))
            if spill_bytes is not None:
                return spill_bytes
            raise RuntimeError(f"step '{op}' spill mode needs spill_memory_bytes for a host byte estimate")
        if _arg(step, 'sorted') is True:
            if filtering:
                return 4096
            max_matches = _positive_int(_arg(step, 'max_matches_per_row'))
            if max_matches is None:
                raise RuntimeError(f"step '{op}' sorted=true needs max_matches_per_row for byte-bounded mutating join")
            return 4096 + max_matches * 512
        max_lookup_bytes = _positive_int(_arg(step, 'max_lookup_bytes'))
        if max_lookup_bytes is None:
            raise RuntimeError(f"step '{op}' needs max_lookup_bytes or sorted=true for byte-bounded native execution")
        est = 4096 + max_lookup_bytes * 4
        max_state_bytes = _positive_int(_arg(step, 'max_state_bytes'))
        if max_state_bytes is not None:
            return est + max_state_bytes
        max_lookup_rows = _positive_int(_arg(step, 'max_lookup_rows'))
        max_lookup_keys = _positive_int(_arg(step, 'max_lookup_keys'))
        if max_lookup_rows is not None:
            est += max_lookup_rows * 96
        if max_lookup_keys is not None:
            est += max_lookup_keys * 192
        return est

    raise RuntimeError(f"step '{op}' has no native byte estimator")


def _load_plan(plan_json: str | Dict[str, Any]) -> Dict[str, Any]:
    plan = json.loads(plan_json) if isinstance(plan_json, str) else plan_json
    if not isinstance(plan, dict):
        raise ValueError('plan must be a JSON object')
    steps = plan.get('steps')
    if not isinstance(steps, list):
        raise ValueError('plan must contain a steps array')
    return plan


def _blocking_steps(steps: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    return [s for s in steps if _memory_class(s) == 'blocking']


def _key_state_steps(steps: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    return [s for s in steps if _memory_class(s) == 'key_state']


def _pivot_step_can_use_native_spill(step: Dict[str, Any]) -> bool:
    if step.get('op') != 'pivot' or _arg(step, 'sorted') is True:
        return False
    return _array_len(_arg(step, 'categories')) > 0 or _positive_int(_arg(step, 'max_categories')) is not None


def _blocking_step_can_use_native_spill(step: Dict[str, Any]) -> bool:
    return step.get('op') == 'sort' or _pivot_step_can_use_native_spill(step)


def _blocking_plan_can_use_native_spill(blocking_steps: Iterable[Dict[str, Any]]) -> bool:
    blocking = list(blocking_steps)
    return bool(blocking) and all(_blocking_step_can_use_native_spill(step) for step in blocking)


def _key_state_step_can_use_native_spill(step: Dict[str, Any]) -> bool:
    return (
        step.get('op') in ('unique', 'dedup', 'group-agg', 'join', 'semi-join', 'anti-join', 'intersect', 'setdiff', 'intersect-all', 'setdiff-all', 'union')
    ) and _arg(step, 'sorted') is not True


def _inject_native_spill(plan: Dict[str, Any], spill_dir: str, memory_bytes: int | None) -> str:
    for step in plan['steps']:
        if not _blocking_step_can_use_native_spill(step) and not _key_state_step_can_use_native_spill(step):
            continue
        args = step.get('args')
        if not isinstance(args, dict):
            args = {}
            step['args'] = args
        args['spill_dir'] = spill_dir
        if memory_bytes is not None:
            args['spill_memory_bytes'] = memory_bytes
    return json.dumps(plan, separators=(',', ':'))


def prepare_native_plan(plan_json: str | Dict[str, Any], *,
                        allow_blocking: bool = False,
                        memory: Any = None,
                        spill_dir: str | None = None) -> str:
    """Validate host native execution policy and inject supported spill args."""
    plan = _load_plan(plan_json)
    steps = list(plan['steps'])
    blocking_steps = _blocking_steps(steps)
    blocking = blocking_steps[0] if blocking_steps else None
    key_steps = _key_state_steps(steps)
    memory_bytes = parse_memory_size(memory)
    can_spill_blocking = bool(spill_dir) and _blocking_plan_can_use_native_spill(blocking_steps)
    spillable_key_steps = [s for s in key_steps if _key_state_step_can_use_native_spill(s)] if spill_dir else []
    key_steps_for_estimate = [s for s in key_steps if s not in spillable_key_steps]

    if spill_dir:
        unsupported_key = next((s for s in key_steps if not _key_state_step_can_use_native_spill(s)), None)
        if unsupported_key is not None:
            raise RuntimeError(
                f"spill_dir was requested, but native spill is not implemented yet for key-state step "
                f"'{unsupported_key.get('op')}'"
            )
        unsupported = next((s for s in blocking_steps if not _blocking_step_can_use_native_spill(s)), None)
        if unsupported is not None:
            raise RuntimeError(
                f"spill_dir was requested, but native spill is not implemented yet for blocking step "
                f"'{unsupported.get('op')}'"
            )

    if memory_bytes is not None and blocking is not None and not can_spill_blocking:
        raise RuntimeError(
            f"memory {format_bytes(memory_bytes)} was requested, but native byte caps are not "
            f"implemented for blocking step '{blocking.get('op')}'"
        )

    if memory_bytes is not None and key_steps_for_estimate:
        total = 0
        for step in key_steps_for_estimate:
            total += estimate_key_state_step_bytes(step)
        if total > memory_bytes:
            raise RuntimeError(
                f"estimated native key-state memory {format_bytes(total)} exceeds memory "
                f"{format_bytes(memory_bytes)}"
            )

    if blocking is not None and not allow_blocking and not can_spill_blocking:
        raise RuntimeError(
            f"blocking step '{blocking.get('op')}' requires full input in native mode; "
            "pass allow_blocking=True for known-small data or use an external engine"
        )

    if can_spill_blocking or spillable_key_steps:
        return _inject_native_spill(plan, spill_dir, memory_bytes)
    return plan_json if isinstance(plan_json, str) else json.dumps(plan, separators=(',', ':'))


def validate_native_memory_policy(plan_json: str | Dict[str, Any], *,
                                  allow_blocking: bool = False,
                                  memory: Any = None,
                                  spill_dir: str | None = None) -> int | None:
    """Validate host native execution policy. Returns key-state bytes if estimated."""
    memory_bytes = parse_memory_size(memory)
    prepared = prepare_native_plan(plan_json, allow_blocking=allow_blocking, memory=memory, spill_dir=spill_dir)
    plan = _load_plan(prepared)
    key_steps = _key_state_steps(list(plan['steps']))

    if memory_bytes is None or not key_steps:
        return None
    return sum(estimate_key_state_step_bytes(step) for step in key_steps)
