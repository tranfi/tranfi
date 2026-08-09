#!/usr/bin/env python3
"""Validate the language-neutral prepared-transform V1 vector corpus."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import struct
from fractions import Fraction
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
VECTOR_DIR = ROOT / "vectors"
SEMANTIC_PATH = VECTOR_DIR / "prepared_transform_v1.json"
TFTR_PATH = VECTOR_DIR / "tftr_malformed_v1.json"
HEX64 = re.compile(r"^[0-9a-f]{16}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")

OK = 0
INVALID_RECIPE = 101
INSUFFICIENT_DATA = 103
RESOURCE_LIMIT = 104
UNSUPPORTED_VERSION = 105
CORRUPT_PLAN = 106
NUMERIC_DOMAIN = 107
UNKNOWN_CATEGORY = 108
UNSUPPORTED_RUNTIME = 113


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def load_json(path: Path) -> dict[str, Any]:
    def no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            require(key not in result, f"{path}: duplicate key {key!r}")
            result[key] = value
        return result

    with path.open("r", encoding="utf-8") as stream:
        value = json.load(stream, object_pairs_hook=no_duplicates)
    require(isinstance(value, dict), f"{path}: root must be an object")
    return value


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def tagged_f64(bits: str) -> dict[str, str]:
    require(bool(HEX64.fullmatch(bits)), f"invalid float64 bits {bits!r}")
    return {"t": "f64", "v": bits}


def validate_cell(cell: Any, *, allow_nonfinite: bool = False) -> None:
    if cell is None:
        return
    require(isinstance(cell, str) and HEX64.fullmatch(cell) is not None,
            f"invalid logical float64 cell {cell!r}")
    exponent = (int(cell, 16) >> 52) & 0x7FF
    if not allow_nonfinite:
        require(exponent != 0x7FF, f"nonfinite cell outside an error vector: {cell}")


def validate_rows(rows: Any, width: int, *, allow_nonfinite: bool = False) -> None:
    require(isinstance(rows, list), "rows must be an array")
    for index, row in enumerate(rows):
        require(isinstance(row, list) and len(row) == width,
                f"row {index} must have width {width}")
        for cell in row:
            validate_cell(cell, allow_nonfinite=allow_nonfinite)


def float_from_bits(bits: str) -> float:
    require(HEX64.fullmatch(bits) is not None, f"invalid binary64 bits {bits!r}")
    return struct.unpack(">d", bytes.fromhex(bits))[0]


def validate_sqrt_case(case: Any) -> None:
    require(isinstance(case, dict) and set(case) == {
        "expected", "id", "input", "roundDirection",
    }, "sqrt case shape")
    source = float_from_bits(case["input"])
    rounded = float_from_bits(case["expected"])
    require(math.isfinite(source) and source >= 0.0, f"{case['id']}: sqrt input")
    require(math.isfinite(rounded) and rounded >= 0.0, f"{case['id']}: sqrt output")
    source_exact = Fraction.from_float(source)
    rounded_exact = Fraction.from_float(rounded)
    previous = Fraction.from_float(math.nextafter(rounded, -math.inf))
    following = Fraction.from_float(math.nextafter(rounded, math.inf))
    lower = (previous + rounded_exact) / 2
    upper = (rounded_exact + following) / 2
    even = (int(case["expected"], 16) & 1) == 0
    require(source_exact > lower * lower
            or (source_exact == lower * lower and even),
            f"{case['id']}: result rounds below the lower midpoint")
    require(source_exact < upper * upper
            or (source_exact == upper * upper and even),
            f"{case['id']}: result rounds above the upper midpoint")
    square = rounded_exact * rounded_exact
    actual_direction = "exact"
    if square > source_exact:
        actual_direction = "up"
    elif square < source_exact:
        actual_direction = "down"
    require(case["roundDirection"] == actual_direction,
            f"{case['id']}: round direction drift")


def validate_recipe(name: str, recipe: Any) -> int:
    require(isinstance(recipe, dict), f"recipe {name}: object required")
    require(set(recipe) == {
        "columns", "format", "outputDtype", "policyVersion",
        "semanticLimits", "version",
    }, f"recipe {name}: wrong top-level keys")
    require(recipe["format"] == "tranfi.transform-recipe", f"recipe {name}: format")
    require(recipe["outputDtype"] == "float64", f"recipe {name}: output dtype")
    require(recipe["policyVersion"] == 1 and recipe["version"] == 1,
            f"recipe {name}: version")
    require(recipe["semanticLimits"] == {
        "maxOutputColumns": 65536,
        "maxOutputElementsPerApply": 134217728,
    }, f"recipe {name}: semantic limits")
    columns = recipe["columns"]
    require(isinstance(columns, list) and columns, f"recipe {name}: columns")
    source_ids: set[str] = set()
    for index, column in enumerate(columns):
        require(isinstance(column, dict) and set(column) == {
            "categorical", "kind", "numeric", "sourceId",
        }, f"recipe {name}: column {index} shape")
        source_id = column["sourceId"]
        require(isinstance(source_id, str) and source_id and source_id not in source_ids,
                f"recipe {name}: sourceId {source_id!r}")
        source_ids.add(source_id)
        kind = column["kind"]
        require(isinstance(kind, dict) and set(kind) == {
            "maxCategories", "op", "rule", "value",
        }, f"recipe {name}: kind shape")
        if kind["op"] == "declared":
            require(kind["maxCategories"] is None and kind["rule"] is None,
                    f"recipe {name}: declared kind extras")
            require(kind["value"] in {"numeric", "categorical"},
                    f"recipe {name}: declared kind value")
            if kind["value"] == "numeric":
                require(column["numeric"] is not None and column["categorical"] is None,
                        f"recipe {name}: numeric branch")
            else:
                require(column["categorical"] is not None and column["numeric"] is None,
                        f"recipe {name}: categorical branch")
        else:
            require(kind["op"] == "infer" and kind["value"] is None,
                    f"recipe {name}: infer kind")
            require(kind["rule"] == "finite-integer-cardinality-v1",
                    f"recipe {name}: infer rule")
            require(isinstance(kind["maxCategories"], int)
                    and kind["maxCategories"] >= 2,
                    f"recipe {name}: infer maxCategories")
            require(column["numeric"] is not None and column["categorical"] is not None,
                    f"recipe {name}: inferred branches")
        categorical = column["categorical"]
        if categorical is not None:
            require(isinstance(categorical, dict) and set(categorical) == {
                "encode", "impute",
            }, f"recipe {name}: categorical shape")
            encode = categorical["encode"]
            require(isinstance(encode, dict) and set(encode) == {
                "categories", "op", "sentinelLabel", "unknown",
            }, f"recipe {name}: categorical encode shape")
            require(encode["categories"] == "discover",
                    f"recipe {name}: only discovered categories are in V1 vectors")
            if encode["op"] == "none":
                require(encode["sentinelLabel"] is None
                        and encode["unknown"] is None,
                        f"recipe {name}: encode-none policy")
            elif encode["op"] == "label":
                require(encode["unknown"] in {"error", "sentinel", "other"},
                        f"recipe {name}: label unknown policy")
                sentinel = encode["sentinelLabel"]
                if encode["unknown"] == "sentinel":
                    require(isinstance(sentinel, int) and not isinstance(sentinel, bool)
                            and abs(sentinel) <= 9007199254740991,
                            f"recipe {name}: safe integer sentinel required")
                else:
                    require(sentinel is None,
                            f"recipe {name}: sentinel is exclusive to sentinel policy")
            else:
                require(encode["op"] == "onehot"
                        and encode["sentinelLabel"] is None
                        and encode["unknown"] == "all_zero",
                        f"recipe {name}: one-hot vector shape")
            impute = categorical["impute"]
            require(isinstance(impute, dict) and set(impute) == {
                "allMissing", "constant", "op",
            }, f"recipe {name}: categorical impute shape")
            if impute["op"] == "mode":
                require(impute["allMissing"] in {"error", "zero"}
                        and impute["constant"] is None,
                        f"recipe {name}: categorical mode policy")
            else:
                require(impute == {
                    "allMissing": None, "constant": None, "op": "none",
                }, f"recipe {name}: categorical impute-none policy")
    return len(columns)


def validate_semantic_vectors(document: dict[str, Any]) -> tuple[int, int, int]:
    require(document.get("format") == "tranfi.prepared-transform-vectors", "semantic format")
    require(document.get("version") == 1, "semantic version")
    codes = document.get("errorCodes")
    require(codes == {
        "corruptPlan": CORRUPT_PLAN,
        "insufficientData": INSUFFICIENT_DATA,
        "invalidRecipe": INVALID_RECIPE,
        "numericDomain": NUMERIC_DOMAIN,
        "resourceLimit": RESOURCE_LIMIT,
        "unknownCategory": UNKNOWN_CATEGORY,
        "unsupportedRuntime": UNSUPPORTED_RUNTIME,
        "unsupportedVersion": UNSUPPORTED_VERSION,
    }, "error code table drift")

    recipes = document.get("recipes")
    require(isinstance(recipes, dict) and recipes, "recipes object required")
    widths = {name: validate_recipe(name, recipe) for name, recipe in recipes.items()}

    semantic_cases = document.get("semanticCases")
    require(isinstance(semantic_cases, list) and semantic_cases, "semantic cases required")
    case_ids: set[str] = set()
    for case in semantic_cases:
        require(isinstance(case, dict), "semantic case must be an object")
        case_id = case.get("id")
        require(isinstance(case_id, str) and case_id not in case_ids,
                f"duplicate/invalid semantic id {case_id!r}")
        case_ids.add(case_id)
        recipe_name = case.get("recipe")
        require(recipe_name in recipes, f"{case_id}: unknown recipe")
        width = widths[recipe_name]
        analyze = case.get("analyze")
        require(isinstance(analyze, dict), f"{case_id}: analyze")
        rows = analyze.get("rows")
        validate_rows(rows, width)
        splits = analyze.get("chunkSplits")
        require(isinstance(splits, list) and splits, f"{case_id}: chunk splits")
        for split in splits:
            require(isinstance(split, list) and split, f"{case_id}: split shape")
            require(all(isinstance(size, int) and size >= 0 for size in split),
                    f"{case_id}: split sizes")
            require(sum(split) == len(rows), f"{case_id}: split does not cover rows")

        apply = case.get("apply")
        require(isinstance(apply, dict), f"{case_id}: apply")
        validate_rows(apply.get("rows"), width)
        expected_rows = apply.get("expectedRows")
        require(isinstance(expected_rows, list), f"{case_id}: expected rows")
        expected_width = apply.get("expectedColumns")
        if expected_width is None:
            expected_width = len(expected_rows[0]) if expected_rows else width
        validate_rows(expected_rows, expected_width, allow_nonfinite=True)
        if "expectedError" in case:
            error = case["expectedError"]
            require(set(error) == {"code", "phase"}
                    and error["phase"] in {"finalize", "apply"},
                    f"{case_id}: expected error shape")
            require(error["code"] in codes.values(), f"{case_id}: expected error code")
            if error["phase"] == "apply":
                require("expectedPlan" in case,
                        f"{case_id}: apply error requires plan expectation")
        else:
            require("expectedPlan" in case, f"{case_id}: plan expectation")
        recipe_column = recipes[recipe_name]["columns"][0]
        encode = (recipe_column["categorical"] or {}).get("encode")
        expected_plan = case.get("expectedPlan")
        if (encode is not None and encode["op"] == "label"
                and expected_plan is not None
                and expected_plan.get("kind") == "categorical"):
            require(expected_plan.get("outputIds") == ["x0%3Alabel"],
                    f"{case_id}: generated label output ID")
            require(expected_plan.get("unknown") == encode["unknown"],
                    f"{case_id}: label unknown policy")
            require(expected_plan.get("sentinelLabel") == encode["sentinelLabel"],
                    f"{case_id}: label sentinel")

    sqrt_cases = document.get("sqrtCases")
    require(isinstance(sqrt_cases, list) and sqrt_cases, "sqrt cases required")
    sqrt_ids: set[str] = set()
    for case in sqrt_cases:
        validate_sqrt_case(case)
        require(case["id"] not in sqrt_ids, f"duplicate sqrt id {case['id']!r}")
        sqrt_ids.add(case["id"])
    require({"normal-round-up", "normal-round-down", "minimum-normal-exact-root",
             "minimum-subnormal-exact-root", "subnormal-variance-round-down"}
            <= sqrt_ids, "sqrt edge corpus incomplete")

    validation_cases = document.get("validationCases")
    require(isinstance(validation_cases, list) and validation_cases,
            "validation cases required")
    validation_ids: set[str] = set()
    for case in validation_cases:
        case_id = case.get("id")
        require(isinstance(case_id, str) and case_id not in validation_ids,
                f"duplicate/invalid validation id {case_id!r}")
        validation_ids.add(case_id)
        require(case.get("expectedCode") in {OK, INVALID_RECIPE, INSUFFICIENT_DATA,
                                             NUMERIC_DOMAIN},
                f"{case_id}: unexpected code")
        mutation = case.get("mutation")
        recipe_name = case.get("recipe")
        if mutation is not None:
            require(isinstance(mutation, dict)
                    and set(mutation) == {"path", "recipe", "value"},
                    f"{case_id}: mutation shape")
            recipe_name = mutation["recipe"]
        if recipe_name is not None:
            require(recipe_name in recipes, f"{case_id}: unknown recipe")
        if "rows" in case:
            require(recipe_name is not None, f"{case_id}: rows require recipe")
            validate_rows(case["rows"], widths[recipe_name], allow_nonfinite=True)

    require("max-categories-one-invalid" in validation_ids, "missing maxCategories=1 boundary")
    require("max-categories-two-valid" in validation_ids, "missing maxCategories=2 boundary")
    require({
        "fractional-sentinel-label-invalid",
        "out-of-safe-range-sentinel-label-invalid",
    } <= validation_ids, "missing label sentinel validation boundaries")
    require({
        "categorical-mode-tie-smallest-signed-zero",
        "categorical-mode-subnormal-order",
        "categorical-mode-all-missing-zero",
        "categorical-mode-all-missing-error",
        "categorical-mode-empty-analysis-zero-policy",
        "categorical-mode-unknown-apply",
        "categorical-mode-tie-smallest-label-sentinel",
        "categorical-mode-label-unknown-error",
        "categorical-mode-label-unknown-other",
        "categorical-mode-label-all-missing-zero-other",
        "categorical-mode-label-sentinel-collision",
    } <= case_ids, "categorical mode edge corpus incomplete")
    return len(semantic_cases), len(validation_cases), len(sqrt_cases)


def tftr(payload: bytes, *, version: int = 1, flags: int = 0,
         header_length: int = 52, payload_length: int | None = None,
         digest: bytes | None = None, magic: bytes = b"TFTR") -> bytes:
    if payload_length is None:
        payload_length = len(payload)
    if digest is None:
        digest = hashlib.sha256(payload).digest()
    header = (
        magic
        + struct.pack("<HHIQ", version, flags, header_length, payload_length)
        + digest
    )
    require(len(header) == 52, "TFTR header construction drift")
    return header + payload


def mutate_payload(base: dict[str, Any], mutation: str) -> bytes:
    value = copy.deepcopy(base)
    if mutation == "unknown_top_key_canonical_rehashed":
        value["unknown"] = 1
    elif mutation == "nonfinite_scale_canonical_rehashed":
        value["steps"][0]["numeric"]["normalize"]["scale"] = tagged_f64(
            "7ff0000000000000"
        )
    elif mutation == "recipe_sha_mismatch_canonical_rehashed":
        value["recipeSha256"] = "0" * 64
    elif mutation == "step_source_mismatch_canonical_rehashed":
        value["steps"][0]["sourceId"] = "x1"
    elif mutation == "plan_version_2_canonical_rehashed":
        value["version"] = 2
    else:
        raise AssertionError(f"unknown canonical mutation {mutation}")
    return canonical_bytes(value)


def mutate_tftr(base_object: dict[str, Any], base_bytes: bytes, mutation: str) -> bytes:
    payload = canonical_bytes(base_object)
    if mutation == "none":
        return base_bytes
    if mutation == "bad_magic":
        return b"XFTR" + base_bytes[4:]
    if mutation == "envelope_version_2":
        return tftr(payload, version=2)
    if mutation == "envelope_flags_1":
        return tftr(payload, flags=1)
    if mutation == "header_length_51":
        return tftr(payload, header_length=51)
    if mutation == "payload_length_max":
        return tftr(payload, payload_length=(1 << 64) - 1)
    if mutation == "truncate_one":
        return base_bytes[:-1]
    if mutation == "append_zero":
        return base_bytes + b"\0"
    if mutation == "flip_payload_without_hash":
        value = bytearray(base_bytes)
        value[-1] ^= 1
        return bytes(value)
    if mutation == "reordered_top_level_rehashed":
        keys = list(reversed(sorted(base_object)))
        reordered = "{" + ",".join(
            json.dumps(key) + ":" + canonical_bytes(base_object[key]).decode("utf-8")
            for key in keys
        ) + "}"
        return tftr(reordered.encode("utf-8"))
    if mutation == "whitespace_rehashed":
        return tftr(payload + b"\n")
    if mutation == "alternate_escape_rehashed":
        altered = payload.replace(b'"x0"', b'"\\u00780"', 1)
        require(altered != payload, "alternate escape mutation did not apply")
        return tftr(altered)
    if mutation == "duplicate_top_key_rehashed":
        return tftr(payload[:-1] + b',"version":1}')
    if mutation.endswith("_canonical_rehashed"):
        return tftr(mutate_payload(base_object, mutation))
    raise AssertionError(f"unknown TFTR mutation {mutation}")


def payload_from_tftr(value: bytes) -> bytes:
    require(len(value) >= 52, "mutated TFTR too short for payload inspection")
    return value[52:]


def validate_tftr_vectors(document: dict[str, Any]) -> int:
    require(document.get("format") == "tranfi.tftr-malformed-vectors", "TFTR format")
    require(document.get("version") == 1, "TFTR vector version")
    base = document.get("base")
    require(isinstance(base, dict), "TFTR base required")
    payload_object = copy.deepcopy(base["canonicalPayload"])
    recipe_fingerprint_object = {
        "inputSchema": payload_object["inputSchema"],
        "policyVersion": 1,
        "recipe": payload_object["recipe"],
    }
    recipe_sha = hashlib.sha256(canonical_bytes(recipe_fingerprint_object)).hexdigest()
    require(SHA256.fullmatch(base["recipeSha256"]) is not None, "recipe SHA shape")
    require(recipe_sha == base["recipeSha256"], "recipe SHA drift")
    require(payload_object["recipeSha256"] == recipe_sha, "payload recipe SHA drift")
    payload = canonical_bytes(payload_object)
    payload_sha = hashlib.sha256(payload).hexdigest()
    require(payload_sha == base["payloadSha256"], "payload SHA drift")
    base_bytes = tftr(payload)
    require(base_bytes.hex() == base["tftrHex"], "canonical TFTR bytes drift")
    require(len(base_bytes) == 52 + len(payload), "TFTR length drift")

    expected_by_mutation = {
        "none": OK,
        "bad_magic": CORRUPT_PLAN,
        "envelope_version_2": UNSUPPORTED_VERSION,
        "envelope_flags_1": UNSUPPORTED_VERSION,
        "header_length_51": CORRUPT_PLAN,
        "payload_length_max": CORRUPT_PLAN,
        "truncate_one": CORRUPT_PLAN,
        "append_zero": CORRUPT_PLAN,
        "flip_payload_without_hash": CORRUPT_PLAN,
        "reordered_top_level_rehashed": CORRUPT_PLAN,
        "whitespace_rehashed": CORRUPT_PLAN,
        "alternate_escape_rehashed": CORRUPT_PLAN,
        "duplicate_top_key_rehashed": CORRUPT_PLAN,
        "unknown_top_key_canonical_rehashed": CORRUPT_PLAN,
        "nonfinite_scale_canonical_rehashed": CORRUPT_PLAN,
        "recipe_sha_mismatch_canonical_rehashed": CORRUPT_PLAN,
        "step_source_mismatch_canonical_rehashed": CORRUPT_PLAN,
        "plan_version_2_canonical_rehashed": UNSUPPORTED_VERSION,
    }
    noncanonical_equivalents = {
        "reordered_top_level_rehashed",
        "whitespace_rehashed",
        "alternate_escape_rehashed",
        "duplicate_top_key_rehashed",
    }
    cases = document.get("cases")
    require(isinstance(cases, list) and cases, "TFTR cases required")
    case_ids: set[str] = set()
    seen_mutations: set[str] = set()
    for case in cases:
        case_id = case.get("id")
        mutation = case.get("mutation")
        require(isinstance(case_id, str) and case_id not in case_ids,
                f"duplicate/invalid TFTR id {case_id!r}")
        case_ids.add(case_id)
        require(mutation in expected_by_mutation, f"{case_id}: unknown mutation")
        expected = expected_by_mutation[mutation]
        if "hostLimits" in case:
            require(mutation == "none" and case["hostLimits"] == {"maxPlanBytes": 52},
                    f"{case_id}: host limit vector")
            expected = RESOURCE_LIMIT
        if "runtimeProbe" in case:
            require(mutation == "none"
                    and case["runtimeProbe"] == "gradual_underflow_unavailable",
                    f"{case_id}: runtime probe vector")
            expected = UNSUPPORTED_RUNTIME
        require(case.get("expectedCode") == expected, f"{case_id}: expected code drift")
        mutated = mutate_tftr(payload_object, base_bytes, mutation)
        if mutation != "none":
            require(mutated != base_bytes, f"{case_id}: mutation did not change bytes")
            seen_mutations.add(mutation)
        if mutation in noncanonical_equivalents:
            decoded = json.loads(payload_from_tftr(mutated).decode("utf-8"))
            require(decoded == payload_object, f"{case_id}: not semantically equivalent")
            require(payload_from_tftr(mutated) != payload,
                    f"{case_id}: unexpectedly canonical")

    require(seen_mutations == set(expected_by_mutation) - {"none"},
            "malformed mutation corpus is incomplete")
    return len(cases)


def main() -> None:
    semantic = load_json(SEMANTIC_PATH)
    malformed = load_json(TFTR_PATH)
    semantic_count, validation_count, sqrt_count = validate_semantic_vectors(semantic)
    malformed_count = validate_tftr_vectors(malformed)
    print(
        "prepared-transform vectors OK: "
        f"{semantic_count} semantic, {validation_count} validation, "
        f"{sqrt_count} sqrt, "
        f"{malformed_count} TFTR cases"
    )


if __name__ == "__main__":
    main()
