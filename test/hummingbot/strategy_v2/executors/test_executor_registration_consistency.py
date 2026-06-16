"""Regression guard for executor registration consistency.

Registering a new executor requires keeping FOUR sync points in lockstep:

1. ``ExecutorOrchestrator._executor_mapping`` (type-string -> executor class)
   in ``hummingbot/strategy_v2/executors/executor_orchestrator.py``
2. the module-level eager imports of those executor classes (same file) -- the
   orchestrator imports every executor at module load, so a single broken or
   missing import takes down EVERY V2 strategy (the failure class behind the
   ``test_no_dead_script_strategy_base_import`` guard).
3. the ``ExecutorConfigBase.type`` ``Literal[...]`` discriminator in
   ``hummingbot/strategy_v2/executors/data_types.py``
4. the ``AnyExecutorConfig`` ``Union[...]`` in
   ``hummingbot/strategy_v2/models/executors_info.py`` (the pydantic
   discriminated union used to deserialize executor configs).

Drift between any of these silently breaks executor creation or config
round-trip. This test parses all three source files with ``ast`` (pure stdlib,
no hummingbot import, no Cython, no runtime deps) so it runs in the lean
connector test environment. Runtime import/boot of the orchestrator is verified
separately under Docker (JEP-147 boot smoke).

Run with:
    cd hummingbot && PYENV_VERSION=py312 python -m pytest \
        test/hummingbot/strategy_v2/executors/test_executor_registration_consistency.py -v
"""
import ast
import pathlib

# <root>/test/hummingbot/strategy_v2/executors/<this file>
#   parents[4] == <root>; package lives at <root>/hummingbot/strategy_v2/...
_ROOT = pathlib.Path(__file__).resolve().parents[4]
_PKG = _ROOT / "hummingbot"

_ORCHESTRATOR = _PKG / "strategy_v2" / "executors" / "executor_orchestrator.py"
_DATA_TYPES = _PKG / "strategy_v2" / "executors" / "data_types.py"
_EXECUTORS_INFO = _PKG / "strategy_v2" / "models" / "executors_info.py"


def _parse(path: pathlib.Path) -> ast.Module:
    assert path.is_file(), f"source file not found: {path}"
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _find_classdef(tree: ast.Module, name: str) -> ast.ClassDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    raise AssertionError(f"class {name!r} not found in module")


def _module_level_imports(tree: ast.Module) -> dict[str, str]:
    """Map imported-name -> source module for top-level ``from x import y``."""
    imports: dict[str, str] = {}
    for node in tree.body:  # module level only
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                imports[alias.asname or alias.name] = node.module
    return imports


def _executor_mapping() -> dict[str, str]:
    """type-string -> executor class name, parsed from _executor_mapping dict."""
    tree = _parse(_ORCHESTRATOR)
    cls = _find_classdef(tree, "ExecutorOrchestrator")
    for node in cls.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "_executor_mapping" for t in node.targets
        ):
            assert isinstance(node.value, ast.Dict), "_executor_mapping is not a dict literal"
            result: dict[str, str] = {}
            for key, value in zip(node.value.keys, node.value.values):
                assert isinstance(key, ast.Constant) and isinstance(key.value, str), (
                    "non-string key in _executor_mapping"
                )
                assert isinstance(value, ast.Name), (
                    f"_executor_mapping[{key.value!r}] value is not a bare class name"
                )
                result[key.value] = value.id
            return result
    raise AssertionError("_executor_mapping not found on ExecutorOrchestrator")


def _literal_type_strings() -> set[str]:
    """type-string set from ExecutorConfigBase.type Literal[...]."""
    tree = _parse(_DATA_TYPES)
    cls = _find_classdef(tree, "ExecutorConfigBase")
    for node in cls.body:
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "type"
        ):
            ann = node.annotation
            assert isinstance(ann, ast.Subscript) and isinstance(ann.value, ast.Name) and (
                ann.value.id == "Literal"
            ), "ExecutorConfigBase.type is not a Literal[...]"
            elts = ann.slice.elts if isinstance(ann.slice, ast.Tuple) else [ann.slice]
            strings: set[str] = set()
            for elt in elts:
                assert isinstance(elt, ast.Constant) and isinstance(elt.value, str), (
                    "non-string entry in ExecutorConfigBase.type Literal"
                )
                strings.add(elt.value)
            return strings
    raise AssertionError("ExecutorConfigBase.type annotation not found")


def _union_config_names() -> set[str]:
    """Config class names from AnyExecutorConfig = Union[...]."""
    tree = _parse(_EXECUTORS_INFO)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "AnyExecutorConfig" for t in node.targets
        ):
            val = node.value
            assert isinstance(val, ast.Subscript) and isinstance(val.value, ast.Name) and (
                val.value.id == "Union"
            ), "AnyExecutorConfig is not a Union[...]"
            elts = val.slice.elts if isinstance(val.slice, ast.Tuple) else [val.slice]
            names: set[str] = set()
            for elt in elts:
                assert isinstance(elt, ast.Name), "non-name entry in AnyExecutorConfig Union"
                names.add(elt.id)
            return names
    raise AssertionError("AnyExecutorConfig assignment not found")


def test_mapping_keys_match_literal_discriminator():
    """_executor_mapping keys must equal the ExecutorConfigBase.type Literal."""
    mapping_keys = set(_executor_mapping().keys())
    literal = _literal_type_strings()
    assert mapping_keys == literal, (
        "executor type-string drift between _executor_mapping and "
        f"ExecutorConfigBase.type Literal.\n  only in mapping: {sorted(mapping_keys - literal)}"
        f"\n  only in Literal: {sorted(literal - mapping_keys)}"
    )


def test_every_mapping_class_is_imported_at_module_level():
    """Each _executor_mapping value class must be eagerly imported (no NameError)."""
    mapping = _executor_mapping()
    imports = _module_level_imports(_parse(_ORCHESTRATOR))
    missing = sorted({cls for cls in mapping.values() if cls not in imports})
    assert not missing, (
        "executor class(es) referenced in _executor_mapping but not imported at "
        f"module level (would NameError on use): {missing}"
    )


def test_every_imported_executor_module_resolves_to_a_file():
    """Eager-imported executor modules must resolve to real files (no dead imports)."""
    imports = _module_level_imports(_parse(_ORCHESTRATOR))
    mapping = _executor_mapping()
    offenders: list[str] = []
    for cls in mapping.values():
        module = imports[cls]
        rel = pathlib.Path(*module.split(".")).with_suffix(".py")
        if not (_ROOT / rel).is_file():
            offenders.append(f"{cls} <- {module} ({rel})")
    assert not offenders, (
        "executor class import path(s) do not resolve to a file on disk "
        f"(dead import): {offenders}"
    )


def test_every_mapping_executor_has_a_registered_config_in_union():
    """For each executor class X, X+'Config' must be in AnyExecutorConfig Union."""
    mapping = _executor_mapping()
    union = _union_config_names()
    missing = sorted(
        {f"{cls}Config" for cls in mapping.values() if f"{cls}Config" not in union}
    )
    assert not missing, (
        "executor(s) in _executor_mapping with no matching config in "
        f"AnyExecutorConfig Union (config round-trip would fail): {missing}"
    )
    # And no orphan configs lingering in the Union without a mapped executor.
    expected = {f"{cls}Config" for cls in mapping.values()}
    orphans = sorted(union - expected)
    assert not orphans, (
        "config(s) in AnyExecutorConfig Union with no executor in _executor_mapping "
        f"(orphan registration): {orphans}"
    )
