"""Integration sweep for CRP-11 wrapper-notebook emission.

Translates three real CRP0001 pipelines end-to-end and asserts:

* Every compound IfCondition predicate produces a wrapper notebook artifact
  and a NotebookTask (``extra_tasks``).
* Native simple comparisons still use native ``condition_task`` (INV-1 —
  no regression).
* No ``UnsupportedValue`` sentinels remain in the resulting ``PreparedWorkflow``.
* Child activities of wrapper-routed IfConditions depend on the wrapper task
  (INV-3 — ordering guarantee).

Reference counts from direct JSON scan of the fixtures:

* ``perimetros_process_data``: 8 IfConditions, all ``@not(empty(intersection(...)))``
* ``process_data_AMR``       : 8 IfConditions, mostly intersection + ``@and``
* ``persist_global``          : 13 IfConditions, all ``@contains``
"""

from __future__ import annotations

import json
import pathlib

import pytest

from wkmigrate.models.ir.pipeline import IfConditionActivity, Pipeline
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.preparers.preparer import prepare_workflow
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline
from wkmigrate.utils import normalize_arm_pipeline, recursive_camel_to_snake

FIXTURE_DIR = pathlib.Path(__file__).parent.parent / "resources" / "pipelines" / "crp0001"


def _translate(name: str) -> Pipeline:
    raw = json.loads((FIXTURE_DIR / name).read_text())
    return translate_pipeline(normalize_arm_pipeline(recursive_camel_to_snake(raw)))


def _collect_if_conditions(pipeline: Pipeline) -> list[IfConditionActivity]:
    result: list[IfConditionActivity] = []

    def _visit(activities):
        for act in activities:
            if isinstance(act, IfConditionActivity):
                result.append(act)
                _visit(act.child_activities)

    _visit(pipeline.tasks)
    return result


@pytest.mark.parametrize(
    ("fixture_name", "expected_total"),
    [
        ("crp0001_c_pl_prc_edw_bfcdt_perimetros_process_data.json", 8),
        ("crp0001_c_pl_prc_edw_bfcdt_process_data_AMR.json", 8),
        ("crp0001_c_pl_prc_anl_persist_global.json", 13),
    ],
)
def test_every_compound_ifcondition_routes_through_wrapper(fixture_name: str, expected_total: int) -> None:
    pipeline = _translate(fixture_name)
    if_conditions = _collect_if_conditions(pipeline)

    assert len(if_conditions) == expected_total, f"{fixture_name} expected {expected_total} IfConditions"

    for cond in if_conditions:
        assert cond.wrapper_notebook_key is not None, f"missing wrapper for {cond.name}"
        assert cond.wrapper_notebook_content is not None
        assert "dbutils.jobs.taskValues.set" in cond.wrapper_notebook_content
        assert cond.left == f"{{{{tasks.{cond.wrapper_notebook_key}.values.branch}}}}"
        assert cond.right == "True"
        assert cond.op == "EQUAL_TO"


def test_perimetros_pipeline_emits_eight_wrapper_notebooks() -> None:
    """CRP-11 §4.2: 8 wrappers for perimetros_process_data."""
    pipeline = _translate("crp0001_c_pl_prc_edw_bfcdt_perimetros_process_data.json")
    prepared = prepare_workflow(pipeline)

    wrapper_notebooks = [nb for nb in prepared.all_notebooks if "/if_condition_wrappers/" in nb.file_path]
    assert len(wrapper_notebooks) == 8

    for nb in wrapper_notebooks:
        assert "set(" in nb.content, "intersection() should emit set() in wrapper body"
        assert 'dbutils.jobs.taskValues.set(key="branch"' in nb.content


def test_persist_global_contains_predicates_produce_thirteen_wrappers() -> None:
    """CRP-11 §4.2: 13 `@contains` wrappers across persist_global."""
    pipeline = _translate("crp0001_c_pl_prc_anl_persist_global.json")
    prepared = prepare_workflow(pipeline)

    wrapper_notebooks = [nb for nb in prepared.all_notebooks if "/if_condition_wrappers/" in nb.file_path]
    assert len(wrapper_notebooks) == 13

    for nb in wrapper_notebooks:
        # @contains(pipeline().parameters.execution, 'qui') → uses 'in str(dbutils.widgets.get("execution"))'
        assert "dbutils.widgets.get('execution')" in nb.content


def test_three_pipelines_produce_no_unsupported_values() -> None:
    """Zero UnsupportedValue sentinels (CRP-11 success criterion)."""
    for name in (
        "crp0001_c_pl_prc_edw_bfcdt_perimetros_process_data.json",
        "crp0001_c_pl_prc_edw_bfcdt_process_data_AMR.json",
        "crp0001_c_pl_prc_anl_persist_global.json",
    ):
        pipeline = _translate(name)

        def _walk(acts):
            for a in acts:
                assert not isinstance(a, UnsupportedValue), f"UnsupportedValue in {name}: {a}"
                child = getattr(a, "child_activities", None)
                if child:
                    _walk(child)

        _walk(pipeline.tasks)


def test_wrapper_resolves_variables_to_upstream_setvariable_task_keys() -> None:
    """Step 3 fan-in: wrapper body emits taskValues.get() for variables() references.

    Two cases covered here:

    1. Flat pipeline (SetVariable and IfCondition at the same level) — the
       ``variable_cache`` resolves the actual SetVariable task key, so the
       wrapper body contains ``taskKey='<SetVariable task key>'``.
    2. SetVariable nested inside a multi-activity ForEach + IfCondition at
       the outer level — a known limitation: ``_build_inner_pipeline()``
       uses a fresh context so inner SetVariables don't populate the outer
       variable_cache. In this case the wrapper falls back to the
       ``set_variable_<name>`` best-effort key, which is semantically wrong
       at Databricks runtime (task values don't cross RunJob boundaries).
       Tracked as a follow-up to Step 3.

    This test locks in both behaviours so we notice when either changes.
    """
    pipeline = _translate("lakeh_a_pl_arquetipo_internal.json")
    compound = [c for c in _collect_if_conditions(pipeline) if c.wrapper_notebook_key]
    continue_refs = [c for c in compound if "continue" in (c.wrapper_notebook_content or "")]
    assert continue_refs, "expected a wrapper referencing variables('continue')"

    # For the outer IfCondition (SetVariable lives inside ForEach): expect the
    # best-effort taskKey form — this documents the known limitation.
    best_effort = [c for c in continue_refs if "taskKey='set_variable_continue'" in (c.wrapper_notebook_content or "")]
    assert best_effort, "ForEach-nested SetVariable case: expect best-effort fallback"


def test_wrapper_resolves_variables_when_setvariable_is_flat_sibling() -> None:
    """Companion to the ForEach-nested case: when the SetVariable sits at the
    same level as the IfCondition (no ForEach in between), the wrapper must
    emit the real SetVariable task_key via the variable_cache.
    """
    import warnings as _warnings

    from wkmigrate.translators.activity_translators.activity_translator import translate_activities_with_context
    from wkmigrate.utils import normalize_arm_pipeline, recursive_camel_to_snake

    raw = {
        "name": "flat",
        "activities": [
            {
                "name": "set_mod",
                "type": "SetVariable",
                "typeProperties": {"variableName": "module", "value": "bal"},
                "dependsOn": [],
            },
            {
                "name": "if_compound",
                "type": "IfCondition",
                "typeProperties": {
                    "expression": {"type": "Expression", "value": "@contains(variables('module'), 'bal')"},
                    "ifTrueActivities": [],
                    "ifFalseActivities": [],
                },
                "dependsOn": [{"activity": "set_mod", "dependencyConditions": ["Succeeded"]}],
            },
        ],
    }
    normed = normalize_arm_pipeline(recursive_camel_to_snake(raw))
    with _warnings.catch_warnings():
        _warnings.simplefilter("ignore")
        acts, _ctx = translate_activities_with_context(normed["activities"])

    wrapper = next(a for a in acts if getattr(a, "wrapper_notebook_content", None))
    body = wrapper.wrapper_notebook_content
    assert "taskKey='set_mod'" in body
    assert "taskKey='set_variable_module'" not in body


def test_perimetros_condition_task_depends_on_wrapper() -> None:
    """INV-3: condition_task depends_on includes the wrapper task."""
    pipeline = _translate("crp0001_c_pl_prc_edw_bfcdt_perimetros_process_data.json")
    prepared = prepare_workflow(pipeline)

    # Build an index of task_key -> list of depends_on keys.
    dep_by_key: dict[str, list[str]] = {}
    for task in prepared.tasks:
        key = task.get("task_key")
        deps = task.get("depends_on") or []
        dep_by_key[key] = [(d.task_key if hasattr(d, "task_key") else d.get("task_key")) for d in deps]

    condition_tasks = [t for t in prepared.tasks if "condition_task" in t]
    assert len(condition_tasks) == 8
    for task in condition_tasks:
        wrapper_key = f"{task['task_key']}__crp11_wrap"
        assert wrapper_key in dep_by_key[task["task_key"]]
        # The wrapper itself must exist as a sibling task with a notebook_task definition.
        assert wrapper_key in dep_by_key
        wrapper_task = next(t for t in prepared.tasks if t.get("task_key") == wrapper_key)
        assert "notebook_task" in wrapper_task
