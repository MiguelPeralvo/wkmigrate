"""This module defines a translator for translating For Each activities.

Translators in this module normalize For Each activity payloads into internal representations.
Each translator must validate required fields, parse the activity's items expression, and emit
``UnsupportedValue`` objects for any unparsable inputs.
"""

import ast
import importlib
import re
import warnings
from wkmigrate.models.ir.pipeline import Activity, ForEachActivity, RunJobActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue


def translate_for_each_activity(activity: dict, base_kwargs: dict) -> ForEachActivity | UnsupportedValue:
    """
    Translates an ADF ForEach activity into a ``ForEachActivity`` object. ForEach activities are translated into For Each tasks in Databricks Lakeflow Jobs.

    This method returns an ``UnsupportedValue`` if the activity cannot be translated. This can be due to:
    * Missing or invalid items expression
    * Unparseable items expression

    Args:
        activity: ForEach activity definition as a ``dict``.
        base_kwargs: Common activity metadata from ``_build_base_activity_kwargs``.

    Returns:
        ``ForEachActivity`` representation of the ForEach task.
    """
    if "items" not in activity:
        return UnsupportedValue(value=activity, message="Missing property 'items' in ForEach activity")

    items = activity.get("items")
    if not isinstance(items, dict):
        return UnsupportedValue(
            value=activity, message=f"Invalid value '{items}' for property 'items' in ForEach activity"
        )

    items_string = _parse_for_each_items(items)
    if isinstance(items_string, UnsupportedValue):
        return items_string

    inner_activity_defs = activity.get("activities") or []
    if not inner_activity_defs:
        return UnsupportedValue(value=activity, message="ForEach activity requires at least one inner activity")

    if len(inner_activity_defs) == 1:
        inner_activity = _parse_for_each_task(inner_activity_defs[0])
        if isinstance(inner_activity, tuple):
            inner_activity = inner_activity[0]
        if isinstance(inner_activity, UnsupportedValue):
            return inner_activity
        for_each_task = inner_activity
    else:
        activity_name = activity.get("name") or "FOR_EACH"
        inner_job_name = f"{activity_name}_inner_activities"
        pipeline_translator = importlib.import_module("wkmigrate.translators.pipeline_translators.pipeline_translator")
        inner_pipeline = pipeline_translator.translate_pipeline(
            {
                "name": inner_job_name,
                "activities": inner_activity_defs,
                "parameters": None,
                "trigger": None,
                "tags": {},
            }
        )
        for_each_task = RunJobActivity(
            name=inner_job_name,
            task_key=inner_job_name,
            pipeline=inner_pipeline,
        )

    return ForEachActivity(
        **base_kwargs,
        items_string=items_string,
        for_each_task=for_each_task,
        concurrency=activity.get("batch_count"),
    )


def _parse_for_each_items(items: dict) -> str | UnsupportedValue:
    """
    Parses a list of items passed to a ForEach task into a serialized list expression.

    Args:
        items: Expression describing ForEach items.

    Returns:
        Serialized list expression understood by Databricks Jobs.
    """
    if "value" not in items:
        return UnsupportedValue(value=items, message="Missing property 'value' in ForEach activity 'items'")
    value = items.get("value")
    if value is None:
        return UnsupportedValue(value=items, message="Missing property 'value' in ForEach activity 'items'")
    # TODO: Move all dynamic function patterns to a common enum list
    array_pattern = r"@array\(\[(.+)\]\)"
    match = re.match(string=value, pattern=array_pattern)
    if match:
        matched_item = match.group(1)
        return _parse_array_string(matched_item)

    create_array_pattern = r"@createArray\((.+)\)"
    match = re.match(string=value, pattern=create_array_pattern)
    if match:
        matched_item = match.group(1)
        list_items = ast.literal_eval(matched_item)
        quoted_items = ",".join([f'"{item}"' for item in list_items])
        return _parse_array_string(quoted_items)
    return UnsupportedValue(value=items, message=f"Unsupported array expression '{value}' in ForEach activity 'items'")


def _parse_array_string(array_string: str) -> str:
    """
    Parses an array string into a JSON-safe format.

    Args:
        array_string: Raw array expression emitted by ADF.

    Returns:
        JSON-safe representation of the array.
    """
    double_quote_character = '"'
    single_quote_character = "'"
    test = f'["{'","'.join([f'{element.replace(single_quote_character, "").replace(double_quote_character, "")}' for element in array_string.split(',')])}"]'
    return test


def _parse_for_each_task(task: dict) -> Activity | tuple[Activity, list[Activity]]:
    """
    Parses a single task definition within a ForEach task into an ``Activity`` object and a list of downstream tasks.

    Args:
        task: Nested activity definition from the ADF pipeline.

    Returns:
        Translated activity, optionally paired with additional downstream tasks.
    """
    task_with_filtered_parameters = _filter_parameters(task)
    if isinstance(task_with_filtered_parameters, UnsupportedValue):
        # Fall back to translating the original task; translate_activity will normalize
        # any unsupported results into placeholder activities.
        task_with_filtered_parameters = task
    activity_translator = importlib.import_module("wkmigrate.translators.activity_translators.activity_translator")
    return activity_translator.translate_activity(task_with_filtered_parameters)


def _filter_parameters(activity: dict) -> dict | UnsupportedValue:
    """
    Filters redundant parameters from an activity definition.

    Args:
        activity: Activity definition as a dictionary.

    Returns:
        Filtered activity definition with redundant parameters removed.

    Raises:
        NotTranslatableWarning: If a base parameter is not provided.
    """
    if "base_parameters" not in activity:
        return UnsupportedValue(value=activity, message="Missing property 'base_parameters' for ForEach inner activity")
    base_parameters = activity.get("base_parameters")
    if base_parameters is None:
        return UnsupportedValue(value=activity, message="Property 'base_parameters' is None for ForEach inner activity")
    parameters = _filter_parameters(base_parameters)
    if isinstance(parameters, UnsupportedValue):
        return parameters
    if parameters is None:
        return {}
    filtered_parameters = {}
    for name, expression in parameters.items():
        if expression is not None and expression.get("value") == "@item()":
            warnings.warn(
                f"Removing redundant parameter {name} with value {expression.get('value')}",
                stacklevel=3,
            )
            continue
        filtered_parameters.update({name: expression})
    activity["base_parameters"] = filtered_parameters
    return activity
