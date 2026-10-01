"""Local Qlik Data Flow (.dfw) draft generator.

This is an intentionally small prototype. It derives a .dfw graph from the
user-provided Qlik export and emits only processor configurations demonstrated
by the bundled reference export.
"""

from __future__ import annotations

import copy
import json
import re
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent
TEMPLATE_PATH = ROOT / "examples" / "myflow1-expanded.dfw"
INDEX_PATH = ROOT / "static" / "index.html"
HOST = "127.0.0.1"
PORT = 8765
MAX_BODY = 2_000_000
DEFAULT_MODELS = {
    "openai": "gpt-5.6-luna",
    "anthropic": "claude-sonnet-5",
}

COMPONENT_TO_OPERATION = {
    "data-cleansing": "replace_empty",
    "split-field": "split_text",
    "date": "format_dates",
    "strings": "strings_upper",
    "math": "math_add",
    "join": "join",
    "aggregate": "aggregate_sum",
    "fork": "fork",
}

PLAN_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "clarification": {"type": "string"},
        "steps": {
            "type": "array",
            "items": {
                "anyOf": [
                    {"type": "object", "properties": {
                        "type": {"type": "string", "enum": ["replace_empty"]},
                        "columns": {"type": "array", "items": {"type": "string"}},
                        "default_value": {"type": "string"},
                    }, "required": ["type", "columns", "default_value"], "additionalProperties": False},
                    {"type": "object", "properties": {
                        "type": {"type": "string", "enum": ["split_text"]},
                        "column": {"type": "string"},
                        "parts": {"type": "integer"},
                        "separator": {"type": "string", "enum": ["SPACE", "CUSTOM"]},
                        "custom_separator": {"type": "string"},
                    }, "required": ["type", "column", "parts", "separator", "custom_separator"], "additionalProperties": False},
                    {"type": "object", "properties": {
                        "type": {"type": "string", "enum": ["format_dates"]},
                        "columns": {"type": "array", "items": {"type": "string"}},
                        "date_pattern": {"type": "string"},
                        "create_new_column": {"type": "boolean"},
                        "new_column_rename": {"type": "string"},
                    }, "required": ["type", "columns", "date_pattern", "create_new_column", "new_column_rename"], "additionalProperties": False},
                    {"type": "object", "properties": {
                        "type": {"type": "string", "enum": ["strings_upper"]},
                        "columns": {"type": "array", "items": {"type": "string"}},
                        "create_new_column": {"type": "boolean"},
                        "new_column_name": {"type": "string"},
                    }, "required": ["type", "columns", "create_new_column", "new_column_name"], "additionalProperties": False},
                    {"type": "object", "properties": {
                        "type": {"type": "string", "enum": ["math_add"]},
                        "column": {"type": "string"},
                        "mode": {"type": "string", "enum": ["WITH_FIELD", "WITH_VALUE"]},
                        "operand_field": {"type": "string"},
                        "constant_value": {"type": "number"},
                        "create_new_column": {"type": "boolean"},
                        "new_column_name": {"type": "string"},
                    }, "required": ["type", "column", "mode", "operand_field", "constant_value", "create_new_column", "new_column_name"], "additionalProperties": False},
                    {"type": "object", "properties": {
                        "type": {"type": "string", "enum": ["join"]},
                        "join_type": {"type": "string", "enum": ["INNER_JOIN"]},
                        "left_key": {"type": "string"},
                        "right_key": {"type": "string"},
                        "keep_both_keys": {"type": "boolean"},
                    }, "required": ["type", "join_type", "left_key", "right_key", "keep_both_keys"], "additionalProperties": False},
                    {"type": "object", "properties": {
                        "type": {"type": "string", "enum": ["aggregate_sum"]},
                        "group_by": {"type": "array", "items": {"type": "string"}},
                        "column": {"type": "string"},
                        "output_name": {"type": "string"},
                    }, "required": ["type", "group_by", "column", "output_name"], "additionalProperties": False},
                ]
            },
        },
    },
    "required": ["summary", "clarification", "steps"],
    "additionalProperties": False,
}


class FlowError(Exception):
    pass


def read_json_template() -> dict:
    try:
        with TEMPLATE_PATH.open("r", encoding="utf-8-sig") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        raise FlowError(f"Could not read the sample flow: {exc}") from exc


def get_template_fields(template: dict) -> list[str]:
    fields: list[str] = []
    for source in template.get("sources", []):
        for table in source.get("tables", []):
            for field in table.get("fields", []):
                if field.get("selected", True) and field.get("name"):
                    fields.append(str(field["name"]))
    return list(dict.fromkeys(fields))


def check_template(template: dict) -> tuple[dict, dict, list[str]]:
    dataflow = template.get("dataflow")
    if not isinstance(dataflow, dict):
        raise FlowError("The selected file does not look like a Qlik .dfw export.")
    graph = dataflow.get("graph") or {}
    nodes = graph.get("nodes") or []
    sources = [n for n in nodes if n.get("type") == "source"]
    targets = [n for n in nodes if n.get("type") == "target"]
    processors = [n for n in nodes if n.get("type") == "processor"]
    if len(sources) != 1 or len(targets) != 1:
        raise FlowError("This prototype supports templates with exactly one source and one target.")
    unknown = [
        n.get("component", {}).get("type", "unknown")
        for n in processors
        if n.get("component", {}).get("type") not in COMPONENT_TO_OPERATION
    ]
    if unknown:
        raise FlowError(
            "The selected template contains processor types this prototype does not handle: "
            + ", ".join(unknown)
            + ". Use a source-to-target template or the provided sample."
        )
    fields = get_template_fields(template)
    if not fields:
        raise FlowError("No selected fields were found in the template's source metadata.")
    return sources[0], targets[0], fields


def request_plan(provider: str, api_key: str, model: str, prompt: str, fields: list[str], correction: str = "") -> dict:
    allowed = {
        "replace_empty": {
            "description": "Replace EMPTY values in one or more fields with a default value.",
            "shape": {"type": "replace_empty", "columns": ["field"], "default_value": "0"},
        },
        "split_text": {
            "description": "Split one text field into a requested number of parts. Supports SPACE or CUSTOM separators.",
            "shape": {"type": "split_text", "column": "field", "parts": 2, "separator": "SPACE", "custom_separator": ""},
        },
        "format_dates": {
            "description": "Format one or more date fields using the sample's YYYYMMDD_DASH output format.",
            "shape": {"type": "format_dates", "columns": ["date_field"], "date_pattern": "YYYY-MM-DD hh:mm:ss.fff", "create_new_column": True, "new_column_rename": ""},
        },
        "strings_upper": {
            "description": "Convert one or more string fields to uppercase, in place or in a new field.",
            "shape": {"type": "strings_upper", "columns": ["field"], "create_new_column": False, "new_column_name": ""},
        },
        "math_add": {
            "description": "Add another numeric field or a numeric constant to a numeric field.",
            "shape": {"type": "math_add", "column": "field", "mode": "WITH_FIELD", "operand_field": "other_field", "constant_value": 0, "create_new_column": False, "new_column_name": ""},
        },
        "join": {
            "description": "Inner-join the two forked branches of the same source on matching key fields. The sample has only one source; do not represent a join between distinct tables.",
            "shape": {"type": "join", "join_type": "INNER_JOIN", "left_key": "key", "right_key": "key", "keep_both_keys": False},
        },
        "aggregate_sum": {
            "description": "Sum a numeric field, optionally grouped by one or more fields.",
            "shape": {"type": "aggregate_sum", "group_by": ["group_field"], "column": "numeric_field", "output_name": "total"},
        },
    }
    system = (
        "You translate a user's plain-English request into a Qlik Analytics data-flow plan. "
        "Return only one JSON object with keys summary and steps. Never produce code or SQL. "
        "Allowed step types and exact shapes are: " + json.dumps(allowed, ensure_ascii=False) + ". "
        "Use source field names from the supplied field list. A split_text step creates fields named <column>-split_1 through <column>-split_N; later steps may use those generated names only after that split step. Do not invent any other fields. "
        "Keep steps in execution order. If the request needs an unsupported operation or is ambiguous, "
        "return {\"summary\":\"Clarification required\",\"steps\":[],\"clarification\":\"...\"}. "
        "For split_text use separator SPACE or CUSTOM; set custom_separator when CUSTOM. "
        "For format_dates use the sample-compatible date_pattern unless the user explicitly provides another. "
        "For joins, this template has one source only: create a self-join between forked branches only when the user explicitly requests that; if they mean joining separate tables, ask for a two-source template. "
        "Every step must include its exact type field. Do not include unsupported keys."
    )
    user = json.dumps({"request": prompt, "available_source_fields": fields, "repair_feedback": correction}, ensure_ascii=False)

    if provider == "openai":
        base_url = "https://api.openai.com/v1/responses"
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        body = {
            "model": model,
            "instructions": system,
            "input": user,
            "max_output_tokens": 1200,
            "text": {"format": {
                "type": "json_schema",
                "name": "qlik_flow_plan",
                "strict": True,
                "schema": PLAN_SCHEMA,
            }},
        }
        result = send_json_request(base_url, headers, body)
        content = result.get("output_text")
        if not content:
            text_parts = []
            for item in result.get("output", []):
                for block in item.get("content", []):
                    if block.get("type") == "output_text":
                        text_parts.append(block.get("text", ""))
            content = "\n".join(text_parts)
    elif provider == "anthropic":
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        body = {
            "model": model,
            "max_tokens": 1200,
            "system": system,
            "messages": [{"role": "user", "content": user}],
            "tools": [{"name": "create_flow_plan", "description": "Return a validated Qlik flow plan using the supplied operation schema.", "input_schema": PLAN_SCHEMA}],
            "tool_choice": {"type": "tool", "name": "create_flow_plan"},
        }
        result = send_json_request(url, headers, body)
        blocks = result.get("content", [])
        tool_block = next((block for block in blocks if block.get("type") == "tool_use" and block.get("name") == "create_flow_plan"), None)
        content = tool_block.get("input") if tool_block else "\n".join(block.get("text", "") for block in blocks if block.get("type") == "text")
    else:
        raise FlowError("Choose OpenAI or Claude as the provider.")

    if not content:
        raise FlowError("The LLM response did not include a plan.")
    if isinstance(content, dict):
        return content
    text = str(content).strip()
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE)
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise FlowError("The LLM returned an invalid JSON plan. Try a clearer request.") from exc


def send_json_request(url: str, headers: dict, body: dict, fallback_body: dict | None = None) -> dict:
    def send(payload: dict) -> dict:
        request = Request(url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")
        try:
            with urlopen(request, timeout=90) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            # Provider error bodies may echo a masked fragment of a bad key.
            # Keep credentials and provider diagnostics out of the browser UI.
            if exc.code == 401:
                raise FlowError(
                    "The selected provider rejected the API key (HTTP 401). "
                    "Check the provider selection and re-enter a key created for that provider."
                ) from exc
            raise FlowError(f"LLM API returned HTTP {exc.code}. Check the provider settings and try again.") from exc
        except URLError as exc:
            raise FlowError(f"Could not reach the LLM API: {exc.reason}") from exc
        except TimeoutError as exc:
            raise FlowError("The LLM API request timed out.") from exc

    try:
        return send(body)
    except FlowError as exc:
        # Some compatible endpoints do not implement structured JSON formatting.
        if fallback_body is not None and ("response_format" in body or "text" in body) and "format" in str(exc).lower():
            return send(fallback_body)
        raise


def validate_plan(plan: dict, available_fields: list[str]) -> list[dict]:
    if plan.get("clarification"):
        raise FlowError("The request needs clarification: " + str(plan["clarification"]))
    steps = plan.get("steps")
    if not isinstance(steps, list):
        raise FlowError("The LLM plan must contain a steps array.")
    if len(steps) > 12:
        raise FlowError("This prototype supports at most 12 processor steps per flow.")
    field_set = set(available_fields)
    validated: list[dict] = []
    join_seen = False

    def columns(value, key: str) -> list[str]:
        if not isinstance(value, list) or not value:
            raise FlowError(f"Each {key} value must be a non-empty list of source fields.")
        clean = list(dict.fromkeys(str(x) for x in value))
        missing = [x for x in clean if x not in field_set]
        if missing:
            raise FlowError("Unknown source field(s): " + ", ".join(missing))
        return clean

    for index, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            raise FlowError(f"Step {index} is not a JSON object.")
        kind = step.get("type")
        if not kind:
            raise FlowError(
                f"Step {index} is missing its operation type. Supported operations are replace empty, split text, dates, uppercase, add, inner join, and sum aggregate."
            )
        if kind == "replace_empty":
            validated.append({
                "type": kind,
                "columns": columns(step.get("columns"), "columns"),
                "default_value": str(step.get("default_value", "")),
            })
        elif kind == "split_text":
            column = str(step.get("column", ""))
            if column not in field_set:
                raise FlowError(f"Unknown source field in step {index}: {column or '(empty)'}")
            try:
                parts = int(step.get("parts", 2))
            except (TypeError, ValueError) as exc:
                raise FlowError(f"Step {index}: parts must be an integer.") from exc
            if not 2 <= parts <= 10:
                raise FlowError(f"Step {index}: split parts must be between 2 and 10.")
            separator = str(step.get("separator", "SPACE")).upper()
            if separator not in {"SPACE", "CUSTOM"}:
                raise FlowError(f"Step {index}: separator must be SPACE or CUSTOM.")
            custom = str(step.get("custom_separator", ""))
            if separator == "CUSTOM" and not custom:
                raise FlowError(f"Step {index}: provide a custom separator.")
            validated.append({"type": kind, "column": column, "parts": parts, "separator": separator, "custom_separator": custom})
            field_set.update(f"{column}-split_{part}" for part in range(1, parts + 1))
        elif kind == "format_dates":
            date_pattern = str(step.get("date_pattern", "YYYY-MM-DD hh:mm:ss.fff"))
            validated.append({
                "type": kind,
                "columns": columns(step.get("columns"), "columns"),
                "date_pattern": date_pattern,
                "create_new_column": bool(step.get("create_new_column", True)),
                "new_column_rename": str(step.get("new_column_rename", "")),
            })
        elif kind == "strings_upper":
            selected = columns(step.get("columns"), "columns")
            create_new = bool(step.get("create_new_column", False))
            new_name = str(step.get("new_column_name", ""))
            if create_new and (len(selected) != 1 or not new_name):
                raise FlowError(f"Step {index}: new uppercase output requires one input field and an output name.")
            validated.append({"type": kind, "columns": selected, "create_new_column": create_new, "new_column_name": new_name})
            if create_new:
                field_set.add(new_name)
        elif kind == "math_add":
            column = str(step.get("column", ""))
            if column not in field_set:
                raise FlowError(f"Unknown source field in step {index}: {column or '(empty)'}")
            mode = str(step.get("mode", "WITH_FIELD")).upper()
            if mode not in {"WITH_FIELD", "WITH_VALUE"}:
                raise FlowError(f"Step {index}: math mode must be WITH_FIELD or WITH_VALUE.")
            operand = str(step.get("operand_field", ""))
            if mode == "WITH_FIELD" and operand not in field_set:
                raise FlowError(f"Unknown math operand field in step {index}: {operand or '(empty)'}")
            try:
                constant = float(step.get("constant_value", 0))
            except (TypeError, ValueError) as exc:
                raise FlowError(f"Step {index}: math constant must be numeric.") from exc
            create_new = bool(step.get("create_new_column", False))
            new_name = str(step.get("new_column_name", ""))
            if create_new and not new_name:
                raise FlowError(f"Step {index}: provide an output name for the new math field.")
            validated.append({"type": kind, "column": column, "mode": mode, "operand_field": operand, "constant_value": constant, "create_new_column": create_new, "new_column_name": new_name})
            if create_new:
                field_set.add(new_name)
        elif kind == "join":
            if join_seen:
                raise FlowError("The sample supports one inner Join per flow; it joins forked branches from the same source.")
            left_key = str(step.get("left_key", ""))
            right_key = str(step.get("right_key", ""))
            if left_key not in field_set or right_key not in field_set:
                raise FlowError("Join keys must be selected source fields in the template.")
            join_type = str(step.get("join_type", "INNER_JOIN")).upper()
            if join_type != "INNER_JOIN":
                raise FlowError("The updated sample demonstrates INNER_JOIN only.")
            keep_both = bool(step.get("keep_both_keys", False))
            validated.append({"type": kind, "join_type": join_type, "left_key": left_key, "right_key": right_key, "keep_both_keys": keep_both})
            join_seen = True
            field_set.update(f"RIGHT_{field}" for field in list(field_set) if field != right_key or keep_both)
        elif kind == "aggregate_sum":
            group_by = step.get("group_by", [])
            if not isinstance(group_by, list):
                raise FlowError(f"Step {index}: group_by must be a list of fields.")
            group_by = list(dict.fromkeys(str(value) for value in group_by))
            missing = [field for field in group_by if field not in field_set]
            if missing:
                raise FlowError("Unknown aggregate group field(s): " + ", ".join(missing))
            column = str(step.get("column", ""))
            if column not in field_set:
                raise FlowError(f"Unknown aggregate field in step {index}: {column or '(empty)'}")
            output_name = str(step.get("output_name", "")).strip()
            if not output_name:
                raise FlowError(f"Step {index}: provide an output name for the sum field.")
            validated.append({"type": kind, "group_by": group_by, "column": column, "output_name": output_name})
            field_set.add(output_name)
        else:
            raise FlowError(f"Step {index} uses unsupported operation {kind!r}.")
    return validated


def make_processor(step: dict, index: int, *, joined: bool = False, join_keys: set[str] | None = None) -> dict:
    node_id = str(uuid.uuid4())
    input_types = ["DEFAULT", "LOOKUP"] if step["type"] == "join" else ["DEFAULT"]
    common = {
        "type": "processor",
        "id": node_id,
        "label": {
            "replace_empty": "Cleanse", "split_text": "Split fields", "format_dates": "Dates",
            "strings_upper": "Strings uppercase", "math_add": "Math add", "join": "Join",
            "aggregate_sum": "Aggregate sum",
        }[step["type"]] + f" {index}",
        "description": "",
        "graphical": {"position": {"x": 220 * index, "y": 0}},
        "handles": {
            "incoming": [{"id": str(uuid.uuid4()), "type": kind} for kind in input_types],
            "outgoing": [{"id": str(uuid.uuid4()), "type": "DEFAULT"}],
        },
    }
    if step["type"] == "replace_empty":
        component = {"type": "data-cleansing", "properties": {"configuration": {
            "type": "REPLACE_WITH_VALUE",
            "replaceWithValueConfiguration": {
                "columnsToProcess": step["columns"], "shouldCreateNewColumn": False,
                "newColumnName": "", "mode": "WITH_VALUE", "defaultValue": step["default_value"], "replace": ["EMPTY"],
            },
            "$state": "CONFIGURED",
        }}}
    elif step["type"] == "split_text":
        component = {"type": "split-field", "properties": {"configuration": {
            "type": "SPLIT_TEXT_IN_PARTS",
            "splitTextInPartsConfiguration": {
                "columnsToProcess": [step["column"]], "parts": step["parts"],
                "separator": step["separator"], "customSeparatorValue": step["custom_separator"],
            },
            "$state": "CONFIGURED",
        }}}
    elif step["type"] == "format_dates":
        component = {"type": "date", "properties": {"configuration": {
            "type": "FORMAT_DATES",
            "formatDatesConfiguration": {
                "columnsToProcess": step["columns"], "format": "YYYYMMDD_DASH",
                "datePattern": step["date_pattern"], "shouldCreateNewColumn": step["create_new_column"],
                "newColumnRename": step["new_column_rename"],
            },
            "$state": "CONFIGURED",
        }}}
    elif step["type"] == "strings_upper":
        component = {"type": "strings", "properties": {"configuration": {
            "type": "UPPERCASE",
            "uppercaseConfiguration": {
                "columnsToProcess": step["columns"], "shouldCreateNewColumn": step["create_new_column"],
                "newColumnName": step["new_column_name"],
            },
            "$state": "CONFIGURED",
        }}}
    elif step["type"] == "math_add":
        component = {"type": "math", "properties": {"configuration": {
            "type": "ADD",
            "addConfiguration": {
                "columnsToProcess": [step["column"]], "shouldCreateNewColumn": step["create_new_column"],
                "newColumnName": step["new_column_name"], "mode": step["mode"],
                "constantValue": step["constant_value"], "selectedColumn": step["operand_field"],
            },
            "$state": "CONFIGURED",
        }}}
    elif step["type"] == "join":
        component = {"type": "join", "properties": {"configuration": {
            "joinFunction": step["join_type"],
            "conditions": [{"leftKey": step["left_key"], "rightKey": step["right_key"], "keepBothKeysInOutput": step["keep_both_keys"]}],
            "$state": "CONFIGURED",
        }}}
    else:
        column = step["column"]
        if joined and column not in (join_keys or set()) and not column.startswith("RIGHT_"):
            column = "RIGHT_" + column
        group_by = step["group_by"]
        component = {"type": "aggregate", "properties": {"configuration": {
            "groupBy": [{"fieldPath": field} for field in group_by],
            "operations": [{"operation": "SUM", "fieldPaths": [column], "delimiter": ",", "outputFieldPath": step["output_name"], "percentile": 10}],
            "$state": "CONFIGURED",
        }}}
    common["component"] = component
    return common


def make_fork(index: int) -> dict:
    return {
        "type": "processor",
        "id": str(uuid.uuid4()),
        "label": f"Fork {index}",
        "graphical": {"position": {"x": 220 * index, "y": 0}},
        "handles": {
            "incoming": [{"id": str(uuid.uuid4()), "type": "DEFAULT"}],
            "outgoing": [{"id": str(uuid.uuid4()), "type": "DEFAULT"}] + [
                {"id": str(uuid.uuid4()), "type": f"OPTIONAL_{slot}"} for slot in range(1, 10)
            ],
        },
        "component": {"type": "fork", "properties": {"configuration": {}}},
    }


def edge_between(left: dict, left_handle: dict, right: dict, right_handle: dict) -> dict:
    return {
        "id": f"reactflow__edge-{left['id']}{left_handle['id']}-{right['id']}{right_handle['id']}",
        "sourceHandle": left_handle["id"],
        "targetHandle": right_handle["id"],
    }


def build_dfw(template: dict, name: str, description: str, steps: list[dict]) -> dict:
    source, target, _ = check_template(template)
    result = copy.deepcopy(template)
    dataflow = result["dataflow"]
    dataflow["id"] = str(uuid.uuid4())
    dataflow["name"] = name.strip() or "Generated data flow"
    dataflow["description"] = description.strip()
    dataflow["createdAt"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    dataflow["updatedAt"] = dataflow["createdAt"]
    dataflow["checksum"] = str(uuid.uuid4())
    if isinstance(dataflow.get("context"), dict):
        dataflow["context"]["version"] = 1

    graph_source = copy.deepcopy(source)
    graph_target = copy.deepcopy(target)
    graph_source["id"] = str(uuid.uuid4())
    graph_target["id"] = str(uuid.uuid4())
    graph_source["graphical"] = {"position": {"x": 0, "y": 0}}
    graph_source["handles"] = {"outgoing": [{"id": str(uuid.uuid4()), "type": "DEFAULT"}]}

    # Keep source metadata references coherent while making this a distinct asset.
    old_source_entity = graph_source.get("component", {}).get("properties", {}).get("dataEntity", {}).get("entityId")
    new_source_entity = str(uuid.uuid4())
    if isinstance(graph_source.get("component", {}).get("properties", {}).get("dataEntity"), dict):
        graph_source["component"]["properties"]["dataEntity"]["entityId"] = new_source_entity
    for source_meta in result.get("sources", []):
        if old_source_entity and source_meta.get("sourceId") == old_source_entity:
            source_meta["sourceId"] = new_source_entity

    join_index = next((i for i, step in enumerate(steps) if step["type"] == "join"), None)
    if join_index is None:
        processors = [make_processor(step, index) for index, step in enumerate(steps, start=1)]
        graph_target["graphical"] = {"position": {"x": 220 * (len(processors) + 1), "y": 0}}
        graph_target["handles"] = {"incoming": [{"id": str(uuid.uuid4()), "type": "DEFAULT"}]}
        nodes = [graph_source, *processors, graph_target]
        edges = [edge_between(left, left["handles"]["outgoing"][0], right, right["handles"]["incoming"][0]) for left, right in zip(nodes, nodes[1:])]
    else:
        before = steps[:join_index]
        join_step = steps[join_index]
        after = steps[join_index + 1:]
        before_nodes = [make_processor(step, index) for index, step in enumerate(before, start=1)]
        fork_node = make_fork(len(before_nodes) + 1)
        join_node = make_processor(join_step, len(before_nodes) + 2)
        keys = {join_step["left_key"], join_step["right_key"]}
        after_nodes = [make_processor(step, len(before_nodes) + 3 + offset, joined=True, join_keys=keys) for offset, step in enumerate(after)]
        nodes = [graph_source, *before_nodes, fork_node, join_node, *after_nodes, graph_target]
        graph_target["graphical"] = {"position": {"x": 220 * (len(nodes) - 1), "y": 0}}
        graph_target["handles"] = {"incoming": [{"id": str(uuid.uuid4()), "type": "DEFAULT"}]}
        edges = []
        upstream = [graph_source, *before_nodes, fork_node]
        for left, right in zip(upstream, upstream[1:]):
            edges.append(edge_between(left, left["handles"]["outgoing"][0], right, right["handles"]["incoming"][0]))
        edges.append(edge_between(fork_node, fork_node["handles"]["outgoing"][0], join_node, join_node["handles"]["incoming"][0]))
        edges.append(edge_between(fork_node, fork_node["handles"]["outgoing"][1], join_node, join_node["handles"]["incoming"][1]))
        downstream = [join_node, *after_nodes, graph_target]
        for left, right in zip(downstream, downstream[1:]):
            edges.append(edge_between(left, left["handles"]["outgoing"][0], right, right["handles"]["incoming"][0]))

    dataflow["graph"] = {"nodes": nodes, "edges": edges}
    return result


class Handler(BaseHTTPRequestHandler):
    server_version = "QlikFlowCopilot/0.1"

    def log_message(self, fmt, *args):
        print("[%s] %s" % (self.log_date_time_string(), fmt % args))

    def send_bytes(self, status: int, body: bytes, content_type: str):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status: int, payload: dict):
        self.send_bytes(status, json.dumps(payload, ensure_ascii=False).encode("utf-8"), "application/json; charset=utf-8")

    def do_GET(self):
        if self.path == "/api/template":
            try:
                self.send_json(200, {"template": read_json_template()})
            except FlowError as exc:
                self.send_json(500, {"error": str(exc)})
            return
        if self.path not in {"/", "/index.html"}:
            self.send_json(404, {"error": "Not found"})
            return
        try:
            self.send_bytes(200, INDEX_PATH.read_bytes(), "text/html; charset=utf-8")
        except OSError as exc:
            self.send_json(500, {"error": f"Could not read the app page: {exc}"})

    def do_POST(self):
        if self.path != "/api/generate":
            self.send_json(404, {"error": "Not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_BODY:
                raise FlowError("Request is empty or too large.")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            provider = str(payload.get("provider", "openai"))
            api_key = str(payload.get("api_key", "")).strip()
            model = DEFAULT_MODELS.get(provider, "")
            prompt = str(payload.get("prompt", "")).strip()
            name = str(payload.get("name", "Generated data flow")).strip()
            description = str(payload.get("description", "")).strip()
            if not model:
                raise FlowError("Choose OpenAI or Claude as the provider.")
            if not api_key or not prompt:
                raise FlowError("Enter an API key and describe the flow you want.")
            key_prefix = "sk-" if provider == "openai" else "sk-ant-"
            if not api_key.lower().startswith(key_prefix):
                provider_name = "OpenAI" if provider == "openai" else "Anthropic"
                raise FlowError(
                    f"That does not look like an {provider_name} API key. "
                    "Check the provider selection and paste the key itself, not a file path."
                )
            template = payload.get("template")
            if not isinstance(template, dict):
                template = read_json_template()
            _, _, fields = check_template(template)
            plan = request_plan(provider, api_key, model, prompt, fields)
            steps = validate_plan(plan, fields)
            dfw = build_dfw(template, name, description or str(plan.get("summary", "")), steps)
            self.send_json(200, {"plan": plan, "dfw": dfw, "fields": fields})
        except FlowError as exc:
            self.send_json(400, {"error": str(exc)})
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            self.send_json(400, {"error": f"Invalid request: {exc}"})
        except Exception as exc:  # Return a helpful local error without leaking a traceback to the browser.
            self.send_json(500, {"error": f"Unexpected generation error: {exc}"})


if __name__ == "__main__":
    print(f"Qlik Flow Copilot is running at http://{HOST}:{PORT}")
    print("Press Ctrl+C to stop. The app binds to this computer only.")
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
