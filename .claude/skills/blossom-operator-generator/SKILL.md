---
name: blossom-operator-generator
description: Create or update Blossom operators under src/blossom/op (chat, text, common, util), including choosing MapOperator/FilterOperator/TransformOperator/MetricFilterOperator/Operator, wiring __init__.py exports, and using Context, Provider, DataFrame, and Schema APIs. Use when asked to add new operators, filters, translators, embedders, distillers, or other pipeline steps.
---

# Blossom Operator Generator

## Overview
Use this skill to create consistent Blossom operators that follow existing patterns and integrate cleanly with Context, Provider, DataFrame, and Schema.

## Workflow
1) Clarify intent and schema
- Identify schema type (ChatSchema, TextSchema, CustomSchema, or generic Schema).
- Decide whether the operator is map, filter, transform, metric filter, or DataFrame-level.
- Capture output location (mutate content, add metadata, or replace messages).

2) Choose the right base class
- Use MapOperator for one-to-one transforms and failed-item skipping.
- Use FilterOperator for boolean keep/drop with reverse support.
- Use TransformOperator for batch-level transforms.
- Use MetricFilterOperator when you must compute per-item metrics once and cache them.
- Use Operator only when you need DataFrame-level behavior.

3) Implement with Blossom patterns
- Use _cast_text/_cast_chat/_cast_custom/_cast_base for runtime type checks.
- Use self.context only inside process/process_item (context is injected by Dataset.execute).
- Follow retry and mark_failed patterns for provider calls.
- Keep process_item deterministic and thread-safe when parallel > 1.

4) Wire and validate
- Place the file under src/blossom/op/<area> and name it snake_case.
- Export the operator in src/blossom/op/__init__.py and add to __all__.

5) Write unit tests
- Add tests under tests/ to mirror the src/blossom/ module path (e.g., src/blossom/op/text/foo.py → tests/op/text/test_foo.py).
- Use pytest with test_<module>.py filenames and cover at least: happy path, schema casting/validation, and filter/transform semantics.
- Stub provider calls or external dependencies; avoid network access in tests.

## References
Read these before implementing or modifying an operator:
- references/operator-workflow.md for base class semantics, error handling, and schema handling.
- references/operator-catalog.md for existing operators and parameter patterns.
- references/integrations.md for Context, Provider, DataFrame, and Schema connections.
- references/examples.md for operator usage examples in example/.
