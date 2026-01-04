# Operator Development Guidelines

## Scope and Layout
Place new operators under `chat/` for `ChatSchema`, `text/` for `TextSchema`, `common/` for shared operators (like `EqualWidthBinner`), and `util/` for helper classes (like `ContentTranslator`). Export public operators in `src/blossom/op/__init__.py` and add them to `__all__`.

## Choose the Right Base Class
- `MapOperator`: one-to-one transforms via `process_item`, skips items already marked failed.
- `FilterOperator`: returns `bool`, keeps failed items by default; use `reverse` to invert logic.
- `TransformOperator`: batch-level `list[Schema]` transforms via `process_items`.
- `MetricFilterOperator`: compute and cache per-item metrics in metadata, then filter.
- `Operator`: implement `process` directly when you need custom `DataFrame` operations.

## Schema Handling and Message Shapes
Use `_cast_text`, `_cast_chat`, and `_cast_base` for runtime type checks. For chat operators, handle `message.content` as either a `str` or a list of parts; only mutate `ChatMessageContentText` and preserve non-text content. Gate optional behavior with flags like `filter_reasoning` or `translate_reasoning` and treat `reasoning_content` as optional.

## Context, Providers, and Retries
`self.context` is injected by the execution framework, so avoid using it in `__init__`. For LLM calls use `self.context.chat_completion` or `chat_completion_with_details` when you need `finish_reason` or token usage. Forward `extra_params` to providers, and prefer `ContentTranslator` or `ContentEmbedder` for shared retry and parsing logic.

## Error Handling and Metadata
Follow the retry pattern used across chat and text operators: keep `last_exception`, log with `logger.exception`, then call `item.mark_failed(...)` if all attempts fail. Avoid raising from `process_item` unless you want to halt the pipeline. When a response is truncated, store a metadata flag (for example `METADATA_RESPONSE_TRUNCATED`).

Store derived outputs in `item.metadata` with configurable field names and optional overwrite flags (see embedder operators). Validate constructor inputs early and raise `ValueError` for invalid ranges or missing dependencies.

## Concurrency and Side Effects
`parallel` runs a thread pool in map and filter operators. Keep `process_item` deterministic and thread-safe; do not mutate shared state across items.

## Minimal Skeleton
```python
from blossom.op.map_operator import MapOperator
from blossom.schema.schema import Schema

class MyTextOp(MapOperator):
    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_text(item)
        # mutate _item.content or _item.metadata here
        return self._cast_base(_item)
```
