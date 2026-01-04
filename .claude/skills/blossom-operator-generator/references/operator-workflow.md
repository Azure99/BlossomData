# Operator Workflow and Patterns

## Base classes and behavior
- Operator (src/blossom/op/operator.py)
  - Abstract base with process(DataFrame) and init_context(Context).
  - Use when you need DataFrame-level operations or custom batching beyond map/filter/transform.
  - Provides _cast_* helpers for runtime Schema checks.

- MapOperator (src/blossom/op/map_operator.py)
  - One-to-one transforms via process_item; process_skip_failed skips items with item.failed.
  - process uses DataFrame.map, or DataFrame.transform with ThreadPoolExecutor when parallel > 1.
  - Override process_item or pass map_func; context_map_operator decorator wraps Context + Schema.

- FilterOperator (src/blossom/op/filter_operator.py)
  - Boolean predicate via process_item; process_skip_failed returns True for failed items.
  - reverse flips keep/drop logic; parallel uses ThreadPoolExecutor + DataFrame.transform.
  - context_filter_operator decorator wraps Context + Schema.

- TransformOperator (src/blossom/op/transform_operator.py)
  - Batch-level transforms via process_items on list[Schema].
  - process calls DataFrame.transform; context_transform_operator injects Context.

- MetricFilterOperator (src/blossom/op/metric_filter_operator.py)
  - Compute expensive per-item metrics once and cache in metadata.
  - metrics_metadata_key defaults to <ClassName>_metrics; recompute_metrics forces refresh.
  - metrics_only populates metrics without filtering; reverse flips keep/drop.
  - Uses internal parallel map/filter helpers when parallel > 1.

## Error handling and retries
- Prefer mark_failed in process_item instead of raising to avoid pipeline aborts.
- Follow retry pattern used in chat/text operators:
  - last_exception variable
  - for _ in range(max_retry): try; except; logger.exception or logger.info
  - mark_failed after retries
- When responses are truncated, store a metadata flag such as METADATA_RESPONSE_TRUNCATED.

## Metadata conventions
- Store derived outputs in item.metadata with configurable field names.
- Add overwrite_field flags when repeated runs might overwrite existing metadata.
- MetricFilterOperator caches metrics under metrics_metadata_key.

## Schema handling
- Use _cast_text, _cast_chat, _cast_custom, or _cast_base for runtime validation.
- Failed item behavior:
  - MapOperator skips failed items automatically.
  - FilterOperator keeps failed items by default; use FailedItemFilter to remove them.
- Chat message content can be str or list of parts; only mutate ChatMessageContentText.
- reasoning_content is optional; gate behavior with explicit flags.

## Concurrency and determinism
- parallel uses ThreadPoolExecutor in map and filter operators.
- Keep process_item deterministic and thread-safe; do not mutate shared state.

## Constructor validation
- Validate input ranges and required dependencies early; raise ValueError on invalid input.

## Decorator helpers
- map_operator, filter_operator, transform_operator wrap simple functions into operators.
- context_map_operator, context_filter_operator, context_transform_operator pass Context.
