# Operator Usage Examples

- example/custom_operator.py
  - Custom MapOperator using Context.chat_completion, plus TextTranslator.

- example/decorator_operator.py
  - map_operator, filter_operator, transform_operator, context_map_operator decorators.

- example/failed_item_filter.py
  - mark_failed behavior and FailedItemFilter usage.

- example/distributed_dataset.py
  - Dataset.execute with operators, parallel usage, and distributed DataFrame engines.

- example/chat_math_distill.py
  - ChatVerifyDistiller with LLM validation and ChatReasoningContentMerger.

- example/chat_content_filter.py
  - ChatContentFilter substring filtering by role.

- example/chat_length_filter.py
  - Custom len_func usage and role-specific limits.

- example/chat_embedding.py
  - ChatEmbedder embeddings in metadata.

- example/multi_turn_chat_synthesis.py
  - ChatMultiTurnSynthesizer for multi-turn generation.
