# Operator Catalog

## Base operators
- src/blossom/op/operator.py
  - Operator: abstract base; init_context injects Context; _cast_* helpers.

- src/blossom/op/map_operator.py
  - MapOperator: per-item transform, skip failed items, optional parallel.
  - Decorators: map_operator, context_map_operator.

- src/blossom/op/filter_operator.py
  - FilterOperator: boolean keep/drop, reverse support, optional parallel.
  - Decorators: filter_operator, context_filter_operator.

- src/blossom/op/transform_operator.py
  - TransformOperator: batch transforms on list[Schema].
  - Decorators: transform_operator, context_transform_operator.

- src/blossom/op/metric_filter_operator.py
  - MetricFilterOperator: compute per-item metrics once, cache in metadata, then filter.

- src/blossom/op/failed_item_filter.py
  - FailedItemFilter: drop failed items (or keep only failed with reverse=True).

## Common operators
- src/blossom/op/common/equal_width_binner.py
  - EqualWidthBinner (Operator): compute bin edges from DataFrame.cache().min/max.
  - Writes bin index and label to metadata (bin_field, bin_label_field).
  - Validates labels length vs num_bins.

## Util helpers (used by operators)
- src/blossom/op/util/content_translator.py
  - ContentTranslator: provider-backed translation with JSON prompt and retries.

- src/blossom/op/util/content_embedder.py
  - ContentEmbedder: provider-backed embedding with retries.

- src/blossom/op/util/char_repetition_filter.py
  - CharRepetitionFilter: n-gram repetition ratio for content filtering.

## Chat operators
- src/blossom/op/chat/chat_embedder.py
  - ChatEmbedder (MapOperator): embed message content by role and strategy.
  - Params: roles, strategy (first/last/full), merge_messages, embeddings_field,
    overwrite_field, max_retry, extra_params, parallel.

- src/blossom/op/chat/chat_translator.py
  - ChatTranslator (MapOperator): translate message content for selected roles.
  - Params: target_language, instruction_only, translate_reasoning, max_retry, parallel.

- src/blossom/op/chat/chat_invalid_filter.py
  - ChatInvalidFilter (FilterOperator): validate message list, content, role order.
  - Params: rules (EMPTY_MESSAGES, EMPTY_CONTENT, INVALID_ROLE_ORDER), reverse.

- src/blossom/op/chat/chat_length_filter.py
  - ChatLengthFilter (FilterOperator): length caps by role and total.
  - Params: len_func, system_max_len, user_max_len, assistant_max_len, total_max_len.

- src/blossom/op/chat/chat_content_filter.py
  - ChatContentFilter (FilterOperator): drop items containing substrings.
  - Params: contents, roles, case_sensitive, filter_reasoning, reverse.

- src/blossom/op/chat/chat_content_replacer.py
  - ChatContentReplacer (MapOperator): replace substrings in message content.
  - Params: replacements, roles, case_sensitive, replace_reasoning.

- src/blossom/op/chat/chat_content_trimmer.py
  - ChatContentTrimmer (MapOperator): strip chars from message content.
  - Params: roles, strip_chars, trim_reasoning.

- src/blossom/op/chat/chat_repetition_filter.py
  - ChatRepetitionFilter (FilterOperator): n-gram repetition filter by role.
  - Params: n, min_ratio, max_ratio, filter_reasoning, reverse.

- src/blossom/op/chat/chat_distiller.py
  - ChatDistiller (MapOperator): regenerate assistant replies by strategy.
  - Params: strategy (first_turn/last_turn/multi_turn), max_total_tokens,
    stop_on_truncate, max_retry, extra_params, parallel.
  - Sets metadata METADATA_RESPONSE_TRUNCATED when finish_reason is length.

- src/blossom/op/chat/chat_conversation_synthesizer.py
  - ChatMultiTurnSynthesizer (MapOperator): synthesize multi-turn conversations.
  - Params: model, user_simulator_model, input_truncate_rounds, max_total_tokens,
    max_rounds, user_simulator_prompt, max_retry, extra_params, parallel.
  - Sets metadata METADATA_RESPONSE_TRUNCATED when finish_reason is length.

- src/blossom/op/chat/chat_reasoning_consistency_filter.py
  - ChatReasoningConsistencyFilter (FilterOperator): compare response consistency
    against a reference answer or model-generated reasoning.
  - Params: review_model, reasoning_model or reference_field, validation_prompt,
    max_retry, reverse, parallel.

- src/blossom/op/chat/chat_verify_distiller.py
  - ChatVerifyDistiller (MapOperator): generate answer then validate it.
  - Modes: NONE, REGEX, LLM, FUNCTION.
  - Params: reference_field, validation_model, validation_prompt,
    validation_function, max_retry, extra_params, parallel.
  - Sets metadata METADATA_VALIDATION_ATTEMPTS.

- src/blossom/op/chat/chat_reasoning_content_merger.py
  - ChatReasoningContentMerger (MapOperator): prepend reasoning to content.
  - Params: prefix, suffix, clear_reasoning, force_wrapper, strategy.

## Text operators
- src/blossom/op/text/text_embedder.py
  - TextEmbedder (MapOperator): embed text content.
  - Params: embedding_field, overwrite_field, max_retry, extra_params, parallel.

- src/blossom/op/text/text_translator.py
  - TextTranslator (MapOperator): translate text content.
  - Params: target_language, max_retry, extra_params, parallel.

- src/blossom/op/text/text_length_filter.py
  - TextLengthFilter (FilterOperator): max length filter.
  - Params: len_func, max_len, reverse.

- src/blossom/op/text/text_content_filter.py
  - TextContentFilter (FilterOperator): drop items containing substrings.
  - Params: contents, case_sensitive, reverse.

- src/blossom/op/text/text_content_replacer.py
  - TextContentReplacer (MapOperator): replace substrings in content.
  - Params: replacements, case_sensitive.

- src/blossom/op/text/text_trimmer.py
  - TextTrimmer (MapOperator): strip chars from content.
  - Params: strip_chars.

- src/blossom/op/text/text_repetition_filter.py
  - TextRepetitionFilter (FilterOperator): n-gram repetition filter.
  - Params: n, min_ratio, max_ratio, reverse.
