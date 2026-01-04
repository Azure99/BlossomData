# Context, Provider, DataFrame, Schema Integration

## Execution flow
- Dataset.execute calls operator.init_context(self.context) then operator.process(dataframe).
- Operators should not access self.context in __init__; the context is injected at runtime.

## Context and Provider
- Context owns ProviderManager and resolves models from config.
- Use Context methods in operators:
  - chat_completion(model, messages, extra_params)
  - chat_completion_with_details(model, messages, extra_params)
  - embedding(model, input_text, extra_params)
  - embedding_with_details(model, input_text, extra_params)
- Provider implements chat_completion, chat_completion_with_details, embedding, embedding_with_details.
- Forward extra_params from operator constructors to providers to preserve model settings.
- Use chat_completion_with_details when you need finish_reason or token usage.

## DataFrame
- Base operators delegate to DataFrame.map, filter, or transform.
- MapOperator and FilterOperator use ThreadPoolExecutor when parallel > 1.
- Operator.process can use DataFrame.cache or aggregations for dataset-level logic.
- DataFrame implementations include LocalDataFrame, MultiProcessDataFrame, RayDataFrame, SparkDataFrame.

## Schema
- Schema fields: id, type, failed, failure_reason, metadata.
- mark_failed sets failed and failure_reason; MapOperator skips failed items.
- ChatSchema: messages are ChatMessage with role and content; content can be str or list of parts.
- TextSchema: content is a plain string.
- CustomSchema: data is arbitrary.
- Use Operator._cast_* methods to assert schema type at runtime.

## Message handling for chat operators
- content can be str or list of ChatMessageContent; only mutate ChatMessageContentText.
- Preserve non-text content such as image content parts.
- reasoning_content is optional; gate translation/filter/trim with explicit flags.
