# BlossomData

[中文](README.md) | English

![](https://img.shields.io/badge/language-Python-214870)
![GitHub License](https://img.shields.io/github/license/Azure99/BlossomData)
[![PyPI - Version](https://img.shields.io/pypi/v/blossom-data)](https://pypi.org/project/blossom-data/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Azure99/BlossomData)

BlossomData is a data processing framework specifically designed for large model training, aiming to provide flexible and efficient data processing solutions. It comes with a rich library of built-in operators, supporting data processing and synthesis for different modalities and training stages. The framework can seamlessly migrate from single-machine to distributed environments, allowing users to flexibly extend custom operators, thereby greatly reducing the development and computational costs of data processing workflows.

# Usage Examples

```bash
pip install blossom-data
# Install from source
# pip install git+https://github.com/Azure99/BlossomData.git
```

Before using, please configure the API keys and related parameters of your model service provider in the `config.yaml` file. (You can refer to `config.yaml.example`)

Config discovery order (first found wins):
- Path specified by env `BLOSSOM_CONFIG`
- `config.yaml` in the current working directory
- User config `~/.blossom.yaml`

## Your First Data Synthesis Task

Below is a practical example that synthesizes verified long-reasoning Chinese training data using only math problems and reference answers.

The framework provides numerous built-in operators, which can be found in [blossom.op](src/blossom/op/__init__.py). For example, when the original data lacks answers, you can use [ChatDistiller](src/blossom/op/chat/chat_distiller.py) to generate responses, and then filter out potentially incorrect samples based on a voting mechanism using [ChatMultiReasoningFilter](src/blossom/op/chat/chat_multi_reasoning_filter.py).

```python
from blossom import *
from blossom.dataframe import *
from blossom.op import *
from blossom.schema import *

# Example math instruction data
data = [
    ChatSchema(
        messages=[
            user("Suppose that $wz = 12-8i$, and $|w| = \\sqrt{13}$. What is $|z|$?"),
            assistant("4"),
        ]
    ),
]

# Define operators to use
ops = [
    # Use a non-reasoning model to translate instruction data to Chinese
    ChatTranslator(model="deepseek-chat", target_language="Chinese"),
    # Use a reasoning model to generate answers to questions, and a non-reasoning model to verify correctness
    ChatVerifyDistiller(
        model="deepseek-reasoner",
        mode=ChatVerifyDistiller.Mode.LLM,
        validation_model="deepseek-chat",
    ),
    # Merge reasoning_content into content for training
    ChatReasoningContentMerger(),
]

dataset = create_dataset(data)
result = dataset.execute(ops).collect()
print(result)
```

# Core Framework Concepts

## Schema

Schema is the basic data structure in the framework, used to represent and process different types of data. All Schemas inherit from the base Schema class, providing a unified interface and features.

```python
# Text data
text_data = TextSchema(content="This is a text content")

# Conversation data
chat_data = ChatSchema(
    messages=[
        user("Hello"),
        assistant("Hello, how can I help you?")
    ]
)

# Structured data
row_data = RowSchema(data={"name": "John", "age": 30, "score": 95})

# Custom data
custom_data = CustomSchema(data=1)
```

Each Schema instance contains the following common fields:

- `id`: Unique identifier
- `type`: Schema type identifier
- `failed`: Processing failure flag
- `failure_reason`: Failure reason, records specific failure information when failed=True
- `metadata`: Additional metadata of type dict

## DataFrame and Dataset

DataFrame is an abstract representation of data, providing interfaces for transformation, filtering, and aggregation. The framework supports multiple DataFrame implementations, including Local, Spark, and Ray, allowing the same set of code to run on different execution engines.

Dataset is a high-level wrapper for DataFrame, providing a more convenient interface and additional functionality, especially operator support. Dataset is the main interface for user interaction, hiding the complexity of the underlying execution engines.

To ensure consistency in data types and structures, the framework recommends using strongly-typed Schemas. For existing datasets, custom [DataHandler](src/blossom/dataframe/data_handler.py) implementations can be created to customize the loading and saving logic for different data formats.

```python
# Create a dataset
dataset = create_dataset(data, engine=DatasetEngine.LOCAL)
# Or load from filesystem, specifying Ray as the execution engine, using the default data loader
dataset = load_dataset(
    "example/data/chat.jsonl",
    engine=DatasetEngine.RAY,
    data_handler=DefaultDataHandler(),
)
dataset = (
    # Repartition the data
    dataset.repartition(2)
    # One-to-one mapping, take the user prompt from the first round of dialogue and convert to text data
    # .map(lambda x: TextSchema(content=x.messages[0].content))
    # Filter dialogs in English
    .filter(lambda x: x.metadata["language"] == "en")
    # Sort, by feedback score in descending order
    .sort(lambda x: x.metadata["feedback"], ascending=False)
    # Add new metadata, calculate total context length based on dialogue content
    .add_metadata(
        lambda x: {
            "context_length": sum(len(message.content) for message in x.messages)
        }
    )
    # Keep at most 4 data points
    .limit(4)
    # Execute a series of operators
    .execute(
        [
            # Translation operator, translate data to Chinese
            ChatTranslator(model="gpt-4o-mini", target_language="Chinese"),
            # Distillation operator, regenerate responses using a model after translation, with parallelism of 4
            ChatDistiller(model="gpt-4o-mini", parallel=4),
        ]
    )
    # Cache the dataset to avoid repeated computation for subsequent branch operations
    .cache()
)

# Collect results (for large data volumes, please use write_json)
results = dataset.collect()
# Write results to file
dataset.write_json("output.jsonl")

# Aggregation operations
agg_result = dataset.aggregate(
    Sum(lambda x: x.metadata["context_length"]),
    Mean(lambda x: x.metadata["context_length"]),
    Count(),
)

# Group and aggregate
grouped_count = dataset.group_by(lambda x: x.metadata["country"]).count().collect()

print(results)
print(agg_result)
print(grouped_count)
```

## Operator

Operators are the core units of data processing, encapsulating specific data processing logic. The framework provides three basic types of operators:

1. **MapOperator**: One-to-one mapping, processing each element individually
2. **FilterOperator**: Filtering operation, deciding whether to keep or remove elements
3. **TransformOperator**: Many-to-many mapping, can operate on multiple elements simultaneously

```python
from blossom.op import *
from blossom.util import loads_markdown_first_json

# Define custom operators using decorators, first parameter is the element
@map_operator()
def uppercase_text(item):
    item.content = item.content.upper()
    return item

@filter_operator()
def filter_short_text(item):
    return len(item.content) > 10

@transform_operator()
def batch_process(items):
    return [item for item in items if "keyword" in item.content]

# Operator using context to call models, name starts with context, first parameter is context, second is element
# Pass parallel to specify concurrency
@context_map_operator(parallel=4)
def translate_with_model(context, item):
    result = context.chat_completion(
        "gpt-4o-mini", 
        [user(f"Translate to Chinese: {item.content}")]
    )
    return TextSchema(content=result)

# Implement custom operators by inheriting from Operator
# For map operators, implement the process_item method, access context via self.context
class SelfQA(MapOperator):
    def process_item(self, item):
        self_qa_prompt = (
            "Based on the given text, freely generate a question and a corresponding long answer.\n"
            "Your output should be a JSON with two string fields: question and answer, without any other irrelevant explanations.\n"
            f"Given text: {item.content}"
        )
        raw_result = self.context.chat_completion("gpt-4o-mini", [user(self_qa_prompt)])
        result = loads_markdown_first_json(raw_result)
        return ChatSchema(
            messages=[
                user(result["question"]),
                assistant(result["answer"]),
            ]
        )
```

## Context

Context provides the environment and resources needed for operator execution, including configuration information and access to model providers. It serves as a bridge for operators to interact with external resources, enabling operators to access model services, configuration parameters, and more.

```python
# Create a context
context = Context()

# Access configuration
config = context.get_config()

# Get model provider
provider = context.get_model("gpt-4o-mini")

# Generate content using a model
response = context.chat_completion(
    "gpt-4o-mini",
    [user("Hello, how's the weather today?")]
)

# Generate embedding vectors
embedding = context.embedding("text-embedding-3-small", "This is a text")
```
