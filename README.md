# BlossomData

![](https://img.shields.io/badge/language-Python-214870)
![GitHub License](https://img.shields.io/github/license/Azure99/BlossomData)
[![PyPI - Version](https://img.shields.io/pypi/v/blossom-data)](https://pypi.org/project/blossom-data/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Azure99/BlossomData)

BlossomData是一个专为大模型训练打造的数据处理框架，旨在提供灵活、高效的数据处理解决方案。它内置丰富的算子库，支持不同模态、不同训练阶段的数据处理与合成。能够从单机环境无缝迁移到分布式环境，允许用户灵活扩展自定义算子，从而大幅降低数据处理流程的开发和计算成本。

# 使用示例

```bash
pip install blossom-data
# 从源码安装
# pip install git+https://github.com/Azure99/BlossomData.git
```

在使用之前，请在`config.yaml`文件中配置模型服务提供商的API密钥和相关参数。（可参考`config.yaml.example`）

## 第一个数据合成任务

下面是一个非常实用的示例，仅依赖数学题目和参考答案，即可合成经过验证的长推理中文训练数据。

框架提供了大量内置算子，可以在[blossom.op](https://github.com/Azure99/BlossomData/blob/main/src/blossom/op/__init__.py)中查看。例如，当原始数据缺失答案时，可以使用[ChatDistiller](https://github.com/Azure99/BlossomData/blob/main/src/blossom/op/chat/chat_distiller.py)生成回答，然后通过[ChatMultiReasoningFilter](https://github.com/Azure99/BlossomData/blob/main/src/blossom/op/chat/chat_multi_reasoning_filter.py)基于投票方式过滤掉潜在错误样本。

```python
from blossom import *
from blossom.dataframe import *
from blossom.op import *
from blossom.schema import *

# 示例数学指令数据
data = [
    ChatSchema(
        messages=[
            user("Suppose that $wz = 12-8i$, and $|w| = \\sqrt{13}$. What is $|z|$?"),
            assistant("4"),
        ]
    ),
]

# 定义要使用的算子
ops = [
    # 利用非推理模型将指令数据翻译为中文
    ChatTranslator(model="deepseek-chat", target_language="Chinese"),
    # 使用推理模型生成问题的回答，并使用非推理模型验证回答正确性
    ChatVerifyDistiller(
        model="deepseek-reasoner",
        mode=ChatVerifyDistiller.Mode.LLM,
        validation_model="deepseek-chat",
    ),
    # 将reasoning_content合并到content中，以便用于训练
    ChatReasoningContentMerger(),
]

dataset = create_dataset(data)
result = dataset.execute(ops).collect()
print(result)
```

# 框架核心概念

## Schema

Schema是框架中的基础数据结构，用于表示和处理不同类型的数据。所有Schema都继承自基础Schema类，提供了统一的接口和功能。

```python
# 文本数据
text_data = TextSchema(content="这是一段文本内容")

# 对话数据
chat_data = ChatSchema(
    messages=[
        user("你好"),
        assistant("你好，请问需要什么帮助？")
    ]
)

# 结构化数据
row_data = RowSchema(data={"name": "张三", "age": 30, "score": 95})

# 自定义数据
custom_data = CustomSchema(data=1)
```

每个 Schema 实例都包含以下通用字段：

- `id`: 唯一标识符
- `type`: Schema类型标识
- `failed`: 处理失败标志
- `metadata`: dict类型的附加元数据

## DataFrame与Dataset

DataFrame是对数据的抽象表示，提供了对数据进行转换、过滤和聚合的接口。框架支持多种DataFrame实现，包括Local、Spark和Ray，使得同一套代码可以在不同的执行引擎中运行。

Dataset是对DataFrame的高级封装，提供了更加便捷的接口和额外的功能，特别是对算子的支持。Dataset是用户交互的主要接口，隐藏了底层执行引擎的复杂性。

```python
# 创建数据集
dataset = create_dataset(data, engine=DatasetEngine.LOCAL)
# 或从文件系统加载，并指定执行引擎为Ray，使用默认的数据加载器
dataset = load_dataset(
    "example/data/chat.jsonl",
    engine=DatasetEngine.RAY,
    data_handler=DefaultDataHandler(),
)
dataset = (
    # 对数据重新分区
    dataset.repartition(2)
    # 一对一映射, 取第一轮对话的用户prompt并转换为文本数据
    # .map(lambda x: TextSchema(content=x.messages[0].content))
    # 过滤出语言为英文的对话
    .filter(lambda x: x.metadata["language"] == "en")
    # 排序，按反馈分数降序
    .sort(lambda x: x.metadata["feedback"], ascending=False)
    # 新增元信息，基于对话内容计算总上下文长度
    .add_metadata(
        lambda x: {
            "context_length": sum(len(message.content) for message in x.messages)
        }
    )
    # 最多保留4条数据
    .limit(4)
    # 执行一系列算子
    .execute(
        [
            # 翻译算子，将数据翻译为中文
            ChatTranslator(model="gpt-4o-mini", target_language="Chinese"),
            # 蒸馏算子，将翻译后的数据使用模型重新生成回复，并发为4
            ChatDistiller(model="gpt-4o-mini", parallel=4),
        ]
    )
    # 缓存数据集，避免后续多个分支操作的重复计算
    .cache()
)

# 收集结果（数据量较大时，请使用write_json）
results = dataset.collect()
# 将结果写入文件
dataset.write_json("output.jsonl")

# 聚合操作
agg_result = dataset.aggregate(
    Sum(lambda x: x.metadata["context_length"]),
    Mean(lambda x: x.metadata["context_length"]),
    Count(),
)

# 分组后聚合
grouped_count = dataset.group_by(lambda x: x.metadata["country"]).count().collect()

print(results)
print(agg_result)
print(grouped_count)
```

## Operator

Operator（算子）是数据处理的核心单元，封装了特定的数据处理逻辑。框架提供了三种基本类型的算子：

1. **MapOperator**: 一对一映射，对每个元素单独处理
2. **FilterOperator**: 过滤操作，决定保留或删除元素
3. **TransformOperator**: 多对多映射，可以对多个元素同时操作

```python
from blossom.op import *
from blossom.util import loads_markdown_first_json

# 使用装饰器定义自定义算子，第一个参数为元素
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

# 使用上下文调用模型的算子，名称以context开头，第一个参数为上下文，第二个参数为元素
# 传入parallel以指定并发数
@context_map_operator(parallel=4)
def translate_with_model(context, item):
    result = context.chat_completion(
        "gpt-4o-mini", 
        [user(f"Translate to Chinese: {item.content}")]
    )
    return TextSchema(content=result)

# 继承Operator实现自定义算子
# 对于map算子，需要实现process_item方法，通过self.context访问上下文
class SelfQA(MapOperator):
    def process_item(self, item):
        self_qa_prompt = (
            "基于给定的文本，随意生成一个问题以及对应的长答案。\n"
            "你的输出应该是一个json，包含question、answer两个字符串字段，不需要输出任何其他的无关解释。\n"
            f"给定的文本：{item.content}"
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

Context（上下文）提供了算子执行所需的环境和资源，包括配置信息和模型提供者的访问。它是算子与外部资源交互的桥梁，使得算子可以访问模型服务、配置参数等。

```python
# 创建上下文
context = Context()

# 访问配置
config = context.get_config()

# 获取模型提供者
provider = context.get_model("gpt-4o-mini")

# 使用模型生成内容
response = context.chat_completion(
    "gpt-4o-mini",
    [user("你好，请问今天天气如何？")]
)

# 生成嵌入向量
embedding = context.embedding("text-embedding-3-small", "这是一段文本")
```