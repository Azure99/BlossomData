# BlossomData

![](https://img.shields.io/badge/language-Python-214870)
![GitHub License](https://img.shields.io/github/license/Azure99/BlossomData)
[![PyPI - Version](https://img.shields.io/pypi/v/blossom-data)](https://pypi.org/project/blossom-data/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/Azure99/BlossomData)

BlossomData是一个专为大模型训练打造的数据处理框架，旨在提供灵活、高效的数据处理解决方案。它内置丰富的算子库，支持不同模态、不同训练阶段的数据处理与合成。能够从单机环境无缝迁移到分布式环境，允许用户灵活扩展自定义算子，从而大幅降低数据处理流程的开发和计算成本。

⚠注意：该项目仍处于原型阶段，API正在快速迭代，建议在实验环境中测试使用。

# 使用示例

```bash
pip install blossom-data
# 从源码安装
# pip install git+https://github.com/Azure99/BlossomData.git
```

在使用之前，请在`config.yaml`文件中配置模型服务提供商的API密钥和相关参数。（可参考`config.yaml.example`）

## 灵活处理数据

下面是一个非常实用的示例，仅依赖数学题目和参考答案，即可合成经过验证的长推理中文训练数据。

框架提供了大量内置算子，可以在[blossom.op](https://github.com/Azure99/BlossomData/blob/main/src/blossom/op/__init__.py)中查看。例如，当原始数据缺失答案时，可以使用[ChatDistiller](https://github.com/Azure99/BlossomData/blob/main/src/blossom/op/chat/chat_distiller.py)生成回答，然后通过[ChatMultiReasoningFilter](https://github.com/Azure99/BlossomData/blob/main/src/blossom/op/chat/chat_multi_reasoning_filter.py)基于投票方式过滤掉潜在错误样本。

```python
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
        validation_model="deepseek-chat"
    ),
    # 将reasoning_content合并到content中，以便用于训练
    ChatReasoningContentMerger()
]

dataset = create_dataset(data)
result = dataset.execute(ops).collect()
print(result)
```

## 分布式数据处理

框架支持多种执行引擎(Local/Spark/Ray)，以便适应各种规模的数据和任务负载。算子仅需关注最核心的处理逻辑，无需感知实际的执行引擎。

```python
# 基于已有数据创建Dataset，仅适合少量数据
# dataset = create_dataset(data, type=DatasetEngine.SPARK)
# 从本地文件创建Dataset
dataset = load_dataset("/path/to/data.json", engine=DatasetEngine.SPARK)
(
    # 可以使用map、filter、transform等底层操作
    dataset.filter(lambda x: x.metadata["language"] == "en")
    # 随机打乱数据并取前10条
    .shuffle()
    .limit(10)
    .execute([ChatTranslator(model="gpt-4o-mini", target_language="Chinese")])
    # 将数据写入本地文件
    .write_json("/path/to/output")
)
```

## 自定义数据加载

对于已经存在的数据，我们可能有特定的格式要求，因此可以通过`DataHandler`来实现自定义的数据加载/保存逻辑。

`from_dict`将自定义数据字典转换为框架内部的Schema，而`to_dict`反向将框架内部的Schema转换为自定义数据字典。

```python
# 自定义加载/保存逻辑
class CustomDataHandler(DataHandler):
    def from_dict(self, data):
        return TextSchema(content=data["your_text_field"])

    def to_dict(self, schema):
        return {"your_text_field": schema.content}


ops = [TextContentFilter(contents="bad_word")]

dataset = load_dataset(path="your_data.json", data_handler=CustomDataHandler())
dataset.execute(ops).write_json(
    "filtered_data.json",
    data_handler=CustomDataHandler(),
)
```

## 自定义算子

定义自己的算子，更灵活地处理和生成训练数据。下面的示例中，首先翻译英文文档为中文，然后从中抽取问答对作为训练数据。

```python
# 自定义Map算子，进行一对一映射
@context_map_operator(parallel=4)
def self_qa_op(context, item):
    self_qa_prompt = (
        "基于给定的文本，随意生成一个问题以及对应的长答案。\n"
        "你的输出应该是一个json，包含question、answer两个字符串字段，不需要输出任何其他的无关解释。\n"
        f"给定的文本：{item.content}"
    )
    raw_result = context.chat_completion("gpt-4o-mini", [user(self_qa_prompt)])
    result = loads_markdown_first_json(raw_result)
    return ChatSchema(
        messages=[
            user(result["question"]),
            assistant(result["answer"]),
        ]
    )


# 纯文本英文数据
data = [
    TextSchema(
        content="""Tomato scrambled eggs is a common dish in Eastern cuisine. 
        Because its ingredients are easy to obtain and the cooking steps are relatively simple, 
        it is also loved by beginners in the kitchen."""
    ),
]

dataset = create_dataset(data)

result = dataset.execute([
    # 翻译英文文本
    TextTranslator(
        model="gpt-4o-mini",
        target_language="Chinese",
    ),
    # 基于翻译后的文本，生成问题和答案
    self_qa_op,
]).collect()
print(result)
```

你可能会得到这样的输出：

```
# User: 为什么厨房新手喜欢做番茄炒蛋？
# Assistant: 厨房新手喜欢做番茄炒蛋有几个主要原因。
# 首先，番茄和鸡蛋这两种食材非常容易获得，几乎在所有的超市和市场都可以买到。
# 其次，番茄炒蛋的烹饪步骤也比较简单，没有复杂的技巧要求，非常适合新手尝试。
# 步骤通常包括切番茄、打鸡蛋、热锅上油、炒熟等，整个过程比较直观。
# 再者，番茄炒蛋作为一道家常菜，口味鲜美，营养丰富，成品容易让人满意，能给新手带来成就感。
# 此外，这道菜还可以根据个人口味进行简易的调味调整，无需严格遵循复杂的配方。
# 这些因素使得番茄炒蛋成为新手下厨时的首选之一。
```

## 聚合与分组分析

框架提供数据聚合、分组、分析能力，可以按照实际需求对数据进行分析。

```python
example_data = [
    RowSchema(
        data={
            "country": random.choice(["US", "CN", "JP", "KR", "TW", "HK"]),
            "score": random.randint(1, 100),
        }
    )
    for _ in range(1024)
]

dataset = create_dataset(example_data)
statistics = {
    # 直接在dataset上进行聚合统计，返回单一结果
    "count": dataset.count(),
    "max": dataset.max(lambda x: x["score"]),

    # 在dataset上进行多重聚合，返回结果字典
    "agg": dataset.aggregate(
        Sum(lambda x: x["score"]),
        # 可以通过name定义结果key
        Sum(lambda x: x["score"] * 2, name="score_x2"),
    ),

    # 对country列进行分组，然后进行聚合分析
    "group_by_country_agg": [
        agg.data
        for agg in dataset.group_by(lambda x: x["country"])
        .aggregate(
            Count(),
            Mean(lambda x: x["score"]),
        )
        .collect()
    ],

    # 自定义聚合函数
    "custom_aggregate_func": dataset.aggregate(
        RowAggregateFunc(
            # 初始值
            initial_value={"cn_count": 0},
            # 分区内元素聚合
            accumulate=lambda x, y: {
                "cn_count": (
                    x["cn_count"] + 1 if y["country"] == "CN" else x["cn_count"]
                ),
            },
            # 分区聚合
            merge=lambda x, y: {
                "cn_count": x["cn_count"] + y["cn_count"],
            },
            # 结果转换
            finalize=lambda x: x["cn_count"],
        ),
    ),
}
print(statistics)
```