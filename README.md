# BlossomData

BlossomData是一个用于处理大型语言模型（LLM）训练数据的框架。它提供了一系列工具，用于合成训练数据，包括但不限于生成、翻译、蒸馏、校验等，帮助用户快速构建高质量的训练数据集。

⚠注意：该项目仍处于原型阶段，算子正在快速更新，并且可能存在大量未知问题。建议在实验环境中进行测试。

# 使用示例

使用pip安装。

```
pip3 install git+https://github.com/Azure99/BlossomData.git
```

在使用之前，请在`config.yaml`文件中配置模型服务提供商的API密钥和相关参数。（可参考`config.yaml.example`）

## 翻译数据

下面是一个最简单的示例，用于将对话数据从英文翻译为中文。

```python
# 示例对话数据
data = [
    ChatSchema(
        messages=[
            user("hello"),
            assistant("Hello."),
        ]
    ),
]

# 创建数据集
dataset = create_dataset(data)

# 执行算子并收集结果
result = dataset.execute([
    # 对话翻译，使用gpt-4o将对话数据翻译为中文
    ChatTranslate(model="gpt-4o-mini", target_language="Chinese"),
]).collect()

print(result)
```

配置参数即可实现仅翻译数据中的指令部分，不翻译代码或其他内容，并开启并行处理。

```python
dataset.execute([
    ChatTranslate(
        model="gpt-4o-mini",
        target_language="Chinese",
        instruction_only=True,
        parallel=4,
    ),
]).collect()
```

## 翻译并重新蒸馏数据

直接翻译的模型回复质量可能不佳，因此可以先翻译用户指令，再使用ChatDistill重新生成Assistant回复以提高质量。

```python
dataset.execute([
    ChatTranslate(
        model="gpt-4o-mini",
        target_language="Chinese",
        # 由于Assistant的回复会被蒸馏覆盖，此处可以仅翻译USER的消息
        roles=[ChatRole.USER],
    ),
    # 提供多种蒸馏模式，第一轮、最后一轮、所有轮次
    ChatDistill(model="gpt-4o-mini", strategy=ChatDistill.Strategy.MULTI_TURN),
]).collect()
```

## 根据答案校验

对于有确切答案的问题（GSM8K等数学数据集），我们可以蒸馏回答，并基于参考答案检查是否正确。

```python
data = [
    ChatSchema(
        messages=[
            user("Find all roots of the polynomial $x^3+x^2-4x-4$. Enter your answer as a list of numbers separated by commas."),
            assistant("−2,−1,2"),
        ]
    )
]
dataset = create_dataset(data)
dataset.execute([
    ChatMathDistill(
        model="gpt-4o-mini",
        validate_mode=ChatMathDistill.ValidateMode.LLM,
        max_retry=3,
    ),
]).collect()
```

## 多模型推理校验

对于没有确切答案的问题，我们可以使用另一个模型进行推理，并由第三个模型对两个回答进行检查，过滤掉不一致的结果。

```python
data = [
    ChatSchema(
        messages=[
            user("Who developed ChatGPT?"),
            assistant("OpenAI"),
        ]
    ),
    ChatSchema(
        messages=[
            user("Who developed ChatGPT?"),
            assistant("Google"),
        ]
    ),
]
dataset = create_dataset(data)
dataset.execute([
    ChatMultiReasoningFilter(review_model="gpt-4o-mini", reasoning_model="gpt-4o-mini"),
]).collect()
```

## 分布式数据处理

框架支持本地执行和分布式执行（Spark）两张模式，算子不需要感知实际的执行环境。

```python
# 基于已有数据创建Dataset，仅适合少量数据
dataset = load_dataset(data, type=DatasetType.SPARK)
# 从本地文件创建Dataset
dataset = create_dataset("/path/to/data.json", type=DatasetType.SPARK)
(
    # 可以使用map、filter、transform等底层操作
    dataset.filter(lambda x: x.metadata["language"] == "en")
    # 随机打乱数据并取前10条
    .shuffle()
    .limit(10)
    .execute([ChatTranslate(model="gpt-4o-mini", target_language="Chinese")])
    # 将数据写入本地文件
    .write_json("/path/to/output")
)
```

## 自定义数据加载

对于已经存在/需要使用的数据，我们可能有特定的格式要求，因此可以通过`DataHandler`来实现自定义的数据加载/保存逻辑。

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

定义自己的算子，以便灵活处理和生成训练数据。下面的示例中，首先翻译英文文档为中文，然后从中抽取问答对作为训练数据。

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
    TextTranslate(
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