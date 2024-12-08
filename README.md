# BlossomData

BlossomData是一个用于处理大型语言模型（LLM）训练数据的框架。它提供了一系列工具，用于合成训练数据，包括但不限于生成、翻译、蒸馏、校验等，帮助用户快速构建高质量的训练数据集。

⚠注意：该项目仍处于原型阶段，算子正在快速更新，并且可能存在大量未知问题。建议在实验环境中进行测试。

# 使用示例

使用pip安装。

```
pip3 install git+https://github.com/Azure99/BlossomData.git
```

在使用之前，请在`config.yaml`文件中配置模型服务提供商的API密钥和相关参数。

## 翻译数据

下面是一个最简单的示例，用于将对话数据从英文翻译为中文。

```python
# 示例对话数据
data = [
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="hello"),
            ChatMessage(role=ChatRole.ASSISTANT, content="Hello."),
        ]
    ),
]
# 定义Pipeline
pipeline = SimplePipeline().add_operators(
    # 对话翻译，使用gpt-4o将对话数据翻译为中文
    ChatTranslate(model="gpt-4o-mini", target_language="Chinese"),
)
# 执行并打印结果
print(pipeline.execute(data))
```

配置参数即可实现仅翻译数据中的指令部分，不翻译代码或其他内容，并开启并行处理。

```python
pipeline = SimplePipeline().add_operators(
    ChatTranslate(
        model="gpt-4o-mini",
        target_language="Chinese",
        instruction_only=True,
        parallel=4,
    ),
)
```

## 翻译并重新蒸馏数据

直接翻译的模型回复质量可能不佳，因此可以先翻译用户指令，再使用ChatDistill重新生成Assistant回复以提高质量。

```python
pipeline = SimplePipeline().add_operators(
    ChatTranslate(
        model="gpt-4o-mini",
        target_language="Chinese",
        # 由于Assistant的回复会被蒸馏覆盖，此处可以仅翻译USER的消息
        roles=[ChatRole.USER],
    ),
    # 提供多种蒸馏模式，第一轮、最后一轮、所有轮次
    ChatDistill(model="gpt-4o-mini", strategy=ChatDistill.Strategy.MULTI_TURN),
)
```

## 根据答案校验

对于有确切答案的问题（GSM8K等数学数据集），我们可以蒸馏回答，并基于参考答案检查是否正确。

```python
data = [
    ChatSchema(
        messages=[
            ChatMessage(
                role=ChatRole.USER,
                content="Find all roots of the polynomial $x^3+x^2-4x-4$. Enter your answer as a list of numbers separated by commas.",
            ),
            ChatMessage(role=ChatRole.ASSISTANT, content="−2,−1,2"),
        ]
    )
]
pipeline = SimplePipeline().add_operators(
    ChatMathDistill(
        model="gpt-4o-mini",
        validate_mode=ChatMathDistill.ValidateMode.LLM,
        max_retry=3,
    ),
)
```

## 多模型推理校验

对于没有确切答案的问题，我们可以使用另一个模型进行推理，并由第三个模型对两个回答进行检查，过滤掉不一致的结果。

```python
data = [
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="Who developed ChatGPT?"),
            ChatMessage(role=ChatRole.ASSISTANT, content="OpenAI"),
        ]
    ),
    ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="Who developed ChatGPT?"),
            ChatMessage(role=ChatRole.ASSISTANT, content="Google"),
        ]
    ),
]
pipeline = SimplePipeline().add_operators(
    ChatMultiReasoningFilter(review_model="gpt-4o-mini", reasoning_model="gpt-4o-mini"),
)
```

## 自定义算子

定义自己的算子，以便灵活处理和生成训练数据。例如，翻译英文文档，然后从中抽取问答对作为训练数据。

```python
# 自定义Map算子，进行一对一映射
class SelfQA(MapOperator):
    def process_item(self, item):
        self_qa_prompt = (
            "基于给定的文本，随意生成一个问题以及对应的长答案。\n"
            "你的输出应该是一个json，包含question、answer两个字符串字段，不需要输出任何其他的无关解释。\n"
            f"给定的文本：{item.content}"
        )
        raw_result = self.context.single_chat_completion("gpt-4o-mini", self_qa_prompt)
        result = loads_markdown_first_json(raw_result)
        return ChatSchema(
            messages=[
                ChatMessage(role=ChatRole.USER, content=result["question"]),
                ChatMessage(role=ChatRole.ASSISTANT, content=result["answer"]),
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

pipeline = SimplePipeline().add_operators(
    # 翻译英文文本
    TextTranslate(
        model="gpt-4o-mini",
        target_language="Chinese",
    ),
    # 基于翻译后的文本，生成问题和答案
    SelfQA(),
)
print(pipeline.execute(data))
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

