from blossom.op.map_operator import MapOperator
from blossom.op.text.text_translate import TextTranslate
from blossom.pipeline.simple_pipeline import SimplePipeline
from blossom.schema.chat_schema import ChatMessage, ChatRole, ChatSchema
from blossom.schema.text_schema import TextSchema
from blossom.util.json import loads_markdown_first_json

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
        translate_model="gpt-4o-mini",
        target_language="Chinese",
    ),
    # 基于翻译后的文本，生成问题和答案
    SelfQA(),
)
print(pipeline.execute(data))

# 示例输出
# User: 为什么厨房新手喜欢做番茄炒蛋？
# Assistant: 厨房新手喜欢做番茄炒蛋有几个主要原因。
# 首先，番茄和鸡蛋这两种食材非常容易获得，几乎在所有的超市和市场都可以买到。
# 其次，番茄炒蛋的烹饪步骤也比较简单，没有复杂的技巧要求，非常适合新手尝试。
# 步骤通常包括切番茄、打鸡蛋、热锅上油、炒熟等，整个过程比较直观。
# 再者，番茄炒蛋作为一道家常菜，口味鲜美，营养丰富，成品容易让人满意，能给新手带来成就感。
# 此外，这道菜还可以根据个人口味进行简易的调味调整，无需严格遵循复杂的配方。
# 这些因素使得番茄炒蛋成为新手下厨时的首选之一。
