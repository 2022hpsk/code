from pathlib import Path
import json
from utils import call_sse


prompt = f"""


# Use the intermediate representation and the schema links to generate the SQL queries for each of the questions.
# all_fields

{{scheme}}


# plan

{{knowledge}}

# note
Many tables such as `dws_jordass_playermatchrecord_stat_df` are not detail records but rather ‘daily snapshot/daily summary tables’.
They record a cumulative player snapshot daily.
For `df` snapshot tables, `old` players can only be specified using `=` to denote the ‘snapshot boundary date


# common_knowledge
## 游戏常识说明
- 砺刃使者、勇者盟约、峡谷行动 为游戏名，均为FPS游戏。乐园是砺刃使者下的UGC玩法模式，包含很多子玩法。
- 手游大盘、平台大盘代表所有游戏集合。
- 如果提到某游戏大盘，是指该游戏本身所有活跃用户，而不是全游戏。比如"砺刃大盘活跃"是指砺刃使者游戏的活跃。
- 一个玩家id可能对应多个角色id。
- 游戏玩家行为明细日志，通常会被叫成“流水”，通常表名的格式为：`dwd_gamecode_行为标识别_hi`。游戏玩家付费充值金额也会被叫成“流水”
- 如果"用户问题"只提到'活跃'，没有明确说具体玩法模式的活跃，默认为游戏活跃，而不是游戏内具体某个玩法模式的活跃。

## 常用指标说明
- DAU：日活跃用户数
- 留存：以次留为例，表示当天活跃第二天依然活跃的用户定义为次留，其他留存以此类推
- 新进：注册

## 数仓设计规范
### 分层规范
- DWD层用于存储玩家行为明细数据，每一条行为事件包含一条记录
- DWS层用于存储玩家粒度或者进一步聚合粒度的数据
- DIM代表维度配表

### 命名规范
以`dws_jordass_mode_roundrecord_di` 为例：
  - `dws` 前缀代表分层
  - `jordass` 代表gamecode
  - `mode_roundrecord` 代表表业务含义
  - `di` 后缀中，`d` 代表按天分区，`i` 代表每天存储增量数据。如果后缀是`df`，代表每天存储游戏开服至今的全量数据，使用时取时间周期最后一天即可

### 字段规范
- cbitmap：100位0和1组成的字符串，左侧第一位代表当天。1表示有对应行为，比如活跃或付费，0 表示未发生对应行为，比如未活跃或未付费。常常使用该字段统计流失、回流、留存等指标




# examples 
(Please study the examples carefully, extract the essence of their usage, and pay close attention to the details)

{{examples}}


Q 是问题，A是回答，按照examples 、plan、special_funcitons生成A

Q: {{query}}

schema_links: {{scheme_links}}

A: Let's think step by step. "{{query}}
" can be solved by knowing the answer to the following sub-questions "{{sub_questions}}
".
...
SQL: ```sql
...
```
The SQL query for the sub-question1
(
...
SQL: ```sql
...
```
The SQL query for the sub-question2
)


# 输出
思考之后只输出最终SQL，不要输出其他任何内容：
"""


def _parse_llm_hard(response: dict):
    content = response['content']
    sql = content.split('```sql')[1].split('```')[0]
    return sql

def get_llm_hard_sql(query: str, sub_questions: str, scheme: str, scheme_links: str, knowledge: str, config: dict):
    templates_path = Path(__file__).parent.parent / 'goldensql_template_sub2sql.json'
    examples_text = ""
    try:
        templates = json.loads(templates_path.read_text(encoding='utf-8'))
        # 保证顺序 
        parts = []
        for k in ('sql28', 'sql30', 'sql33'):
            if k in templates:
                parts.append(templates[k])
        examples_text = "\n\n".join(parts)
    except Exception:
        examples_text = ""

    formatted_prompt = prompt.format(
        query=query,
        sub_questions=sub_questions,
        scheme=scheme,
        scheme_links=scheme_links,
        knowledge=knowledge,
        examples=examples_text
    )
    response = call_sse(config['app_key'], '', formatted_prompt)
    sql = _parse_llm_hard(response)
    return sql