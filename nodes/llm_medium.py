import time
from utils.util import call_sse

external_knowledge = """
数据表注意事项：
Many tables such as `dws_jordass_playermatchrecord_stat_df` are not detail records but rather ‘daily snapshot/daily summary tables’.
They record a cumulative player snapshot daily.
For `df` snapshot tables, `old` players can only be specified using `=` to denote the ‘snapshot boundary date

游戏常识说明：
- 砺刃使者、勇者盟约、峡谷行动 为游戏名，均为FPS游戏。乐园是砺刃使者下的UGC玩法模式，包含很多子玩法。
- 手游大盘、平台大盘代表所有游戏集合。
- 如果提到某游戏大盘，是指该游戏本身所有活跃用户，而不是全游戏。比如"砺刃大盘活跃"是指砺刃使者游戏的活跃。
- 一个玩家id可能对应多个角色id。
- 游戏玩家行为明细日志，通常会被叫成“流水”，通常表名的格式为：`dwd_gamecode_行为标识别_hi`。游戏玩家付费充值金额也会被叫成“流水”
- 如果"用户问题"只提到'活跃'，没有明确说具体玩法模式的活跃，默认为游戏活跃，而不是游戏内具体某个玩法模式的活跃。

常用指标说明：
- DAU：日活跃用户数
- 留存：以次留为例，表示当天活跃第二天依然活跃的用户定义为次留，其他留存以此类推
- 新进：注册

数仓设计规范：
分层规范：
- DWD层用于存储玩家行为明细数据，每一条行为事件包含一条记录
- DWS层用于存储玩家粒度或者进一步聚合粒度的数据
- DIM代表维度配表

命名规范：
以`dws_jordass_mode_roundrecord_di` 为例：
  - `dws` 前缀代表分层
  - `jordass` 代表gamecode
  - `mode_roundrecord` 代表表业务含义
  - `di` 后缀中，`d` 代表按天分区，`i` 代表每天存储增量数据。如果后缀是`df`，代表每天存储游戏开服至今的全量数据，使用时取时间周期最后一天即可

字段规范：
- cbitmap：100位0和1组成的字符串，左侧第一位代表当天。1表示有对应行为，比如活跃或付费，0 表示未发生对应行为，比如未活跃或未付费。常常使用该字段统计流失、回流、留存等指标
"""

prompt = f"""
# Task
You are a SQL expert.
Use all the following information to generate the SQL query for the question.

# All_fields

{{scheme}}

# External_knowledge
{external_knowledge}

当前问题所需知识：
{{knowledge}}

# Examples

Question: "Find the total budgets of the Marketing or Finance department."
Schema_links: [department.budget,department.dept_name,Marketing,Finance]
Answer: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = []. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: select sum(department.budget) from department  where  department.dept_name = "Marketing"  or  department.dept_name = "Finance"
SQL: SELECT sum(budget) FROM department WHERE dept_name  =  'Marketing' OR dept_name  =  'Finance'

Question: "Find the total number of students and total number of instructors for each department."
Schema_links: [department.dept_name = student.dept_name,student.id,department.dept_name = instructor.dept_name,instructor.id]
Answer: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = [department,student,instructor]. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: "select count( distinct student.ID) , count( distinct instructor.ID) , department.dept_name from department  group by instructor.dept_name
SQL: SELECT count(DISTINCT T2.id) ,  count(DISTINCT T3.id) ,  T3.dept_name FROM department AS T1 JOIN student AS T2 ON T1.dept_name  =  T2.dept_name JOIN instructor AS T3 ON T1.dept_name  =  T3.dept_name GROUP BY T3.dept_name

Question: "Find the title of courses that have two prerequisites?"
Schema_links: [course.title,course.course_id = prereq.course_id]
Answer: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = [course,prereq]. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: select course.title from course  where  count ( prereq.* )  = 2  group by prereq.course_id
SQL: SELECT T1.title FROM course AS T1 JOIN prereq AS T2 ON T1.course_id  =  T2.course_id GROUP BY T2.course_id HAVING count(*)  =  2

Question: "Find the name of students who took any class in the years of 2009 and 2010."
Schema_links: [student.name,student.id = takes.id,takes.YEAR,2009,2010]
Answer: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = [student,takes]. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: select  distinct student.name from student  where  takes.year = 2009  or  takes.year = 2010
SQL: SELECT DISTINCT T1.name FROM student AS T1 JOIN takes AS T2 ON T1.id  =  T2.id WHERE T2.YEAR  =  2009 OR T2.YEAR  =  2010

Question: "list in alphabetic order all course names and their instructors' names in year 2008."
Schema_links: [course.title,course.course_id = teaches.course_id,teaches.id = instructor.id,instructor.name,teaches.year,2008]
Answer: Let’s think step by step. For creating the SQL for the given question, we need to join these tables = [course,teaches,instructor]. First, create an intermediate representation, then use it to construct the SQL query.
Intermediate_representation: select course.title , instructor.name from course  where  teaches.year = 2008  order by course.title asc
SQL: SELECT T1.title ,  T3.name FROM course AS T1 JOIN teaches AS T2 ON T1.course_id  =  T2.course_id JOIN instructor AS T3 ON T2.id  =  T3.id WHERE T2.YEAR  =  2008 ORDER BY T1.title


# Output

Question: {{query}}

Schema_links: {{scheme_links}}

Answer: Let’s think step by step.
...
SQL: ```sql
...
```

请你思考之后输出完整的Answer，其中必须包含最终的SQL，用```sql包裹。
"""

def _parse_llm_medium(response: dict):
    content = response['content']
    sql = content.split('```sql')[1].split('```')[0]
    return sql

def get_llm_medium_sql(query: str, scheme: str, scheme_links: str, knowledge: str, config: dict, max_retry_times: int):
    try:
        for i in range(max_retry_times):
            formatted_prompt = prompt.format(query=query, scheme=scheme, scheme_links=scheme_links, knowledge=knowledge)
            response = call_sse(config['app_key'], '', formatted_prompt)
            sql = _parse_llm_medium(response)
            return sql
    except Exception as e:
        raise Exception(f"Get LLM Medium SQL failed for sql {query}")