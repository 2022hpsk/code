import time
from utils.util import call_sse

prompt = f"""
# Task
你是一个SQL专家，现在需要分析数据库表。通常给定的问题是nested question，因此你要将给定的问题拆解前置需要查询的子问题。
因为你只只需要拆解关键子问题，因此最多不超过2个。

# Common knowledge
这是一些背景信息
{{common_knowledge}}

# Question knowledge
这是一些与当前问题相关的信息
{{question_knowledge}}

# Examples
这是一些示例，供你参考

## Example 1
Question: "How many courses that do not have prerequisite?"
Schema_links: [course.*,course.course_id = prereq.course_id]
Answer: Let’s think step by step. The SQL query for the question "How many courses that do not have prerequisite?" needs these tables = [course,prereq], so we need JOIN.
It requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the sub_questions = ["Which courses have prerequisite?"].

## Example 2
Question: "Find the title of course that is provided by both Statistics and Psychology departments."
Schema_links: [course.title,course.dept_name,Statistics,Psychology]
Answer: Let’s think step by step. The SQL query for the question "Find the title of course that is provided by both Statistics and Psychology departments." needs these tables = [course], so we don't need JOIN.
Plus, it requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the sub_questions = ["Find the titles of courses that is provided by Psychology departments", "Find the titles of courses that is provided by Statistics departments"].
So, we don't need JOIN and need nested queries, then the the SQL query can be classified as "NESTED".

## Example 3
Question: "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010."
Schema_links: [teaches.id,teaches.semester,teaches.year,Fall,2009,Spring,2010]
Answer: Let’s think step by step. The SQL query for the question "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010." needs these tables = [teaches], so we don't need JOIN.
It requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the sub_questions = ["Find the id of instructors who taught a class in Spring 2009", "Find the id of instructors who did not taught a class in Spring 2010"].

## Example 4
Question: "Give the name and building of the departments with greater than average budget."
Schema_links: [department.budget,department.dept_name,department.building]
Answer: Let’s think step by step. The SQL query for the question "Give the name and building of the departments with greater than average budget." needs these tables = [department], so we don't need JOIN.
It requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the sub_questions = ["What is the average budget of the departments"].

# Input
这是你当前需要处理的数据，请输出完整的Answer，最终必须输出 sub_questions = ["sub-question1", ...]。

Question： {{question}}

Schema_links：{{scheme_links}}

Answer: Let’s think step by step ...
"""

import re

def _parse_classification(response: dict):
    import ast

    content = response['content']
    try:
        # 用正则表达式匹配 sub_questions = ["..."]
        m = re.search(r'sub_questions\s*=\s*(\[[^\]]*\])', content)
        sub_questions_raw = m.group(1) if m else ''

        if sub_questions_raw == '':
            raise Exception("sub_questions not found in classification result")

        # 去除\n和多余空格
        # 先把内容转换成合法的Python列表
        try:
            sub_questions_list = ast.literal_eval(sub_questions_raw)
            # 如果是字符串列表，strip每项
            sub_questions_list = [q.strip() for q in sub_questions_list]
        except Exception:
            # fallback: 简单清理字符串（不解析为list）
            sub_questions_list = [q.strip() for q in sub_questions_raw.replace('\n', '').split(',') if q.strip().strip('"').strip("'")]

        return {
            "sub_questions": f"{sub_questions_list}",
        }

    except Exception as e:
        raise Exception(f"Error parsing classification: {e}")


def get_classification(scheme: str, scheme_links: str, data: dict, config: dict):
    for i in range(config["max_retry_times"]):
        try:
            formatted_prompt = prompt.format(
                common_knowledge=config["common_knowledge"],
                question_knowledge=data["knowledge"],
                question=data["question"],
                scheme=scheme,
                scheme_links=scheme_links,
            )

            # print(formatted_prompt)

            response = call_sse(config["sse"]['app_key'], '', formatted_prompt)
            
            # print(response)
            
            classification_result = _parse_classification(response)
            
            return classification_result

        except Exception as e:
            print(f"Error getting classification: {e}")
            print(f"Retrying {i+1} of {config['max_retry_times']}")
            time.sleep(1)

    print(f"Get Classification failed for current sql")
    raise Exception(f"Get Classification failed for current sql")