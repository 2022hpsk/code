import time
from utils.util import call_sse

prompt = f"""

# Task
You are a SQL expert.
For the given question, you need to analyze the question and the schema_links, and then split the question into sub_questions.
The question usually needs nested SQL queries to solve.

# Examples
Question: "How many courses that do not have prerequisite?"
Schema_links: [course.*,course.course_id = prereq.course_id]
Answer: Let’s think step by step. The SQL query for the question "How many courses that do not have prerequisite?" needs these tables = [course,prereq], so we need JOIN.
It requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the sub_questions = ["Which courses have prerequisite?"].

Question: "Find the title of course that is provided by both Statistics and Psychology departments."
Schema_links: [course.title,course.dept_name,Statistics,Psychology]
Answer: Let’s think step by step. The SQL query for the question "Find the title of course that is provided by both Statistics and Psychology departments." needs these tables = [course], so we don't need JOIN.
Plus, it requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the sub_questions = ["Find the titles of courses that is provided by Psychology departments", "Find the titles of courses that is provided by Statistics departments"].
So, we don't need JOIN and need nested queries, then the the SQL query can be classified as "NESTED".

Question: "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010."
Schema_links: [teaches.id,teaches.semester,teaches.year,Fall,2009,Spring,2010]
Answer: Let’s think step by step. The SQL query for the question "Find the id of instructors who taught a class in Fall 2009 but not in Spring 2010." needs these tables = [teaches], so we don't need JOIN.
It requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the sub_questions = ["Find the id of instructors who taught a class in Spring 2009", "Find the id of instructors who did not taught a class in Spring 2010"].

Question: "Give the name and building of the departments with greater than average budget."
Schema_links: [department.budget,department.dept_name,department.building]
Answer: Let’s think step by step. The SQL query for the question "Give the name and building of the departments with greater than average budget." needs these tables = [department], so we don't need JOIN.
It requires nested queries with (INTERSECT, UNION, EXCEPT, IN, NOT IN), and we need the answer to the sub_questions = ["What is the average budget of the departments"].

# Output

Question： 

{{query}}

Schema_links：

{{scheme_links}}

Answer: Let’s think step by step ...

Please follow the above `Examples` and output the complete Answer. It must contains `sub_questions = ["sub-question1", "sub-question2", ...]`.
"""

import re

def _parse_classification(response: dict):
    content = response['content']
    try:
        # 用正则表达式匹配 sub_questions = ["..."]
        m = re.search(r'sub_questions\s*=\s*(\[[^\]]*\])', content)
        sub_questions = m.group(1) if m else ''
        return {
            "sub_questions": sub_questions,
        }

    except Exception as e:
        raise Exception(f"Error parsing classification: {e}")


def get_classification(query: str, scheme: str, scheme_links: str, config: dict, max_retry_times: int):
    for i in range(max_retry_times):
        try:
            formatted_prompt = prompt.format(
                query=query,
                scheme=scheme,
                scheme_links=scheme_links,
            )
            response = call_sse(config['app_key'], '', formatted_prompt)
            classification_result = _parse_classification(response)
            return classification_result

        except Exception as e:
            print(f"Error getting classification: {e}")
            print(f"Retrying {i+1} of {max_retry_times}")
            time.sleep(1)