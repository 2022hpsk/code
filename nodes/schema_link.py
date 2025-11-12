import time
from utils.util import call_sse

prompt = f"""

# Task
你是一个SQL专家，现在需要分析数据库表，得出与目标查询的schema_links。
我会给你提供与这个问题相关的表在创建时的SQL指令，你需要首先分析与问题相关的字段，并且给出schema_links。

# Common knowledge
这是一些背景信息
{{common_knowledge}}

# Question knowledge
这是一些与当前问题相关的信息
{{question_knowledge}}

# Examples
这是一些示例，供你参考
## Example 1

Tables:
Table advisor, columns = [*,s_ID,i_ID]
Table classroom, columns = [*,building,room_number,capacity]
Table course, columns = [*,course_id,title,dept_name,credits]
Table department, columns = [*,dept_name,building,budget]
Table instructor, columns = [*,ID,name,dept_name,salary]
Table prereq, columns = [*,course_id,prereq_id]
Table section, columns = [*,course_id,sec_id,semester,year,building,room_number,time_slot_id]
Table student, columns = [*,ID,name,dept_name,tot_cred]
Table takes, columns = [*,ID,course_id,sec_id,semester,year,grade]
Table teaches, columns = [*,ID,course_id,sec_id,semester,year]
Table time_slot, columns = [*,time_slot_id,day,start_hr,start_min,end_hr,end_min]
Foreign_keys = [course.dept_name = department.dept_name,instructor.dept_name = department.dept_name,section.building = classroom.building,section.room_number = classroom.room_number,section.course_id = course.course_id,teaches.ID = instructor.ID,teaches.course_id = section.course_id,teaches.sec_id = section.sec_id,teaches.semester = section.semester,teaches.year = section.year,student.dept_name = department.dept_name,takes.ID = student.ID,takes.course_id = section.course_id,takes.sec_id = section.sec_id,takes.semester = section.semester,takes.year = section.year,advisor.s_ID = student.ID,advisor.i_ID = instructor.ID,prereq.prereq_id = course.course_id,prereq.course_id = course.course_id]

Question:
"Find the buildings which have rooms with capacity more than 50."

Answer: 
Let’s think step by step. In the question "Find the buildings which have rooms with capacity more than 50.", we are asked:
"the buildings which have rooms" so we need column = [classroom.capacity]
"rooms with capacity" so we need column = [classroom.building]
Based on the columns and tables, we need these Foreign_keys = [].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = [50]. So the Schema_links are:
Schema_links: [classroom.building,classroom.capacity,50]

## Example 2

Tables:
Table city, columns = [*,City_ID,Official_Name,Status,Area_km_2,Population,Census_Ranking]
Table competition_record, columns = [*,Competition_ID,Farm_ID,Rank]
Table farm, columns = [*,Farm_ID,Year,Total_Horses,Working_Horses,Total_Cattle,Oxen,Bulls,Cows,Pigs,Sheep_and_Goats]
Table farm_competition, columns = [*,Competition_ID,Year,Theme,Host_city_ID,Hosts]
Foreign_keys = [farm_competition.Host_city_ID = city.City_ID,competition_record.Farm_ID = farm.Farm_ID,competition_record.Competition_ID = farm_competition.Competition_ID]

Question: 
"Show the status of the city that has hosted the greatest number of competitions."

Answer:
Let’s think step by step. In the question "Show the status of the city that has hosted the greatest number of competitions.", we are asked:
"the status of the city" so we need column = [city.Status]
"greatest number of competitions" so we need column = [farm_competition.*]
Based on the columns and tables, we need these Foreign_keys = [farm_competition.Host_city_ID = city.City_ID].
Based on the tables, columns, and Foreign_keys, The set of possible cell values are = []. So the Schema_links are:
Schema_links: [city.Status,farm_competition.Host_city_ID = city.City_ID,farm_competition.*]

# Input
这是你当前需要处理的数据，请输出完整的Answer

Table name list:
{{table_list}}

Table scheme:
{{scheme}}

Question:
{{question}}

Answer:
Let’s think step by step ...
"""

def _parse_schema_link(response: dict):
    content = response['content']
    Schema_links = content.split('Schema_links:')[-1]
    return Schema_links

def get_schema_link(table_list: str, scheme: str, data: dict, config: dict):
    for i in range(config["max_retry_times"]):
        try:
            formatted_prompt = prompt.format(common_knowledge=config["common_knowledge"], question_knowledge=data["knowledge"], table_list=table_list, scheme=scheme, question=data["question"])

            # print(formatted_prompt)

            response = call_sse(config["sse"]['app_key'], '', formatted_prompt)

            # print(response)

            schema_links = _parse_schema_link(response)

            # print(schema_links)

            return f"{schema_links}"

        except Exception as e:
            print(f"Error getting schema link: {e}")
            print(f"Retrying {i+1} of {config['max_retry_times']}")
            time.sleep(1)

    print(f"Get Schema Links failed for current sql")
    raise Exception("Get Schema Links failed for current sql")
