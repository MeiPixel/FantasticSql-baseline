from llm import llm, try_n_times
import json
import traceback
from utils import get_table_desc, exec_sql, recall_table, process_answer
from pre_process_question import process_question

with open('data/question.json', encoding='utf8') as f:
    all_question = json.load(f)


@try_n_times(3)
def FantasticSql(question_dict, recall_column=50,time_back=True):
    '''
    主函数
    :param question_dict:题目
    :param recall_column:每个表格召回多少列,越大越耗费token
    :param time_back 是否将三个问题一起输入
    :return:
    '''
    question_list = question_dict['team']
    question_content = '\n'.join([i['question'] for i in question_list])
    info, tables = process_question(question_content)
    if info:
        process_info = f"实体匹配数据表信息为：{info},查询尽量使用InnerCode。"
    else:
        process_info = ''

    recall_tables = recall_table(question_content)

    db_recall = list(set([i['table_name'] for i in recall_tables]))
    for i in tables:
        if i not in db_recall:
            db_recall.append(i)

    table_desc = '\n'.join([get_table_desc(i, recall_by=question_content, recall_num=recall_column) for i in db_recall])

    if time_back:
        question_info = f'''
你本次需要回答的问题串为：
{question_content}
请在用户的引导下，一个一个完成问题，不要抢答'''
    else:
        question_info = ''



    messages = [{'role': 'system', 'content': f'''
    任务：股票金融场景的sql编写问答，你将书写专业的金融行业SQL，确保理解用户的需求，纠正用户的输入错误，并确保SQL的正确性。
    请仔细分析表结构后输出SQL.

    用户会给你表格信息和问题，请你编写sql回答问题，
    表格使用 DB.TABLE的形式，即 ```sql SELECT xxx from DB.TABLE```
    数据库使用的是MySQL，
    日期时间的查询方法为：
    ```sql
    DATE(STR_TO_DATE(TradingDay, '%Y-%m-%d %H:%i:%s.%f')) = '2021-01-01'
    DATE(STR_TO_DATE(EndDate , '%Y-%m-%d %H:%i:%s.%f')) = '2021-12-31'
    ```
    所有查询请使用日，不要有时分秒。

    你书写的sql在 ```sql ```内。

    对于一些名称不确定的信息，如板块等，可以使用模糊查询，并且基于常识修正用户的输入。

    用户的数据库描述为:
    {table_desc}
    {question_info}
    {process_info}
    
    用户提问中的公司名称，简称等问题，你可以直接回答 `InnerCode` 字段使用格式`InnerCode:xxx` (例如：用户问题A股最好的公司是？ 回答：`InnerCode:123`)
    你书写的sql必须是一次可以执行完毕的，不要有注释，SQL一般不难，请不要过分复杂化，不要添加多余的过滤条件，
    注意回答日期格式xxxx年xx月xx日，例如2020年01月01日 xxxx-xx-xx -> 2020-01-01 
    '''}]

    question_list = question_dict['team']
    for q in question_list:
        messages.append({'role': 'user', 'content': f"""
    请编写sql解决问题：{q['question']}
    """})

        for try_num in range(3):
            answer = llm(messages)
            if '```sql' in answer:
                sql_result, sql = exec_sql(answer)
                messages.append({'role': 'user', 'content': f"""sql查询结果为：{sql_result}，请忽略结果的时分秒，
    如果可以回答问题：{q['question']}，则输出问题答案，
    如果报错则重写sql，
    如果不能回答问题或者查询为空，count为0请先分析sql是否正确，如果正确则回答问题，
    如果错误则分析错因以及表结构，或者放宽条件，使用简称，然后重写sql
    """})
            else:
                q['answer'] = process_answer(answer)
                # 每次回答都保存
                with open('submit.json', 'w', encoding='utf8') as f:
                    json.dump(all_question, f)
                break


if __name__ == '__main__':

    for i in all_question:
        try:
            FantasticSql(i)
        except:
            traceback.print_exc()
