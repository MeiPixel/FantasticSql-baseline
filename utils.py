import traceback

import requests
import re
import json
from llm import super_eval, llm
from fuzzywuzzy import fuzz
import pandas as pd
import numpy as np
from config import Config

def exec_sql(sql_message, is_sql=False):
    if is_sql:
        sql = [sql_message]
    else:
        sql_list = re.findall('```sql(.*?)```', sql_message, re.DOTALL)
        sql_list2 = []
        for sql in sql_list:
            sql = '\n'.join([i for i in sql.split('\n') if not i.strip().startswith('--')])
            sql_list2.append(sql)
        sql = sql_list2
    headers = {
        "Authorization": f"Bearer {Config.team_token}",
        "Accept": "application/json"
    }
    url = "https://comm.chatglm.cn/finglm2/api/query"
    res = []
    for sub_sql in sql:
        response = requests.post(url, headers=headers, json={
            "sql": sub_sql,
            "limit": 100
        })
        json_dt = response.json()
        if 'data' in json_dt:
            for i, dt in enumerate(json_dt['data']):
                if 'InnerCode' in dt and 'process_name' not in dt:
                    dt['process_name'] = innercode2name(dt['InnerCode'])
                if 'CompanyCode' in dt and 'process_name' not in dt:
                    dt['process_name'] = CompanyCode2name(dt['CompanyCode'])
                if 'SecuCode' in dt and 'process_name' not in dt:
                    dt['process_name'] = SecuCode2name(dt['SecuCode'])
                if 'SecurityCode' in dt and 'process_name' not in dt:
                    dt['process_name'] = SecurityCode2name(dt['SecurityCode'])
                json_dt['data'][i] = dt
        res.append(json_dt)
        print(response.text)
    return json.dumps(res, indent=2, ensure_ascii=False), sql


data = pd.read_excel('./data/数据字典.xlsx')
table_name2db_name = dict(zip(data['表英文'], data['库名英文']))
data = data.to_dict(orient='records')
table_name2desc = {}
for i in data:
    s = ''
    for j, k in i.items():
        s += f"{j}:{k}\n"

    table_name2desc[i['表英文']] = s

# Group the dataframe by 'table_name' and then convert each group to a list of dictionaries
table2column = pd.read_excel('./data/数据字典.xlsx', sheet_name='表字段信息')
grouped_dict = {
    table_name: group.drop('table_name', axis=1).to_dict(orient='records')
    for table_name, group in table2column.groupby('table_name')
}
for k, v in grouped_dict.items():
    vv = []
    for i in v:
        i = {p: q for p, q in i.items() if isinstance(q, str) and p != 'Annotation'}

        vv.append(i)
    grouped_dict[k] = vv


def get_table_desc(table_name, columns=[], get_sample=False, get_colunm=True, recall_num=30, recall_by='',
                   auto_recall=True):
    '''
    根据表格名称获取表格内容
    :param table_name:
    :param columns:
    :param get_sample:
    :param get_colunm:
    :param recall_num:
    :param recall_by:
    :return:
    '''
    if '.' in table_name:
        full_name = table_name
        table_name = table_name.split('.')[-1]
    else:
        full_name = table_name2db_name[table_name] + '.' + table_name
    a1 = table_name2desc[table_name]
    if columns:

        pos_columns = [i['column_name'] for i in grouped_dict[table_name] if \
                       i['column_name'] in columns or re.search('id|code|day$', i['column_name'], re.IGNORECASE)]
        a2 = json.dumps(
            [i for i in grouped_dict[table_name] if \
             i['column_name'] in pos_columns],
            ensure_ascii=False, indent=1)
    else:
        if recall_by and len(grouped_dict[table_name]) > recall_num:

            column_names = [i['column_description'] + i.get('注释', '') for i in grouped_dict[table_name]]
            sim = [fuzz.ratio(i, recall_by) for i in column_names]
            if auto_recall:
                sim_index = np.argsort(sim)[::-1]  # [:recall_num]
            else:
                sim_index = np.argsort(sim)[::-1][:recall_num]

            pos_columns = [i['column_name'] for i in grouped_dict[table_name] if
                           re.search('id|code|day$|^if', i['column_name'], re.IGNORECASE)]

            for inx in sim_index:
                if sim[inx] > 0:
                    pos_columns.append(grouped_dict[table_name][inx]['column_name'])

            a2 = json.dumps(
                [i for i in grouped_dict[table_name] if \
                 i['column_name'] in pos_columns],
                ensure_ascii=False, indent=1)

        else:
            a2 = json.dumps(grouped_dict[table_name], ensure_ascii=False, indent=1)

    res = f'表格名称:{full_name}\n描述:{a1}'

    if get_colunm:
        res += f'\n字段描述:{a2}'

    if get_sample:
        if columns:
            a3 = str(pd.read_csv(f'./database/{full_name}.csv', encoding='utf8')[pos_columns].head(5))
        else:
            a3 = str(pd.read_csv(f'./database/{full_name}.csv', encoding='utf8').head(5))
        res += f'\n示例:{a3}'

    return res


data = pd.read_excel('./data/数据字典.xlsx')

data_list = data.to_dict(orient='records')

table_content = ''
for i in data_list:
    table_content += str(i)
    table_content += '\n---\n'


def recall_table(question_content, tables_desc=table_content, sample=''):
    '''
    召回表格
    :param question_content:
    :param tables_desc:
    :return:
    '''

    sample = '''
示例1
user:今天是2020年10月27日，当日收盘价第3高的港股是？(以下都回答简称)
成交量第3高的是？
换手率第3高的是？
assistant:```json
[
    {"question":"收盘价第3高的港股","query_requirements":"查询2020年10月27日港股的收盘价并排序得到第3高的记录","table_name":"CS_HKStockPerformance"},
    {"question":"收盘价第3高的港股","query_requirements":"获取港股的简称","table_name":"HK_SecuMain"},

    {"question":"成交量第3高的港股","query_requirements":"查询2020年10月27日港股的成交量并排序得到第3高的记录","table_name":"CS_HKStockPerformance"},
    {"question":"成交量第3高的港股","query_requirements":"获取港股的简称","table_name":"HK_SecuMain"},

    {"question":"换手率第3高的港股","query_requirements":"查询2020年10月27日港股的换手率并排序得到第3高的记录","table_name":"CS_HKStockPerformance"},
    {"question":"换手率第3高的港股","query_requirements":"获取港股的简称","table_name":"HK_SecuMain"}
]
```
示例2
user:唐山港集团股份有限公司是什么时间上市的（回答XXXX-XX-XX）
当年一共上市了多少家企业？
这些企业有多少是在北京注册的？
哪些是注册和办公都在海淀的？
assistant:
```json
[
  {"question":"唐山港集团股份有限公司是什么时间上市的（回答XXXX-XX-XX）","query_requirements":"查询唐山港集团股份有限公司的上市日期","table_name":"SecuMain"},

  {"question":"当年一共上市了多少家企业？","query_requirements":"查询该年度所有上市企业的数量","table_name":"SecuMain"},

  {"question":"这些企业有多少是在北京注册的？","query_requirements":"查询该年度上市企业中注册地为北京的企业数量","table_name":"LC_StockArchives"},

  {"question":"哪些是注册和办公都在海淀的？","query_requirements":"查询注册地和办公地址都在海淀的企业","table_name":"LC_StockArchives"},
  {"question":"哪些是注册和办公都在海淀的？","query_requirements":"获取A股企业名称","table_name":"SecuMain"}
]
```

示例3
user:天顺风能属于哪个三级行业？
2021年发布的该行业的股票有多少只？
该行业8月公布的最高行业营业收入是多少万元？
assistant:
```json
[
    {"question":"天顺风能属于哪个三级行业？","query_requirements":"查询天顺风能的所属行业","table_name":"LC_ExgIndustry"},

    {"question":"2021年发布的该行业的股票有多少只？","query_requirements":"查询2021年发布的该行业股票数量","table_name":"LC_ExgIndustry"},

    {"question":"该行业8月公布的最高行业营业收入是多少万元？","query_requirements":"查询该行业8月的营业收入","table_name":"LC_IndFinIndicators"},
]
```
示例4:
user:易方达基金管理有限公司在19年成立了多少支基金？
哪支基金的规模最大？
这支基金20年最后一次分红派现比例多少钱？
```json
[
    {"question":"易方达基金管理有限公司在19年成立了多少支基金？","query_requirements":"查询易方达基金管理有限公司成立的信息","table_name":"LC_InstiArchive"},
    {"question":"易方达基金管理有限公司在19年成立了多少支基金？","query_requirements":"查询2019年由易方达基金管理有限公司成立的基金数量","table_name":"MF_FundArchives"},
    {"question":"哪支基金的规模最大？","query_requirements":"查询由易方达基金管理有限公司管理的基金中规模最大的基金","table_name":"MF_FundArchives"},
    {"question":"这支基金20年最后一次分红派现比例多少钱？","query_requirements":"查询该基金2020年的最后一次分红派现比例","table_name":"MF_Dividend"}
]
```

'''

    if sample:
        sample_format = f'以下是一些示例:\n{sample}'
    else:
        sample_format = ''

    messages = [{'role': 'system', 'content': f'''
数据表说明如下：
{tables_desc}
用户会给你数据表描述和问题串，请针对每个问题串仔细分析其中的每个问题需要用哪些表格查询，可以是一个或者多个。'''},
                {'role': 'user', 'content': f"""问题串为:`{question_content}`""" + """
使用以下格式回答问题：
```json
[
{"question":"针对的question1","query_requirements":"针对问题里的哪些查询需求","table_name":"表格名称"},
{"question":"针对的question1","query_requirements":"针对问题里的哪些查询需求","table_name":"表格名称"},
{"question":"针对的question2","query_requirements":"针对问题里的哪些查询需求","table_name":"表格名称"},
...
]
```
""" + sample_format + """
备注，针对同一个问题，可以有多条表数据。
请区分港股美股A股的数据在对应的表格内。
table_name只多不少，尽可能列举全，且为`表英文`字段。
"""}]

    min_table = super_eval(llm(messages))
    return min_table


def SecurityCode2name(SecurityCode):
    try:
        sql = f'select InnerCode from PublicFundDB.MF_FundArchives where SecurityCode={SecurityCode}'
        r1, s1 = exec_sql(sql, True)
        r1 = json.loads(r1)[0]
        if 'data' in r1:
            InnerCode = r1['data'][0]['InnerCode']
            return innercode2name(InnerCode)
    except:
        traceback.print_exc()


def innercode2name(innercode):
    '''
    答案替换 innercode
    :param innercode:
    :return:
    '''
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain']
    for table in tables:
        sql = f'''```sql
select ChiName,ChiNameAbbr from {table} where InnerCode={innercode}
```'''
        resp, sql = exec_sql(sql)
        res = json.loads(resp)[0]
        if res['data']:
            s = ''
            for i in res['data']:
                s += f"{i['ChiName']}({i['ChiNameAbbr']})"
            return s


def CompanyCode2name(CompanyCode):
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain']
    for table in tables:
        sql = f'''```sql
    select ChiName,ChiNameAbbr from {table} where CompanyCode={CompanyCode}
    ```'''
        resp, sql = exec_sql(sql)
        res = json.loads(resp)[0]
        if res['data']:
            s = ''
            for i in res['data']:
                s += f"{i['ChiName']}({i['ChiNameAbbr']})"
            return s


def SecuCode2name(SecuCode):
    tables = ['ConstantDB.SecuMain', 'ConstantDB.HK_SecuMain']
    for table in tables:
        sql = f'''```sql
    select ChiName,ChiNameAbbr from {table} where SecuCode={SecuCode}
    ```'''
        resp, sql = exec_sql(sql)
        res = json.loads(resp)[0]
        if res['data']:
            s = ''
            for i in res['data']:
                s += f"{i['ChiName']}({i['ChiNameAbbr']})"
            return s


def process_answer(answer):
    if 'InnerCode' in answer:
        InnerCode = re.findall('InnerCode[:：]\s?(\d+)', answer)
        for i in InnerCode:
            s = innercode2name(i)
            if s:
                answer = re.sub(r'(?<!\d){}(?!\d)'.format(re.escape(i)), s, answer)
    if 'CompanyCode' in answer:
        CompanyCode = re.findall('CompanyCode[:：]\s?(\d+)', answer)
        for i in CompanyCode:
            s = CompanyCode2name(i)
            if s:
                answer = re.sub(r'(?<!\d){}(?!\d)'.format(re.escape(i)), s, answer)
    answer = re.sub(r'(?<=\d),(?=\d{3}[^0-9])', '', answer)
    return answer


