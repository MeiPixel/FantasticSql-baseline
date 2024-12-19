from llm import llm, try_n_times
import json
import traceback
from utils import get_table_desc, exec_sql, recall_table, process_answer
from pre_process_question import process_question

with open('data/question.json', encoding='utf8') as f:
    all_question = json.load(f)


@try_n_times(3)
def FantasticSql(question_dict):
    question_list = question_dict['team']
    question_content = '\n'.join([i['question'] for i in question_list])

    info, tables = process_question(question_content)
    if info:
        process_info = f"实体匹配数据表信息为：{info}"
    else:
        process_info = ''

    recall_tables = recall_table(question_content)

    db_recall = list(set([i['table_name'] for i in recall_tables]))
    for i in tables:
        if i not in db_recall:
            db_recall.append(i)
    if 'SecuMain' not in db_recall:
        db_recall.append('SecuMain')
    table_desc = '\n'.join([get_table_desc(i,recall_by=question_content,recall_num=20) for i in db_recall])

    messages = [{'role': 'system', 'content': f'''
任务：股票金融场景的sql编写问答，你将书写专业的金融行业SQL，确保理解用户的需求，纠正用户的输入错误，并确保SQL的正确性。
请仔细分析表结构后输出SQL.

用户会给你表格信息和问题，请你编写SQL回答问题，SQL不要有注释，
表格使用 DB.TABLE的形式，即 ```sql SELECT xxx from DB.TABLE```
数据库使用的是MySQL，
日期时间的查询方法为：
```sql
DATE(STR_TO_DATE(TradingDay, '%Y-%m-%d %H:%i:%s.%f')) = '2021-01-01'
DATE(STR_TO_DATE(EndDate , '%Y-%m-%d %H:%i:%s.%f')) = '2021-12-31'
```
所有查询请使用日，不要有时分秒。
对于有 InfoPublDate 的字段，你需要查询此字段，并且在回答问题的时候，使用最新发布的数据。

你书写的sql在 ```sql ```内，如果需要多次查询，则将不同次数的查询放在不同的框内。
如：
问题：首钢股份发布增发和配股次数分别是多少次？
SQL: 首钢股份发布增发次数的SQL为:
```sql
SELECT COUNT(*) AS IncreaseIssueCount
FROM AStockFinanceDB.LC_AShareSeasonedNewIssue
WHERE InnerCode = 579;
```
首钢股份发布配股次数的SQL为:
```sql
SELECT COUNT(*) AS PlacementCount
FROM AStockFinanceDB.LC_ASharePlacement
WHERE InnerCode = 579;
```

以下是一些示例：
---
示例1:
问题:天顺风能属于哪个三级行业？
SQL:
```sql
SELECT ThirdIndustryName
FROM AStockIndustryDB.LC_ExgIndustry
WHERE CompanyCode = 81722
AND IfPerformed = 1
ORDER BY DATE(STR_TO_DATE(InfoPublDate, '%Y-%m-%d %H:%i:%s.%f')) DESC
LIMIT 1;
```
问题:2021年发布的该行业的股票有多少只？
SQL:
```sql
SELECT COUNT(DISTINCT CompanyCode) AS StockCount
FROM AStockIndustryDB.LC_ExgIndustry
WHERE ThirdIndustryName = '风电零部件'
AND IfPerformed = 1
AND YEAR(STR_TO_DATE(InfoPublDate, '%Y-%m-%d %H:%i:%s.%f')) = 2021;
```
问题:该行业8月公布的最高行业营业收入是多少万元？
SQL:
```sql
SELECT MAX(IndOperatingRevenue) AS MaxOperatingRevenue
FROM AStockIndustryDB.LC_IndFinIndicators
WHERE IndustryName = '风电零部件'
AND MONTH(STR_TO_DATE(InfoPublDate, '%Y-%m-%d %H:%i:%s.%f')) = 8
AND YEAR(STR_TO_DATE(InfoPublDate, '%Y-%m-%d %H:%i:%s.%f')) = 2021;
```

示例2:
问题:中南出版传媒集团股份有限公司的证券代码是多少？
SQL:
```sql
SELECT SecuCode 
FROM ConstantDB.SecuMain 
WHERE InnerCode = 11314;
```
问题:该公司2019年母公司一季报中预付款项是多少？
SQL:
```sql
SELECT AdvancePayment 
FROM AStockFinanceDB.LC_BalanceSheetAll 
WHERE CompanyCode = 80194 
AND IfMerged = 2 
AND DATE(STR_TO_DATE(EndDate, '%Y-%m-%d %H:%i:%s.%f')) = '2019-03-31'
AND InfoPublDate = (
  SELECT MAX(InfoPublDate) 
  FROM AStockFinanceDB.LC_BalanceSheetAll 
  WHERE CompanyCode = 80194 
  AND IfMerged = 2 
  AND DATE(STR_TO_DATE(EndDate, '%Y-%m-%d %H:%i:%s.%f')) = '2019-03-31'
);
```
问题:总营收呢？
SQL:```sql
SELECT OperatingRevenue 
FROM AStockFinanceDB.LC_IncomeStatementAll 
WHERE CompanyCode = 80194 
AND IfMerged = 2 
AND DATE(STR_TO_DATE(EndDate, '%Y-%m-%d %H:%i:%s.%f')) = '2019-03-31'
AND InfoPublDate = (
  SELECT MAX(InfoPublDate) 
  FROM AStockFinanceDB.LC_IncomeStatementAll 
  WHERE CompanyCode = 80194 
  AND IfMerged = 2 
  AND DATE(STR_TO_DATE(EndDate, '%Y-%m-%d %H:%i:%s.%f')) = '2019-03-31'
);

```
问题:经营活动现金流入了多少？
SQL:
```sql
SELECT SubtotalOperateCashInflow 
FROM AStockFinanceDB.LC_CashFlowStatementAll 
WHERE CompanyCode = 80194 
AND IfMerged = 2 
AND DATE(STR_TO_DATE(EndDate, '%Y-%m-%d %H:%i:%s.%f')) = '2019-03-31'
AND InfoPublDate = (
  SELECT MAX(InfoPublDate) 
  FROM AStockFinanceDB.LC_CashFlowStatementAll 
  WHERE CompanyCode = 80194 
  AND IfMerged = 2 
  AND DATE(STR_TO_DATE(EndDate, '%Y-%m-%d %H:%i:%s.%f')) = '2019-03-31'
);
```

示例3:
问题:海信视像科技股份有限公司在什么时候成立的，XXXX-XX-XX？
SQL:
```sql
SELECT EstablishmentDate 
FROM AStockBasicInfoDB.LC_StockArchives
WHERE CompanyCode = 1070
LIMIT 1;
```
问题:该公司在2021年的半年度报告中未调整的**合并报表**营业总成本是多少？
SQL:
```sql
SELECT TotalOperatingCost 
FROM AStockFinanceDB.LC_IncomeStatementAll
WHERE CompanyCode = 1070
AND DATE(STR_TO_DATE(EndDate , '%Y-%m-%d %H:%i:%s.%f')) = '2021-06-30'
AND IfAdjusted = 2
AND IfMerged = 1
ORDER BY InfoPublDate DESC
LIMIT 1;
```
问题:该公司在2021年的半年度报告中未调整的**合并报表**净利润是多少？
SQL:
```sql
SELECT NetProfit 
FROM AStockFinanceDB.LC_IncomeStatementAll
WHERE CompanyCode = 1070
AND DATE(STR_TO_DATE(EndDate , '%Y-%m-%d %H:%i:%s.%f')) = '2021-06-30'
AND IfAdjusted = 2
AND IfMerged = 1
ORDER BY InfoPublDate DESC
LIMIT 1;
```
---

对于一些名称不确定的信息，如板块等，可以使用模糊查询，并且基于常识修正用户的输入。

用户的数据库描述为:
{table_desc}
{process_info}
请在用户的引导下，一个一个完成问题，不要抢答,查询尽量使用InnerCode。
用户提问中的公司名称，简称等问题，你可以直接回答 `InnerCode` 字段使用格式`InnerCode:xxx` (例如：用户问题A股最好的公司是？ 回答：`InnerCode:123`)
对于每个问题，你有三次机会回答，如果一次编写SQL比较复杂，你可以分步骤书写，你的每一条SQL我都会给你运行结果。
'''}]

    question_list = question_dict['team']
    question_dict['table'] = recall_tables
    for q in question_list:
        messages.append({'role': 'user', 'content': f"""
请编写sql解决问题：{q['question']}，你有三次机会，你可以一次编写或者分步骤编写，sql在 ```sql ```内。
请仅仅使用我给你的信息，不要自己编造code。
"""})

        for try_num in range(3):
            answer = llm(messages)
            if '```sql' in answer:
                sql_result, sql = exec_sql(answer)
                messages.append({'role': 'user', 'content': f"""sql查询结果为：{sql_result}，请忽略结果的时分秒，
    如果可以回答问题：{q['question']}，则输出问题答案，如果需要输出时间格式，月份和日期前需要带0 如2020年01月01日，
    如果报错则重写sql，其中`查询执行失败: Commands out of sync; you can't run this command now"`的原因是因为有注释，或者你在一个block里写了多个sql，遇到此问题请分开书写去掉注释。
    如果不能回答问题或者查询为空，count为0请先分析sql是否正确，如果正确则回答问题，如果发现答案不唯一，则寻找更多筛选条件，
    如果出现重复答案，也思考为何重复，确保SQL查询到唯一值。
    如果错误则分析错因以及表结构，然后重写sql
    """})
                q['sql'] = sql
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
    for i in all_question:
        if 'table' in i:
            del i['table']
        for j in i['team']:
            if 'answer' in j and 'CompanyCode' in j['answer']:
                j['answer'] = process_answer(j['answer'])

            if 'answer' not in j:
                j['answer'] = ''

            if 'sql' in j:
                del j['sql']

    with open('submit.json', 'w', encoding='utf8') as f:
        json.dump(all_question, f, ensure_ascii=False, indent=4)