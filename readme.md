# FantasticSql

作者: 躺躺不想动了

---

~~因为官方baseline做的像打工，很不舒服，规则老变，越改越烦，越改越不稳定，工作量太大了，而且本人不太喜欢这个方案， 所以不走官方开源路线了，痛失2000块钱，准备戒股一天。~~

---

12.19更新 加了三个示例，感觉稳定了不少。glm-plus 43分。。800万token，节衣缩食的小伙伴建议做以下修改，比较简单：
1. 每一问做出来之后就把之前尝试的对话记录删掉
2. 表格说明按照问题丢进去，不要一下全部丢进去，中文说明去掉。
3. 之前的优化方法

---

运行前，下载question和数据字典到 data 目录下，修改config中的team key



初版比较糙，耗费token会比较多，昨天使用GPT4o是47分，今天改了一点还没测，
建议做以下优化，可以提升准确率还有节省token。
以下是优化方案，感兴趣的可以自己优化一下

优化方案：

针对token：
1. 将长表尤其是50个字段以上的大表拆分成 n个小表并配上描述
2. 使用embedding召回，召回top-k 即可，可以节省召回表格所需token，如果召回不准，建议在描述后面加上一些题目，使用题目embedding做召回
3. column召回可以使用 word2vec 我记得哈工大有一个金融的，可以试试，配合bm25可以达到不错的效果，列名和描述都可以试试

针对SQL
1. 在书写SQL阶段，对于每个表格，可以书写部分案例，可以增加书写准确率,跟着表格一起召回，
2. 尝试增加更多提示词，例如书写SQL的一般思路，或者自己做几个题目，试验一下
3. 尝试CoT,在书写SQL之前，对用户需求进行分析等操作。

当前基本运行逻辑

1. 预处理

   题目->实体->InnerCode-> 等初步信息

2. 召回表格

   输入所有表格说明->输出召回的表格

3. 获取表格详细说明

   表格名称->表格详细说明
4. 拼接提示词

```
system_prompt:
[任务介绍]
[数据库介绍]
输入问题并回答
for question in question_list:
[编写sql]
```

5. 处理答案
   替换 InnerCode 等

   
上述方法都实现后的运行逻辑

1. 准备阶段：向量化

对表格进行向量化，可以是表格的说明，也可以是表格对应的题目。

2. 预处理 

题目->实体->InnerCode-> 等初步信息
题目进行embedding

3. 召回

```
根据题目向量召回 k 个表格描述
使用LLM进行精召回
假设一个表格需要 100 字描述 召回 10 个表格 需要 1000 token
基于表格list，召回列名，也可以召回top-k
```


4. 拼接 SQL Agent 
```
system prompt
【任务介绍】
【召回的表格说明】
【召回的表格示例】
```

5. 输入问题并生成SQL运行,替换等
   。。。
 
各位小伙伴看完记得点个star，不高兴了，这个方案不管了，star超过100可以考虑做视频讲一下，
优化后的小伙伴可以继续开源，
只需说明基于本项目即可，本人想到了一个更好的方法，准备去做新的尝试了。

祝各位小伙伴都能拿第一 。