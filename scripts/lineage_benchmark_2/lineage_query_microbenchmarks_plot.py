import pandas as pd
from pygg import *

"""
Notes for On Time study while it's downloading:
Columns to query: FlightDate <date>, Origin <lat, long>, DepDelay <departure delay>, Reporting_Airline <carrier>
Load in data - anfy gotchas here?
Confirm that there are exactly 8100 buckets with values in them:
with
cte as (
    select distinct
        FlightDate,
        Origin,
        DepDelay,
        Reporting_Airline
    from ontime
)

select
    count(*)
from cte

Execute study.
1. Execute sql from cte and put resulting quads into a list.
1.5. Sample 10k buckets.
2. Execute the following queries while capturing lineage && capturing time:
select
    FlightDate,
    count(*)
from ontime
Swap in Origin, DepDelay, and Reporting_Airline for FlightDate
3. Execute lineage query for each individual row id && capture time:
lineage_query('select FlightDate, count(*) from ontime', 0...N, 'LIN', 0)
Swap in Origin, DepDelay, and Reporting_Airline for FlightDate
4. Execute 3 aggregations with filters for each of the 8100 results && capture time
select
    Origin,
    count(*)
from data
where rowid in (<result from FlightDate lineage query>)
select
    DepDelay,
    count(*)
from data
where rowid in (<result from FlightDate lineage query>)
select
    Reporting_Airline,
    count(*)
from data
where rowid in (<result from FlightDate lineage query>)
5. Clear lineage
5. Repeat 2 through 5 four more times (5 total)
"""

# for each query,
lq_micro = pd.read_csv('lineage_ops_4_9_2023_with_rand_and_skew.csv')
data = lq_micro

print(data)

# p = ggplot(data, aes(x='op', y='avg_duration', color='op', fill='op', group='op', shape='op'))
# p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
# p += axis_labels('Number of Output IDs', "Runtime (ms)", "discrete")#, "log10")
# #p += ylim(lim=[0,300])
# # p += legend_bottom
# ggsave("lineage_querying_micro.png", p,  width=10, height=10)

# p = ggplot(filter_micro, aes(x='num_records', y='avg_duration'))\
#     + geom_point()\
#     + ylim(0, 0.001)\
#     + ggtitle(esc('Filter, 1000 records queried, vary base query table size'))
# ggsave("incr_num_records.png", p, width=10, height=10)
#
# filter_vary_intermediate = data[data['op'] == 'filter']
# p = ggplot(filter_vary_intermediate, aes(x='oids', y='avg_duration')) \
#     + geom_point() \
#     + ggtitle(esc('Filter, 10000000 base query table size, vary records queried'))
#     # + ylim(0, 0.001) \
# ggsave("incr_records_queried.png", p, width=10, height=10)

# vary_op = data[data['oids'] == 1000]
# data['oids'] = data['oids'].astype('str')
# import duckdb
# con = duckdb.connect(database=':memory:', read_only=False)
# con.execute("CREATE TABLE my_df AS SELECT * FROM data")
# print(con.execute('select * from my_df').df())
#
# new_data = con.execute(
#     '''
#     select
#         df1.op,
#         df1.oids,
#         df1.avg_duration - df2.avg_duration as avg_duration
#     from my_df as df1
#     join my_df as df2 on (df1.oids = df2.oids and df2.op = 'simpleagg')
#     '''
# ).df()
#
# print(new_data)

ops = {
    'groupby',
    'filter',
    'perfgroupby',
    'hashjoin',
    'mergejoin',
    'nljoin',
    'hashagg',
    'simpleagg',
    'orderby',
}

data = data[data['avg_parse_time'] != 0]
data = data[['oids', 'avg_duration', 'op']]
data = data[data['op'].isin(ops)]

data['avg_duration'] = data['avg_duration'].mul(1000)
data = data.rename(columns={'oids': 'Queried_ID_Count', 'avg_duration': 'Runtime'})

legend = theme_bw() + theme(**{
    "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
    # "legend.justification":"c(1,0)",
    # "legend.position":"c(1,0)",
    "legend.key" : element_blank(),
    "legend.title":element_blank(),
    "text": element_text(colour = "'#333333'", size=8, family = "'Arial'"),
    "axis.text": element_text(colour = "'#333333'", size=8),
    # "plot.background": element_blank(),
    # "panel.border": element_rect(color=esc("#e0e0e0")),
    # "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
    "strip.text": element_text(color=esc("#333333")),
    "legend.position":"c(0.33,0.67)",
    "legend.margin": margin(t = 0, r = 0, b = 0, l = 0, unit = esc("pt")),
    "legend.text": element_text(colour = "'#333333'", size=8, family = "'Arial'"),
    "legend.key.size": unit(6, esc('pt')),
})

p = ggplot(data, aes(x='Queried_ID_Count', y='Runtime', condition='op', color='op', fill='op', group='op'))\
    + scale_y_continuous(breaks=[0.5, 1.0, 1.5, 2.0, 2.5], labels=[esc('0.5ms'), esc('1ms'), esc('1.5ms'), esc('2ms'), esc('2.5ms')]) \
    + scale_x_log10(name=esc('Queried ID Count (log)')) \
    + geom_line() \
    + legend
ggsave("lq_microbench.png", p, width=2, height=2)

#stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5) \
# + scale_y_log10() + scale_x_log10() \
