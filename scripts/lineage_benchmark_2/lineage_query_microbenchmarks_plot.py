import pandas as pd
from pygg import *

# for each query,
lq_micro = pd.read_csv('lineage_ops_3_19_2023.csv')
filter_micro = pd.read_csv('filter_explor_3_19_2023.csv')
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

data['avg_duration'] = data['avg_duration'].mul(1000)
data = data.rename(columns={'oids': 'Queried_ID_Count', 'avg_duration': 'Runtime'})

legend = theme_bw() + theme(**{
    "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
    # "legend.justification":"c(1,0)",
    # "legend.position":"c(1,0)",
    "legend.key" : element_blank(),
    "legend.title":element_blank(),
    "text": element_text(colour = "'#333333'", size=9, family = "'Arial'"),
    "axis.text": element_text(colour = "'#333333'", size=9),
    # "plot.background": element_blank(),
    # "panel.border": element_rect(color=esc("#e0e0e0")),
    # "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
    "strip.text": element_text(color=esc("#333333")),
    # "legend.position": esc("bottom"),
    "legend.margin": margin(t = 0, r = 0, b = 0, l = 0, unit = esc("pt")),
    "legend.text": element_text(colour = "'#333333'", size=9, family = "'Arial'"),
    "legend.key.size": unit(8, esc('pt')),
})

p = ggplot(data, aes(x='Queried_ID_Count', y='Runtime', condition='op', color='op', fill='op', group='op'))\
    + scale_y_continuous(breaks=[0.25, 0.5, 0.75, 1, 1.25], labels=[esc('0.25ms'), esc('0.5ms'), esc('0.75ms'), esc('1ms'), esc('1.25ms')]) \
    + scale_x_log10(name=esc('Queried ID Count (log)')) \
    + geom_line() \
    + legend
ggsave("lq_microbench.png", p, width=3, height=2)

#stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5) \
# + scale_y_log10() + scale_x_log10() \