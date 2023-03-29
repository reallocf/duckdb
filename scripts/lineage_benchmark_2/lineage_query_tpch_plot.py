import pandas as pd
from pygg import *

smokedduck = pd.read_csv('tpch_sf1_2_28_2023_2.csv')
logical_rid = pd.read_csv('tpch_bw_notes_mar1_sf1_lineage_type_Logical-RID.csv')
print(smokedduck)
print(logical_rid)

smokedduck = smokedduck[['queryId', 'avg_duration']]
smokedduck['system'] = ['SD_Query' for _ in range(len(smokedduck))]
smokedduck = smokedduck.rename(columns={'queryId': 'query', 'avg_duration': 'runtime'})
logical_rid = logical_rid[['query', 'runtime']]
logical_rid['system'] = ['Logical-RID' for _ in range(len(logical_rid))]
data = smokedduck.append(logical_rid)
data['query'] = data['query'].astype('str')
data['runtime'] = data['runtime'].mul(1000)
data = data.rename(columns={'query': 'Query', 'runtime': 'Runtime'})

print(data)

legend = theme_bw() + theme(**{
    "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
    "legend.justification":"c(1,0)",
    # "legend.position":"c(1,0)",
    "legend.key" : element_blank(),
    "legend.title":element_blank(),
    "text": element_text(colour = "'#333333'", size=11, family = "'Arial'"),
    "axis.text": element_text(colour = "'#333333'", size=11),
    # "plot.background": element_blank(),
    # "panel.border": element_rect(color=esc("#e0e0e0")),
    # "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
    "strip.text": element_text(color=esc("#333333")),
    "legend.position": esc("bottom"),
})

p = ggplot(data, aes(x='Query', y='Runtime', condition='system', color='system', fill='system', group='system')) \
    + geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5) \
    + scale_y_log10(name=esc("Runtime (log)"), breaks=[1, 10, 100, 1000], labels=[esc('1ms'), esc('10ms'), esc('100ms'), esc('1000ms')]) \
    + scale_x_continuous(breaks=[i for i in range(1, 23)], minor_breaks='NULL') \
    + legend
ggsave("querying_tpch_sf1.png", p, width=6, height=3)
# + scale_x_discrete(labels=[esc(f'{i}') for i in range(1, 23)]) \
# + ggtitle(esc('TPC-H SF=1 Lineage Querying')) \
# + axis_labels('Query', 'Runtime (log)', 'discrete', 'log10') \

