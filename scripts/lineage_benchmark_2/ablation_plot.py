import pandas as pd
from pygg import *

query_list = [1, 4, 14, 15, 17]
query_list_to_pos_map = {
    q: idx for (idx, q) in enumerate(query_list)
}
no_indexes = pd.read_csv('tpch_sf1_ablation1_2_28_2023.csv')
zone_maps = pd.read_csv('tpch_sf1_ablation3_2_28_2023_2.csv')
smokedduck = pd.read_csv('tpch_sf1_2_28_2023_2.csv')
postprocess = pd.read_csv('postprocess_times3.csv')

no_indexes = no_indexes[no_indexes.queryId.isin(query_list)]
no_indexes = no_indexes[['queryId', 'avg_duration']]
no_indexes['ablation'] = ['aaa No Indexes' for _ in range(len(no_indexes))]
zone_maps = zone_maps[zone_maps.queryId.isin(query_list)]
zone_maps = zone_maps[['queryId', 'avg_duration']]
zone_maps['ablation'] = ['bbb Zone Maps' for _ in range(len(zone_maps))]
smokedduck = smokedduck[smokedduck.queryId.isin(query_list)]
smokedduck = smokedduck[['queryId', 'avg_duration']]
smokedduck['ablation'] = ['ccc Group By Indexes' for _ in range(len(smokedduck))]
postprocess = postprocess[postprocess['ablation'] == -1]
postprocess = postprocess[postprocess['sf'] == 1]
postprocess = postprocess[postprocess.query_id.isin(query_list)]
postprocess = postprocess.reset_index()
postprocess['avg_duration'] = [(
                                   postprocess['time_in_sec1'][i] +
                                   postprocess['time_in_sec2'][i] +
                                   postprocess['time_in_sec3'][i] +
                                   postprocess['time_in_sec4'][i] +
                                   postprocess['time_in_sec5'][i]
                               ) / 5 for i in range(len(postprocess))]
postprocess = postprocess[['query_id', 'avg_duration']]
postprocess['ablation'] = ['ddd Group By Postprocess' for _ in range(len(postprocess))]
postprocess = postprocess.rename(columns={'query_id': 'queryId'})
print(no_indexes)
print(zone_maps)
print(smokedduck)
print(postprocess)

data = no_indexes.append(zone_maps).append(smokedduck).append(postprocess)
data['queryId'] = [query_list_to_pos_map[q] for q in data['queryId']]
data['queryId'] = data['queryId'].astype('str')
data['avg_duration'] = data['avg_duration'].mul(1000)
data = data.rename(columns={'queryId': 'Query', 'avg_duration': 'Runtime'})

print(data)

legend = theme_bw() + theme(**{
    "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
    "legend.justification":"c(1,0)",
    # "legend.position":"c(1,0)",
    "legend.key" : element_blank(),
    "legend.title":element_blank(),
    "text": element_text(colour = "'#333333'", size=8, family = "'Arial'"),
    "axis.text": element_text(colour = "'#333333'", size=8),
    # "plot.background": element_blank(),
    # "panel.border": element_rect(color=esc("#e0e0e0")),
    # "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
    "strip.text": element_text(color=esc("#333333")),
    "legend.position":"c(0.3,-0.7)",
    "legend.margin": margin(t = 0, r = 0, b = 0, l = 0, unit = esc("pt")),
    "legend.text": element_text(colour = "'#333333'", size=6, family = "'Arial'"),
    "legend.key.size": unit(6, esc('pt')),
})

condition_labels = [esc('No Indexes'), esc('Zone Maps'), esc('Group By Indexes'), esc('Group By Postprocess')]

p = ggplot(data, aes(x='Query', y='Runtime', condition='ablation', color='ablation', fill='ablation', group='ablation', factor='Runtime')) \
    + geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5) \
    + scale_y_log10(name=esc("Runtime (log)"), breaks=[1, 10, 100, 1000, 10000], labels=[esc('1ms'), esc('10ms'), esc('100ms'), esc('1000ms'), esc('10000ms')]) \
    + scale_x_continuous(breaks=[i for i in range(len(query_list))], labels=[esc(f'{q}') for q in query_list], minor_breaks='NULL') \
    + scale_color_discrete(labels=condition_labels) \
    + scale_fill_discrete(labels=condition_labels) \
    + legend
ggsave("ablation_figure.png", p, width=2.5, height=1)
# + scale_x_discrete(labels=[esc(f'{i}') for i in range(1, 23)]) \
# + ggtitle(esc('TPC-H SF=1 Lineage Querying')) \
# + axis_labels('Query', 'Runtime (log)', 'discrete', 'log10') \

