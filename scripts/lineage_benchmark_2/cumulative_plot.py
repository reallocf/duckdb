import pandas as pd
from pygg import *

number_of_queries = 5
smokedduck_capture = pd.read_csv('tpch_benchmark_capture_feb28.csv')
smokedduck_postprocess = pd.read_csv('postprocess_times3.csv')
smokedduck_query = pd.read_csv('tpch_sf1_2_28_2023_2.csv')
logical_rid_capture = pd.read_csv('tpch_benchmark_capture_feb28.csv')
logical_rid_query = pd.read_csv('tpch_bw_notes_mar1_sf1_lineage_type_Logical-RID.csv')


smokedduck_capture = smokedduck_capture[smokedduck_capture['notes'] == 'feb28_full']
smokedduck_capture = smokedduck_capture[smokedduck_capture['sf'] == 1]
smokedduck_capture = smokedduck_capture[['query', 'runtime']]
smokedduck_capture = smokedduck_capture.reset_index()
smokedduck_postprocess = smokedduck_postprocess[smokedduck_postprocess['ablation'] == -1]
smokedduck_postprocess = smokedduck_postprocess[smokedduck_postprocess['sf'] == 1]
smokedduck_postprocess['avg_duration'] = [(
                                                  smokedduck_postprocess['time_in_sec1'][i] +
                                                  smokedduck_postprocess['time_in_sec2'][i] +
                                                  smokedduck_postprocess['time_in_sec3'][i] +
                                                  smokedduck_postprocess['time_in_sec4'][i] +
                                                  smokedduck_postprocess['time_in_sec5'][i]
                                          ) / 5 for i in range(len(smokedduck_postprocess))]
smokedduck_postprocess = smokedduck_postprocess[['query_id', 'avg_duration']]
smokedduck_postprocess = smokedduck_postprocess.rename(columns={'queryId': 'query', 'avg_duration': 'runtime'})
smokedduck_query = smokedduck_query[['queryId', 'avg_duration']]
smokedduck_query = smokedduck_query.rename(columns={'queryId': 'query', 'avg_duration': 'runtime'})
logical_rid_capture = logical_rid_capture[logical_rid_capture['lineage_type'] == 'Logical-RID']
logical_rid_capture = logical_rid_capture[logical_rid_capture['sf'] == 1]
logical_rid_capture = logical_rid_capture[['query', 'runtime']]
logical_rid_capture = logical_rid_capture.reset_index()
logical_rid_query = logical_rid_query[['query', 'runtime']]
print(smokedduck_capture, smokedduck_postprocess, smokedduck_query, logical_rid_capture, logical_rid_query)

for i in range(22):
    print(
        i,
        smokedduck_capture['runtime'][i] + smokedduck_postprocess['runtime'][i] > logical_rid_capture['runtime'][i],
        smokedduck_capture['runtime'][i] + smokedduck_postprocess['runtime'][i],
        logical_rid_capture['runtime'][i]
    )

query_final = [0]
smokedduck_final = [0]
logical_final = [0]
smokedduck_running_val = 0
logical_running_val = 0
for i in range(22):
    # Capture
    smokedduck_running_val += smokedduck_capture['runtime'][i]
    smokedduck_final.append(smokedduck_running_val)
    logical_running_val += logical_rid_capture['runtime'][i]
    logical_final.append(logical_running_val)
    query_final.append(i + 0.333)
    # Postprocessing
    smokedduck_running_val += smokedduck_postprocess['runtime'][i]
    smokedduck_final.append(smokedduck_running_val)
    logical_final.append(logical_running_val) # No Logical work to do - just repeat
    query_final.append(i + 0.667)
    # Querying
    smokedduck_running_val += smokedduck_query['runtime'][i] * number_of_queries
    smokedduck_final.append(smokedduck_running_val)
    logical_running_val += logical_rid_query['runtime'][i] * number_of_queries
    logical_final.append(logical_running_val)
    query_final.append(i + 1)
data = pd.DataFrame()
data['runtime'] = smokedduck_final + logical_final
data['query'] = query_final + query_final
data['system'] = ['SmokedDuck' for _ in range(len(smokedduck_final))] + ['Logical-RID' for _ in range(len(logical_final))]
data = data.rename(columns={'query': 'Query', 'runtime': 'Runtime'})
print(data.to_string())

legend = theme_bw() + theme(**{
    "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
    "legend.justification":"c(1,0)",
    # "legend.position":"c(1,0)",
    "legend.key" : element_blank(),
    "legend.title":element_blank(),
    "text": element_text(colour = "'#333333'", size=11, family = "'Arial'"),
    "axis.text": element_text(colour = "'#333333'", size=9),
    # "plot.background": element_blank(),
    # "panel.border": element_rect(color=esc("#e0e0e0")),
    # "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
    "strip.text": element_text(color=esc("#333333")),
    "legend.position": esc("bottom"),
    "legend.margin": margin(t = 0, r = 0, b = 0, l = 0, unit = esc("pt")),
    "legend.text": element_text(colour = "'#333333'", size=9, family = "'Arial'"),
    "legend.key.size": unit(8, esc('pt')),
})

p = ggplot(data, aes(x='Query', y='Runtime', condition='system', color='system', fill='system', group='system')) \
    + geom_line() \
    + scale_y_continuous(
        name=esc('Cumulative Runtime'),
        breaks=[0, 10, 20, 30, 40, 50, 60],
        labels=[esc('0s'), esc('10s'), esc('20s'), esc('30s'), esc('40s'), esc('50s'), esc('60s')]
    ) \
    + scale_x_continuous(
        breaks=[i for i in range(0, 23)],
        minor_breaks=[i + 0.333 for i in range(0, 22)] + [i + 0.667 for i in range(0, 22)],
        labels=[esc(f'{i}') for i in range(1, 23)] + [esc('')]
    ) \
    + legend
ggsave("cumulative.png", p, width=6, height=2)
# + scale_x_discrete(labels=[esc(f'{i}') for i in range(1, 23)]) \
# + ggtitle(esc('TPC-H SF=1 Lineage Querying')) \
# + axis_labels('Query', 'Runtime (log)', 'discrete', 'log10') \
# name=esc("Runtime (log)"), breaks=[1, 10, 100, 1000], labels=[esc('1ms'), esc('10ms'), esc('100ms'), esc('1000ms')]
# breaks=[i for i in range(1, 23)], minor_breaks='NULL'
# + scale_x_discrete(breaks=[1], labels=[esc('1')]) \
