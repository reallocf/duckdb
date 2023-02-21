import pandas as pd
from pygg import *
from wuutils import *

# for each query,
def overhead(base, extra):
    #return max(((extra-base)/base)*100, 0)
    return extra*1000

df_micro_nolineage = pd.read_csv("output_logs/micro_benchmark_notes_nolineage.csv")
df_micro_nooffset = pd.read_csv("output_logs/micro_benchmark_notes_lineage_nooffset.csv")
#df_micro_offset = pd.read_csv("output_logs/micro_benchmark_notes_lineage_offset.csv")
df_micro_offset = pd.read_csv("output_logs/micro_benchmark_notes_smokedduck_capture.csv")
df_micro_tables = pd.read_csv("output_logs/micro_benchmark_notes_lineage_tables.csv")
df_micro_perm_group_concat = pd.read_csv("output_logs/micro_benchmark_notes_perm_group_concat.csv")
df_micro_perm_list = pd.read_csv("output_logs/micro_benchmark_notes_perm_list.csv")
df_micro_perm_join = pd.read_csv("output_logs/micro_benchmark_notes_perm_join_table.csv")

for q in ["groupby", "orderby", "filter", "pkfk", "m:n"]:
    data = []
    for i in range(len(df_micro_nolineage)) :
        query_type = df_micro_nolineage.loc[i, "query"]
        if query_type != q:
            continue
        cardinality = df_micro_nolineage.loc[i, "cardinality"]
        groups = df_micro_nolineage.loc[i, "groups"]
        no_lineage_runtime = df_micro_nolineage.loc[i, "runtime"]
        nooffset_runtime = df_micro_nooffset.loc[i, "runtime"]
        offset_runtime = df_micro_offset.loc[i, "runtime"]
        tables_runtime = df_micro_tables.loc[i, "runtime"]
        perm_group_concat_runtime = df_micro_perm_group_concat.loc[i, "runtime"]
        perm_list_runtime = df_micro_perm_list.loc[i, "runtime"]
        perm_join_runtime = df_micro_perm_join.loc[i, "runtime"]
        print(df_micro_nolineage.loc[i, "query"], df_micro_nolineage.loc[i, "cardinality"], df_micro_nolineage.loc[i, "groups"])
        print(no_lineage_runtime, nooffset_runtime, offset_runtime, tables_runtime, perm_group_concat_runtime, perm_list_runtime, perm_join_runtime)
        print("Overhead nooffset: ", overhead(no_lineage_runtime, nooffset_runtime))
        print("Overhead offset: ", overhead(no_lineage_runtime, offset_runtime))
        print("Overhead tables: ", overhead(no_lineage_runtime, tables_runtime))
        print("Overhead group concat: ", overhead(no_lineage_runtime, perm_group_concat_runtime))
        print("Overhead list: ", overhead(no_lineage_runtime, perm_list_runtime))
        print("Overhead join: ", overhead(no_lineage_runtime, perm_join_runtime))
        print("*****************")
        data.append(dict(system="Baseline", query=query_type, groups=groups, cardinality=cardinality, overhead=overhead(no_lineage_runtime, no_lineage_runtime)))
        #data.append(dict(system="nooffset", query=query_type, groups=groups, cardinality=cardinality, overhead=overhead(no_lineage_runtime, nooffset_runtime)))
        data.append(dict(system="SD-Capture", query=query_type, groups=groups, cardinality=cardinality, overhead=overhead(no_lineage_runtime, offset_runtime)))
        data.append(dict(system="SD-Persist", query=query_type, groups=groups, cardinality=cardinality, overhead=overhead(no_lineage_runtime, tables_runtime)))
        #if (query_type == "groupby"):
        #  data.append(dict(system="group_concat", query=query_type, groups=groups, cardinality=cardinality, overhead=overhead(no_lineage_runtime, perm_group_concat_runtime)))
        #  data.append(dict(system="list", query=query_type, groups=groups, cardinality=cardinality, overhead=overhead(no_lineage_runtime, perm_list_runtime)))
        data.append(dict(system="Logical-RID", query=query_type, groups=groups, cardinality=cardinality, overhead=overhead(no_lineage_runtime, perm_join_runtime)))
    if (len(data) == 0):
        continue
    print(data)

    p = ggplot(data, aes(x='system', y='overhead', color='system', fill='system', group='system', shape='system'))
    p += geom_bar(stat=esc('identity'), alpha=0.8, width=0.5) + coord_flip()
    p += facet_wrap("~cardinality", scales=esc("free_x"))
    p += axis_labels('System', "Time (ms)", "discrete")
    p += legend_bottom
    ggsave("micro_overhead_"+q+"_.png", p,  width=6, height=2.5)

data = []
for sf in [1]:

    df_tpch_nolineage = pd.read_csv("output_logs/tpch_benchmark_sf"+str(sf)+".0_notes_nolineage.csv")
    df_tpch_nooffset = pd.read_csv("output_logs/tpch_benchmark_sf"+str(sf)+".0_notes_lineage_nooffset.csv")
    df_tpch_offset = pd.read_csv("output_logs/tpch_benchmark_sf"+str(sf)+".0_notes_lineage_offset.csv")
    df_tpch_tables = pd.read_csv("output_logs/tpch_benchmark_sf"+str(sf)+".0_notes_lineage_tables.csv")
    df_tpch_lineage_query = pd.read_csv("output_logs/tpch_benchmark_sf"+str(sf)+".0_notes_test.csv")
    df_tpch_perm = pd.read_csv("output_logs/tpch_benchmark_sf"+str(sf)+".0_notes_perm_v2.csv")

    for i in range(len(df_tpch_nolineage)) :
        no_lineage_runtime = df_tpch_nolineage.loc[i, "runtime"]

        nooffset_runtime = df_tpch_nooffset.loc[i, "runtime"]
        offset_runtime = df_tpch_offset.loc[i, "runtime"]
        tables_runtime = df_tpch_tables.loc[i, "runtime"]
        lineage_query_runtime_original = df_tpch_lineage_query.loc[i, "runtime"]
        lineage_query_runtime = df_tpch_lineage_query.loc[i, "lineage_query_runtime"]
        perm_runtime = df_tpch_perm.loc[i, "runtime"]
        q = df_tpch_nolineage.loc[i, "query"]
        print(df_tpch_nolineage.loc[i, "query"], df_tpch_nolineage.loc[i, "sf"])
        print(no_lineage_runtime, nooffset_runtime, offset_runtime, tables_runtime)
        print("Overhead nooffset: ", overhead(no_lineage_runtime, nooffset_runtime))
        print("Overhead offset: ", overhead(no_lineage_runtime, offset_runtime))
        print("Overhead tables: ", overhead(no_lineage_runtime, tables_runtime))
        print("Overhead perm: ", overhead(no_lineage_runtime, perm_runtime))
        print("*****************")
        x_label = "q"+str(q)
        data.append(dict(system="Baseline", query=x_label, overhead=no_lineage_runtime*1000))
        #data.append(dict(system="nooffset", query=x_label, overhead=overhead(no_lineage_runtime, nooffset_runtime)))
        data.append(dict(system="SmokedDuck-Capture", query=x_label, overhead=overhead(no_lineage_runtime, offset_runtime)))
        data.append(dict(system="SmokedDuck-Persist", query=x_label, overhead=overhead(no_lineage_runtime, lineage_query_runtime_original)))
        #data.append(dict(system="lineage_query", query=x_label, overhead=overhead(no_lineage_runtime, lineage_query_runtime)))
        #data.append(dict(system="all", query=x_label, overhead=overhead(no_lineage_runtime, lineage_query_runtime+lineage_query_runtime_original)))
        data.append(dict(system="Logical-RID", query=x_label, overhead=overhead(no_lineage_runtime, perm_runtime)))

print(data)

p = ggplot(data, aes(x='query', y='overhead', color='system', fill='system', group='system', shape='system'))
p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
p += axis_labels('Query', "Runtime (ms)", "discrete", "log10")
#p += ylim(lim=[0,300])
p += legend_bottom
ggsave("tpch_overhead.png", p,  width=6, height=3)