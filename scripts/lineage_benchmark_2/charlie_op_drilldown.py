import json

outer = range(1, 23)
inner = range(0, 5)

print("queryId,mode,iterId,opName,duration,cardinality")

def print_query_profile(queryId, iterId, query_profile):
    opName = query_profile['name']
    duration = query_profile['timing']
    cardinality = query_profile['cardinality']
    print(f'{queryId},LINEAGE,{iterId},{opName},{duration},{cardinality}')
    for child in query_profile['children']:
        print_query_profile(queryId, iterId, child)

for i in outer:
    for j in inner:
        f = open(f'profile_output2/profile_output_{i}_{j}.json')
        query_profile = json.load(f)
        print_query_profile(i, j, query_profile["children"][0])
