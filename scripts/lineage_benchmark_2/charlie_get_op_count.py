import json

outer = ['base', 'perm', '0']
inner = range(1, 23)

print("queryId,mode,opCount,joinCount")

def count_children(query_profile):
    count = 1 # self
    for child in query_profile["children"]:
        count = count + count_children(child) # increment by number of (grand)children
    return count

def count_joins(query_profile):
    count = 1 if 'JOIN' in query_profile['name'] else 0 # self
    for child in query_profile["children"]:
        count = count + count_joins(child) # increment by number of (grand)children
    return count

for i in outer:
    for j in inner:
        if i == 'perm' and j == 22:
            continue # Skip since perm can't do this one
        f = open(f'profile_output2/profile_output_{j}_{i}.json')
        query_profile = json.load(f)
        opCount = count_children(query_profile["children"][0])
        joinCount = count_joins(query_profile["children"][0])
        print(f'{j},{"LINEAGE" if i == "0" else i},{opCount},{joinCount}')
