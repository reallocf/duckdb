import csv
import numpy as np

data = []
header = None
with open("data/fires_with_dropped_cols.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    for row in csv_reader:
        if header is None:
            header = row[2:]
            continue

        data.append(row[2:])

new_rows = []
FACTOR = len(data) * 10
for i in range(FACTOR):
    random_index = np.random.randint(0,len(data), dtype='int')
    row = data[random_index]
    new_rows.append(row)

print(header)
print("data size before: {}".format(len(data)))
data.extend(new_rows)
print("data size after: {}".format(len(data)))

with open("data/fires_large.csv", "w") as csv_file:
    write = csv.writer(csv_file)
    write.writerow(header)
    write.writerows(data)
