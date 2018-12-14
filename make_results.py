import json, collections, os, glob
import matplotlib.pyplot as plt

json_dict = {}

def process_file():
    for item, key in json_file[1].items():
        if item not in json_dict:
            json_dict[item] = key
        else:
            json_dict[item] += key

for filename in glob.glob(os.getcwd()+'/*.json'):
    if filename.endswith(".json"):
        with open(filename, "r") as f:
            json_file = json.load(f)
        process_file()
    else:
        print "huh"

#descending order
#sorted_dict = collections.OrderedDict(sorted(json_dict.items(), key=lambda t: t[1], reverse=True))

#get top 20 values
top_20 = collections.OrderedDict(sorted(json_dict.items(), key=lambda t: t[1], reverse=True)[:20])

# print function
# for item, key in top_20.items():
#     print item, ":", key

# plot the data
marked = [0, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
barplot = plt.bar(range(len(top_20)), list(top_20.values()))
plt.xticks(range(len(top_20)), list(top_20.keys()), rotation='vertical')
plt.subplots_adjust(bottom=0.4)
plt.title("CNAME domain results")
for i in marked:
    barplot[i].set_color('r')
plt.show()

# write data to json
# with open('data.json', 'wb') as outfile:
#     json.dump(sorted_dict, outfile)