from google.cloud import bigquery
import os


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "" # path to serv_account
client = bigquery.Client()
datasets = list(client.list_datasets())  # Make an API request.
project = client.project
data = {}


if datasets:
    print("Datasets in project {}:".format(project))
    for dataset in datasets:
        print(dataset.dataset_id)
        tables = list(client.list_tables(dataset.dataset_id))
        for t in tables:
            if t.table_id in data:
                data[t.table_id].append(t.dataset_id)
            else:
                data[t.table_id] = [t.dataset_id]
else:
    print("{} project does not contain any datasets.".format(project))
    exit()



report = []
for items in data:
    if len(data[items]) > 1:
        row = {
            "project":client.project,
            "table":items,
            "count":len(data[items]),
            "datasets":" , ".join(data[items])
        }
        report.append(row)




import csv
report_name = "/".join(__name__.split("/")[:-1])+"duplicates_bq.csv"
op = open(report_name, "w", newline='')
headers = ['project', 'table', 'count', 'datasets']
data = csv.DictWriter(op, delimiter=',', fieldnames=headers)
data.writerow(dict((heads, heads) for heads in headers))
data.writerows(report)






