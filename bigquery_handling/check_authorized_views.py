import os
from google.cloud import bigquery

###############################################################################################################################################



views = [
#""# project_id.dataset_id.table_id
]  # add the views which you want to check access
dataset = ""  #  dataset_id  datasets to check views access  

serv_acc = ""# path to serv_account



###############################################################################################################################################




# start
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serv_acc
client = bigquery.Client()
REPORT = [['dataset', 'view', 'entity_id', 'enitity_type', 'user_by_email' ]]
dt = client.get_dataset(dataset)
access_view_entries = [str(item.view) for item in dt.access_entries]
for v in views:
    if v in access_view_entries:
        status = "Y"
    else:
        status = "N"
    
    print(f"{v} {status}")
    







