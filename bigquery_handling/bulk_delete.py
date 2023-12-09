import os
import time
from google.cloud import bigquery
import yaml
import logging 

to_delete = [
] #""# project_id.dataset_id.table_id or project_id.dataset_id

serv_acc = "" #path to serv_account




log_file = os.path.dirname(os.path.abspath(__file__)) + "/deletion_prod.log"
logger = logging.getLogger('deletion')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(log_file, mode="w")
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serv_acc
client = bigquery.Client()
REPORT = []


logger.info("STARTED DELETING PROCESS ")
for t in to_delete:
    logger.info(t)

    row = {
        "entity":t,
        "status":"N",
        "error":"N"
    }
    if len(t.split(".")) == 2:
        entity = t.split(".")[-1] 
        try:
            client.delete_dataset(
            entity, delete_contents=True, not_found_ok=True
            )  # Make an API request.
            row['status'] = "Y"
        except Exception as e:
            row['error'] = "{0}".format(e)

    else:
        try:
            client.delete_table(t, not_found_ok=True)     
            row['status'] = "Y"
            logger.info("removed")
        except Exception as e:
            print(e)
            row['error'] = "{0}".format(e)
            logger.info("{0}".format(str(e)))

    REPORT.append(row)
    print("deleted = ", row['status'])
    time.sleep(3)

logger.info("COMPLETED DELETING PROCESS ")

#dump report 
import csv
report_name = "2_"+__file__.replace(".py", ".csv")
op = open(report_name, "w", newline='')
headers = ['entity', 'status', 'error']
data = csv.DictWriter(op, delimiter=',', fieldnames=headers)
data.writerow(dict((heads, heads) for heads in headers))
data.writerows(REPORT)


