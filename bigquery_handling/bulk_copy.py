import os
import time
from google.cloud import bigquery
import yaml
import logging



to_copy = [
] #""# project_id.dataset_id.table_id, 
serv_acc = "" #path to serv_account




log_file = os.path.dirname(os.path.abspath(__file__)) + "/prod_cleaning_bkp.log"
logger = logging.getLogger('clean_bq')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(log_file, mode="a")
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


logger.info("Starting Moving To Sandbox Dataset ")
for t in to_copy:
    row = {
        "old_table":t,
        "new_table":"tc-sc-bi-bigdata-dfl-prod.sandbox_dfl_prod.{}".format(t.split(".")[-1]),
        "status":"N",
        "error":"N"
    }
    logger.info(t)
    try:
        job = client.copy_table(t, row["new_table"])
        job.result()    
        row['status'] = "Y"
        logger.info(" Copied To {0}".format(row['new_table']))
    except Exception as e:
        print(e)
        logger.error("{0}".format(str(e)))

    REPORT.append(row)
    print("copied = ", row['status'], row['old_table'], row['new_table'])
    time.sleep(1)

logger.info("Completed Moving To Sandbox Dataset ")

#dump report 
import csv
report_name = __file__.replace(".py", ".csv")
op = open(report_name, "w", newline='')
headers = ['old_table', 'new_table', 'status', 'error']
data = csv.DictWriter(op, delimiter=',', fieldnames=headers)
data.writerow(dict((heads, heads) for heads in headers))
data.writerows(REPORT)


