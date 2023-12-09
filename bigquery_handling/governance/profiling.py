import json
import os
import io
from google.cloud import bigquery
from nested_handler import NestedHandler
from flatten_handler import FlattenHandler
from table_level import TableLevel
import pandas as pd
from base_logger import logger
import os
import sys

logger.info("########################## START ##########################")
PREFIX = sys.argv[1].strip()  # os.environ.get('prefix').strip()
COLUMN_LEVEL_DATA = []
TABLE_LEVEL_DATA = []
TABLE_STRUCTURE = {}

if not PREFIX:
    logger.info("Usage: python3 profiling.py <prefix>")
    exit()

logger.info("Prefix -> {0}".format(PREFIX))


def flatten_process(table_id, table_obj, data, prefix=""):
    table_structure = {
        table_id: {"type_of_table": "flatten", "structure": {}, "table_obj": table_obj}
    }

    def algo(table_id, data, prefix):
        for item in data:
            if item['type'].upper() == "RECORD":
                table_structure[table_id]['type_of_table'] = "nested"
                algo(table_id, item['fields'], prefix=prefix + item['name'] + ".")

            table_structure[table_id]['structure'].update({
                prefix + item['name']: {"type": item['type'], "mode": item['mode']}
            })
    algo(table_id, data, prefix)

    return table_structure


bq_client = bigquery.Client(project=PREFIX.split(".")[0])
tables = list(bq_client.list_tables(PREFIX.split(".")[1]))
logger.info("########################## PROFILING ON    ")

for table in tables:

    table_id = table.project + "." + table.dataset_id + "." + table.table_id
    if not table_id.startswith(PREFIX):
        continue

    table = bq_client.get_table(table_id)
    op = io.StringIO("")
    t = bq_client.schema_to_json(table.schema, op)
    op = op.getvalue()
    op = json.loads(op)
    results = flatten_process(table_id, table, op)
    TABLE_STRUCTURE.update(results)
    logger.info("{0} {1}".format(table_id, results[table_id]['type_of_table']))

if not TABLE_STRUCTURE:
    logger.info("########################## No Tables/Views With Prefix {0}".format(PREFIX))
    exit()

nested_obj = NestedHandler(
    table_structure=TABLE_STRUCTURE
)
flatten_obj = FlattenHandler(
    table_structure=TABLE_STRUCTURE
)
table_level_obj = TableLevel(
    table_structure=TABLE_STRUCTURE
)

TABLE_LEVEL_DATA = table_level_obj.get_results()
nested_results = nested_obj.get_results()
flatten_results = flatten_obj.get_results()

if nested_results:
    COLUMN_LEVEL_DATA.extend(nested_results)

if flatten_results:
    COLUMN_LEVEL_DATA.extend(flatten_results)

COLUMN_LEVEL_DATA = pd.concat(COLUMN_LEVEL_DATA)

with pd.ExcelWriter(os.path.dirname(os.path.abspath(__file__)) + "/profiling.xlsx") as writer:
    COLUMN_LEVEL_DATA.to_excel(writer, engine='xlsxwriter', index=False, sheet_name="PROFILING_cols")
    TABLE_LEVEL_DATA.to_excel(writer, engine='xlsxwriter', index=False, sheet_name="PROFILING_tb")

logger.info("########################## END ##########################")
