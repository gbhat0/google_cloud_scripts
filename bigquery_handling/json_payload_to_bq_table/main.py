import json, yaml
from google.cloud import bigquery


def addValue(event, src_field_name, bq_datatype=None, transformation=None):
    val = None
    if event.get(src_field_name) is not None:
        val = event.get(src_field_name)

    return val


def handleRepeatedValues(ff_data, event_data, row):
    for data in event_data:
        for key in ff_data:
            row.append({key: None})
            if data.get(ff_data[key]['source']):
                row[-1][key] = convertJsonToBigqueryRow(ff_data, data, {})


def convertJsonToBigqueryRow(ff_data, event_data, row):
    for key in ff_data:
        row.update({key: None})

        if ff_data[key].get("fields") and str(ff_data[key].get("mode")).lower() == "repeated":
            if event_data.get(ff_data[key]['source']):
                row[key] = handleRepeatedValues(ff_data[key]['fields'], event_data.get(ff_data[key]['source']), [])

        if ff_data[key].get("fields") and (
                str(ff_data[key].get("mode")).lower() == "nullable" or ff_data[key].get("mode") is None):
            if event_data.get(ff_data[key]['source']):
                row[key] = convertJsonToBigqueryRow(ff_data[key]['fields'], event_data.get(ff_data[key]['source']), {})

        else:
            row[key] = addValue(event_data, ff_data[key]['source'], ff_data[key]['datatype'],
                                ff_data[key].get("transformation"))

    return row


with open("/Users/gopalkrishna/Downloads/ff.yaml") as f:
    ff = yaml.load(f, yaml.FullLoader)
    ff = ff.copy()

with open("/Users/gopalkrishna/Downloads/test.json") as f:
    event = json.loads(f.read())
    event = event.copy()

results = convertJsonToBigqueryRow(ff, event, {})
print(json.dumps(results))