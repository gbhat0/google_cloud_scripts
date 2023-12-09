import yaml, csv, os

contract_path = "/Users/gopalkrishna/dfl-data-etl/daily_usage_scripts/contract_to_catalog/dispatch.yaml"
event = "dispatchTaskCANCELLED"  # "FOCreated-Event" #"ReverseLogisticOrderCreated-Event"#collectTaskDONE"
headers = ["field", "datatype", "required", "definition", "example"]
csv_data = []

with open(contract_path) as cf:
    contract = yaml.load(cf, yaml.FullLoader)

event_data = contract.get("components").get("schemas").get(event)
if not event_data:
    print(f"{event} not found in contract...")
    exit()


def getRefFromContract(key):
    data = contract['components']['schemas'][key]
    if "$ref" in data:
        getRefFromContract(key=data['$ref'].split("/")[-1])

    return data


def updateCsv(field, datatype, required, definition, example):
    if datatype:
        csv_data.append({
            "field": field,
            "datatype": datatype,
            "required": required,
            "definition": definition,
            "example": example
        })


def getFromContract(data, prefix=""):
    required = data.get("required")
    if not required:
        required = []

    ### corner case ####
    if "$ref" in data:
        """ recognize event atributes """
        temp = getRefFromContract(key=data['$ref'].split("/")[-1])
        data = temp

    ### corner case ####

    for val in data['properties']:
        if "$ref" in data['properties'][val]:
            temp = getRefFromContract(key=data['properties'][val]['$ref'].split("/")[-1])
            dt = temp.get("type") if temp.get("type") else data.get("type")
            if "items" in temp:
                temp = temp['items']
                dt = "{0}({1})".format(dt, temp.get("type"))
            updateCsv(
                field=prefix + val,
                datatype=dt,
                required="Y" if val in required else "N",
                definition=temp.get("description"),
                example=temp.get("example")
            )
            getFromContract(data=temp, prefix=prefix + val + ".")

        elif data['properties'][val]['type'] == "object":
            if "oneOf" in data['properties'][val]:
                temp = data['properties'][val]['oneOf'][0]
            else:
                temp = data['properties'][val]
            if "$ref" in temp:
                temp = getRefFromContract(key=temp['$ref'].split("/")[-1])
            getFromContract(data=temp, prefix=prefix + val + ".")


        elif data['properties'][val]['type'] == "array":
            if "oneOf" in data['properties'][val]['items']:
                temp = data['properties'][val]['items']['oneOf'][0]
            else:
                temp = data['properties'][val]['items']
            if "$ref" in temp:
                dt = "array"
                test = getRefFromContract(key=temp['$ref'].split("/")[-1])
                if test.get("type"):
                    dt = "{0}({1})".format(dt, test.get("type"))
                updateCsv(
                    field=prefix + val,
                    datatype=dt,
                    required="Y" if val in required else "N",
                    definition=temp.get("description"),
                    example=temp.get("example")
                )
                getFromContract(data=test, prefix=prefix + val + ".")

            elif "properties" not in temp:
                updateCsv(
                    field=prefix + val,
                    datatype="array (" + temp.get("type") + ")" if temp.get("type") else 'array',
                    required="Y" if val in required else "N",
                    definition=temp.get("description"),
                    example=temp.get("example")
                )
            else:
                getFromContract(data=temp, prefix=prefix + val + ".")

        updateCsv(
            field=prefix + val,
            datatype=data['properties'][val].get("type"),
            required="Y" if val in required else "N",
            definition=data['properties'][val].get("description"),
            example=data['properties'][val].get("example")
        )


getFromContract(data=event_data)

with open("/Users/gopalkrishna/dfl-data-etl/daily_usage_scripts/events_bq_match/testing.csv", "a") as op:
    data = csv.DictWriter(op, delimiter=',', fieldnames=headers)
    data.writerow(dict((heads, heads) for heads in headers))
    data.writerows(csv_data)


