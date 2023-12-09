import os 
from google.cloud import pubsub_v1
from google.api_core import retry
import json 
import gzip


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/gopalkrishna/dfl-qa-credentials.json"



sub_project = ""
sub_id = ""	
gzip_compressed = False # or True



subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(sub_project, sub_id)
response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 5000},
            retry=retry.Retry(deadline=300),
            )     
data = []
for  index, message in enumerate(response.received_messages):
    if gzip_compressed:
        msg = json.loads(gzip.decompress(message.message.data).decode())
    else:
        msg = dict(json.loads(message.message.data.decode()))
    
    attr = {}
    print(message.message.attributes)
    for k, v in message.message.attributes.items():
        print("attr")
        print(k, v)
        attr.update({k:v})
    
    attr.update({
        "messageId":message.message.message_id
    })
    msg.update({
    "header":attr
    })  
    print(msg)
    
print("No Of Messags Pulled . ", len(data))
