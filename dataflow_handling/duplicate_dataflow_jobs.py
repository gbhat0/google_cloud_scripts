import google.auth
from google.auth.transport.requests import AuthorizedSession
import os
import csv


QA = {
    "serv_acc":"",
    "project_id":"",
    "location":""
}
PROD = {
    "serv_acc":"",
    "project_id":"",
    "location":""
}



#### begin 


class DuplicateJobs:

    def __init__(self, projectId, location, state="Active"):
        self.projectId = projectId
        self.location = location
        self.state = state
        self.pageSize = 10

        self.all_jobs_results = []
        self.error_message = []

        self.results = []

    def getAllJobs(self, nextPageToken=None):
        if nextPageToken:
            url = "https://dataflow.googleapis.com/v1b3/projects/{projectId}/locations/{location}/jobs?filter={state}&pageSize={pageSize}&pageToken=" + nextPageToken
        else:
            url = "https://dataflow.googleapis.com/v1b3/projects/{projectId}/locations/{location}/jobs?filter={state}&pageSize={pageSize}"

        response = authed_session.request("GET",
                                          url.format(
                                              projectId=self.projectId, location=self.location,
                                              state=self.state,
                                              pageSize=self.pageSize
                                          ))

        if response.json().get("jobs") and type(response.json().get("jobs")) == list:
            self.all_jobs_results.extend(response.json().get("jobs"))

        if response.json().get("nextPageToken"):
            self.getAllJobs(nextPageToken=response.json().get("nextPageToken"))

    def getJobErrorMessage(self, jobId, nextPageToken=None):
        if nextPageToken:
            url = "https://dataflow.googleapis.com/v1b3/projects/{projectId}/locations/{location}/jobs/" + jobId + "/messages" + "?pageToken=" + nextPageToken
        else:
            url = "https://dataflow.googleapis.com/v1b3/projects/{projectId}/locations/{location}/jobs/" + jobId + "/messages"

        response = authed_session.request("GET",
                                          url.format(
                                              projectId=self.projectId,
                                              location=self.location
                                          ))
        if response.json().get("jobMessages") and type(response.json().get("jobMessages")) == list:
            for msg in response.json().get("jobMessages"):
                if msg.get("messageImportance") == "JOB_MESSAGE_ERROR":
                    self.error_message.append(msg)

        if response.json().get("nextPageToken"):
            self.getJobErrorMessage(nextPageToken=response.json().get("nextPageToken"))

    def start(self):
        self.getAllJobs()
        for item in self.all_jobs_results:
            if item[
                'type'] == "JOB_TYPE_STREAMING":  # and item['name'] == "corp-transport-planning-ingester-547c47b6-de5e-4f13-89ab-882ba103c51d-bd194f12":
                if len(self.error_message) > 0:
                    self.error_message = "\n".join(self.error_message[len(self.error_message) - 5:])
                else:
                    self.error_message = ""

                common_name = "-".join(item['name'].split("-")[:len(item['name'].split("-")) - 6])
                item.update({
                    "JOB_MESSAGE_ERROR": self.error_message,
                    "common_name": common_name
                })
                self.results.append(item)
                self.error_message = []


def analysis(qa, prod):
    csv_data = [["QA", "QA_CREATED", "QA_STATE", "QA_ERROR", "PROD", "PROD_CREATED", "PROD_STATE", "PROD_ERROR"]]
    for qa_job in qa:
        for prod_job in prod:
            if qa_job['common_name'] == prod_job['common_name']:
                print(qa_data['name'])
                csv_data.append(
                    [qa_job['name'], qa_job['createTime'], qa_job['currentState'], qa_job['JOB_MESSAGE_ERROR'],
                     prod_job['name'], prod_job['createTime'], prod_job['currentState'], prod_job['JOB_MESSAGE_ERROR']])
                break

    with open(os.path.dirname(os.path.abspath(__file__))+"/duplicate_jobs_results.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(csv_data)



if __name__ == "__main__":

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = QA['serv_acc']
    AUTH_SCOPE = ["https://www.googleapis.com/auth/cloud-platform"]
    CREDENTIALS, _ = google.auth.default(scopes=AUTH_SCOPE)
    authed_session = AuthorizedSession(CREDENTIALS)
    obj = DuplicateJobs(
       projectId=QA['project_id'],
        location=QA['location']
    )
    obj.start()
    qa_data = obj.results

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PROD['serv_acc']
    AUTH_SCOPE = ["https://www.googleapis.com/auth/cloud-platform"]
    CREDENTIALS, _ = google.auth.default(scopes=AUTH_SCOPE)
    authed_session = AuthorizedSession(CREDENTIALS)
    obj = DuplicateJobs(
        projectId=PROD['project_id'],
        location=PROD['location']
    )
    obj.start()
    prod_data = obj.results
    analysis(qa_data, prod_data)










