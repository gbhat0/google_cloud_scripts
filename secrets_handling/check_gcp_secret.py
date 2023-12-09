import os

serv_acc = ""
project_id = ""
secret_name = ""


def get_secret(project_id, secret_id):
    """
    Get information about the given secret. This only returns metadata about
    the secret container, not any secret material.
    """
    # Import the Secret Manager client library.
    from google.cloud import secretmanager
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret.
    name = client.secret_path(project_id, secret_id)
    print(name)
    response = None
    try:
        secret_response = client.get_secret(request={"name": name})
        if secret_response is not None:
            secret_value = name + "/versions/latest"
            response = client.access_secret_version(name=secret_value)
            print(response.payload.data.decode('UTF-8'))

    except:
        print("No Secret present in GCP")

    if response is not None:
        # Print data about the secret.
        print("Got secret {} ".format(response.name))



os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = serv_acc 

get_secret(project_id, secret_name)
