"""Google Functions that collect data for DOTUFP, and are triggered by Pub/Sub."""

from jinja2 import Environment, FileSystemLoader
from google.cloud import storage
from google.cloud import kms_v1
import redis
import json
# from jinja2 import Template

def _update_redis(key: str, value: str):
    secrets = _get_secrets()

    r = redis.Redis(host=secrets['redis']['host'],
                    port=secrets['redis']['port'],
                    password=secrets['redis']['password'])

    current_value = r.get(key)

    if int(current_value) <= int(value) <= 1.1 * int(current_value):
        r.set(key, value)
        print(f'{key} is now {value}')
    else:
        raise ValueError(f'new {key} is maybe {value}, current value is {current_value}')

def _upload_data(bucket_name: str, blob_name: str, data: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(data, content_type='text/html')

def _get_secrets():
    """Fetch and decrypt project secrets."""
    # get encrypted secrets
    storage_client = storage.Client()
    bucket = storage_client.bucket('dotufp-sm')
    blob = bucket.blob('vaqmr-secrets.v3.json.encrypted')
    ciphertext = blob.download_as_string()

    # decrypt secrets
    kms_client = kms_v1.KeyManagementServiceClient()
    key_name = kms_client.crypto_key_path('secret-manager-258521', 'global', 'dotufp-secrets', 'dotufp-secrets-key')
    secrets = kms_client.decrypt(key_name, ciphertext)

    return json.loads(secrets.plaintext)

def update_planets_site(event, context):
    """Check most recent exoplanet data and update website. Triggered by Pub/Sub.

    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    Returns:
        None; the output is written to Storage.

    """
    # Get most recent data
    secrets = _get_secrets()

    r = redis.Redis(host=secrets['redis']['host'],
                    port=secrets['redis']['port'],
                    password=secrets['redis']['password'])

    num_planets_eu = int(r.get('planets_data_eu'))
    num_planets_nasa = int(r.get('planets_data_nasa'))

    # Update site
    planets_data = {'num_planets_eu': num_planets_eu,
                    'num_planets_nasa': num_planets_nasa}

    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)

    template = env.get_template('template.html')
    output = template.render(planets_data=planets_data)

    # with open('site/index.html','w') as f:
    #     f.write(output)

    _upload_data('www.howmanyplanetsarethere.com',
                 'index.html',
                 output)
