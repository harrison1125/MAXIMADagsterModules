from dagster import resource, StringSource
from girder_client import GirderClient as GC

@resource(config_schema={"api_url": StringSource, "api_key": StringSource})
def GirderClient(context):
    api_url = context.resource_config["api_url"]
    api_key = context.resource_config["api_key"]
    client = GC(apiUrl=api_url)
    client.authenticate(apiKey=api_key)
    return client