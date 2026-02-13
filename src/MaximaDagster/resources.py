from dagster import ConfigurableResource

class GirderClient(ConfigurableResource):
    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url
        self.api_key = api_key

    def connect(self):
        # Here you would implement the logic to connect to the Girder API
        # using the provided api_url and api_key.
        pass

    def download_file(self, file_id: str, destination: str):
        # Implement logic to download a file from Girder using the file_id
        # and save it to the specified destination.
        pass

    def upload_file(self, file_path: str, folder_id: str):
        # Implement logic to upload a file to Girder into the specified folder_id.
        pass