import json
from datetime import datetime

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError


class AzureBlobEventWriter:
    """
    Writes event data to Azure Blob Storage as JSON.

    Usage:
        writer = AzureBlobEventWriter()
        writer.write_events(list_of_events)
        writer.write_event(single_event)
    """

    def __init__(self, storage_account_name="xpdataproject", container_name="events", credential=None):
        """
        Args:
            storage_account_name (str): Name of the Azure Storage account.
            container_name (str): Blob container to write into (created if it doesn't exist).
            credential: An azure-identity credential. Defaults to DefaultAzureCredential(),
                        which uses your local `az login` session.
        """
        self.storage_account_name = storage_account_name
        self.storage_account_url = f"https://{storage_account_name}.blob.core.windows.net"
        self.container_name = container_name
        self.credential = credential or DefaultAzureCredential()

        self.blob_service_client = BlobServiceClient(
            account_url=self.storage_account_url,
            credential=self.credential
        )
        self._container_client = None

    @property
    def container_client(self):
        """Container client, created lazily on first use (and the container itself if needed)."""
        if self._container_client is None:
            self._container_client = self._get_or_create_container()
        return self._container_client

    def _get_or_create_container(self):
        container_client = self.blob_service_client.get_container_client(self.container_name)
        try:
            container_client.create_container()
        except ResourceExistsError:
            pass  # container already exists - fine
        return container_client

    def _upload(self, data, blob_name):
        json_data = json.dumps(data, indent=2)
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.upload_blob(json_data, overwrite=True)
            return blob_client.url
        except Exception as e:
            print(f"Error uploading to Azure Blob Storage: {e}")
            raise

    def write_events(self, events, blob_name=None):
        """
        Write a list of events to a single JSON blob.

        Args:
            events (list): List of event dictionaries to upload.
            blob_name (str): Optional blob filename; auto-generated (timestamped) if omitted.

        Returns:
            str: URL of the uploaded blob.
        """
        if blob_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            blob_name = f"events_{timestamp}.json"

        url = self._upload(events, blob_name)
        print(f"Successfully uploaded {len(events)} events to blob: {blob_name}")
        return url

    def write_event(self, event, blob_name=None):
        """
        Write a single event to its own JSON blob.

        Args:
            event (dict): Single event dictionary to upload.
            blob_name (str): Optional blob filename; auto-generated (timestamped) if omitted.

        Returns:
            str: URL of the uploaded blob.
        """
        if blob_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            blob_name = f"event_{timestamp}.json"

        url = self._upload(event, blob_name)
        print(f"Successfully uploaded event to blob: {blob_name}")
        return url