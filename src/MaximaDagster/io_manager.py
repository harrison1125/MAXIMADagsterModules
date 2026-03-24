import io
import os
from urllib.parse import urlparse

from dagster import (
    ConfigurableIOManagerFactory,
    InputContext,
    IOManager,
    OutputContext,
)
from girder_client import GirderClient


class GirderIOManager(IOManager):
    """
    IO Manager for reading/writing to Girder with separate source/target folders.
    - load_input(): Download from source_folder_id
    - handle_output(): Upload to target_folder_id
    Enables asset chaining and data persistence.
    NOT CURRENTLY ENABLED
    """

    def __init__(
        self,
        api_url: str,
        api_key: str,
        source_folder_id: str,
        target_folder_id: str,
    ):
        """Initialize manager with Girder credentials and folder IDs."""
        self._cli = GirderClient(apiUrl=api_url)
        self._cli.authenticate(apiKey=api_key)
        self.source_folder_id = source_folder_id
        self.target_folder_id = target_folder_id

    def _get_path(self, context: InputContext | OutputContext) -> str:
        """Build logical path from asset key and partition if present."""
        if context.has_partition_key:
            return f"{'/'.join(context.asset_key.path)}/{context.partition_key}"
        return "/".join(context.asset_key.path)

    def load_input(self, context: InputContext) -> io.BytesIO:
        """
        Download input from source folder.
        Enables passing data between assets without temp files.
        """
        path = self._get_path(context)
        item_name = os.path.basename(path)

        items = list(self._cli.listItem(self.source_folder_id))
        matching_item = next(
            (item for item in items if item["name"] == item_name),
            None,
        )

        if not matching_item:
            raise FileNotFoundError(
                f"No input '{item_name}' found in source folder {self.source_folder_id}"
            )

        files = self._cli.get(f"item/{matching_item['_id']}/files")
        if not files:
            raise FileNotFoundError(f"No files in item '{item_name}'")

        fp = io.BytesIO()
        self._cli.downloadFile(files[0]["_id"], fp)
        fp.seek(0)

        context.log.info(f"Downloaded '{item_name}' from Girder")
        return fp

    def handle_output(self, context: OutputContext, obj: io.BytesIO) -> None:
        """
        Upload output to target folder.
        Handles both new uploads and updates to existing items.
        """
        if not obj:
            context.log.warning("Empty output, skipping upload")
            return

        path = self._get_path(context)
        item_name = os.path.basename(path)

        try:
            existing_item = next(
                (item for item in self._cli.listItem(self.target_folder_id)
                 if item["name"] == item_name),
                None,
            )

            if existing_item:
                item = existing_item
                context.log.info(f"Updating existing item '{item_name}'")
            else:
                item = self._cli.loadOrCreateItem(item_name, self.target_folder_id)
                context.log.info(f"Created new item '{item_name}'")

            size = obj.seek(0, os.SEEK_END)
            obj.seek(0)

            files = self._cli.get(f"item/{item['_id']}/files", parameters={"limit": 1})

            if files:
                fobj = self._cli.uploadFileContents(files[0]["_id"], obj, size)
            else:
                file_meta = self._cli.post(
                    "file",
                    parameters={
                        "parentType": "item",
                        "parentId": item["_id"],
                        "name": item_name,
                        "size": size,
                        "mimeType": "application/octet-stream",
                    },
                )
                fobj = self._cli._uploadContents(file_meta, obj, size)

            import datetime
            girder_metadata = {
                "run_id": str(context.run.run_id),
                "code_version": str(context.version),
                "asset_key": "/".join(context.asset_key.path),
                "timestamp": datetime.datetime.now().isoformat(),
            }
            self._cli.addMetadataToItem(item["_id"], girder_metadata)

            girder_url = urlparse(self._cli.urlBase)
            context.add_output_metadata({
                "girder_item_url": f"{girder_url.scheme}://{girder_url.netloc}/#item/{item['_id']}",
                "file_size_bytes": fobj["size"],
                "girder_file_id": fobj["_id"],
            })

            context.log.info(f"Uploaded '{item_name}' to Girder")

        except Exception as e:
            context.log.error(f"Failed to upload '{item_name}': {str(e)}")
            raise


class ConfigurableGirderIOManager(ConfigurableIOManagerFactory):
    """
    Factory for creating GirderIOManager with environment variable configuration.
    
    Usage in definitions.py:
        "girder_io_manager": ConfigurableGirderIOManager(
            api_url=EnvVar("GIRDER_API_URL"),
            api_key=EnvVar("GIRDER_API_KEY"),
            source_folder_id=EnvVar("GIRDER_SOURCE_FOLDER_ID"),
            target_folder_id=EnvVar("GIRDER_TARGET_FOLDER_ID"),
        )
    """

    api_url: str
    api_key: str
    source_folder_id: str
    target_folder_id: str

    def create_io_manager(self, context) -> GirderIOManager:
        """Create IO manager instance."""
        return GirderIOManager(
            api_url=self.api_url,
            api_key=self.api_key,
            source_folder_id=self.source_folder_id,
            target_folder_id=self.target_folder_id,
        )