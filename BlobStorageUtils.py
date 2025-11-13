import tempfile
import os
import logging
from typing import BinaryIO, List

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# If you use DefaultAzureCredential or other credential, import accordingly:
# from azure.identity import DefaultAzureCredential

logger = logging.getLogger(__name__)

class BlobStorageUtils:
    """
    Utility class for Azure Blob Storage operations.
    """

    def __init__(self, connection_string: str, container_name: str):
        """
        Initialize the utility.

        :param connection_string: Azure Blob Storage connection string.
        :param container_name: Name of the blob container.
        """
        if not connection_string:
            raise ValueError("Connection string is required")
        if not container_name:
            raise ValueError("Container name is required")

        self.container_name = container_name
        self.blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(connection_string)
        self.container_client: ContainerClient = self.blob_service_client.get_container_client(container_name)

    def _get_blob_client(self, blob_path: str) -> BlobClient:
        """
        Get the BlobClient for a specific blob path.
        """
        # Remove leading slash if present
        if blob_path.startswith("/"):
            blob_path = blob_path[1:]
        return self.container_client.get_blob_client(blob_path)

    def exists(self, path: str) -> bool:
        """
        Check whether the blob at `path` exists.
        """
        blob_client = self._get_blob_client(path)
        return blob_client.exists()

    def read(self, path: str) -> BinaryIO:
        """
        Read the blob at `path` and return a stream (BytesIO) of its content.
        """
        blob_client = self._get_blob_client(path)
        downloader = blob_client.download_blob()
        return downloader.readall()

    def write(self, path: str, content_stream: BinaryIO) -> None:
        """
        Write the content from content_stream to the blob at `path`.
        This will overwrite existing content.
        """
        blob_client = self._get_blob_client(path)
        blob_client.upload_blob(content_stream, overwrite=True)

    def upload_file(self, local_file_path: str, path: str) -> None:
        """
        Upload a local file to blob at `path`.
        """
        blob_client = self._get_blob_client(path)
        with open(local_file_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)

    def remove(self, path: str) -> None:
        """
        Delete the blob at `path`.
        """
        blob_client = self._get_blob_client(path)
        blob_client.delete_blob()

    def list_names(self, prefix: str = "") -> List[str]:
        """
        List blob names under the given prefix (path).
        """
        blobs = self.container_client.list_blobs(name_starts_with=prefix)
        return [b.name for b in blobs]

    def append(self, path: str, content_stream: BinaryIO) -> None:
        """
        Append data from content_stream to the blob at given path.
        If the blob does not exist, it is created with the new data.
        If it exists, existing content is read, combined with new content,
        and the blob is overwritten with the combined content.
        """
        try:
            # Check if exists and read existing bytes
            if self.exists(path):
                existing_bytes = self.read(path)
            else:
                existing_bytes = b""

            # Read new bytes
            new_bytes = content_stream.read()

            # Create temporary file with combined content
            with tempfile.NamedTemporaryFile(delete=False, prefix="append-", suffix=".tmp") as tf:
                tf.write(existing_bytes)
                tf.write(new_bytes)
                temp_path = tf.name

            # Upload the temporary file
            self.upload_file(temp_path, path)

        except Exception as e:
            logger.error(f"Failed to append to blob: {path}", exc_info=e)
            raise
        finally:
            # Attempt to delete the temporary file
            try:
                if 'temp_path' in locals() and os.path.exists(temp_path):
                    os.remove(temp_path)
            except OSError as oe:
                logger.warning(f"Could not delete temporary file {temp_path}: {oe}")
