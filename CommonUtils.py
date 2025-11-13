import datetime
import io
import logging

logger = logging.getLogger(__name__)

class CommonUtils:
    """
    Utility class for common operations, including writing/appending CSV files to blob storage.
    """

    @staticmethod
    def write_csv_to_blob(blob_utils, base_path: str, prefix: str, header: str, row: str) -> None:
        """
        Write or append a CSV file to blob storage.

        :param blob_utils: Instance of a blob‑storage utility class providing exists(path), write(path, stream), append(path, stream) methods.
        :param configuration_service: Instance providing get_configuration().get_string(key, default) method for fetching config values.
        :param prefix: The prefix for the file name (will be combined with current date).
        :param header: The header line for the CSV file.
        :param row: The row to write (or append) into the CSV.
        :raises: Exception if the operation fails.
        """
        # format current date as MM‑dd‑yyyy
        date_str = datetime.datetime.now().strftime("%m-%d-%Y")
        # build full file path
        file_path = f"{base_path}{prefix}_{date_str}.csv"
        # build content for new file: header + row + newline
        content = f"{header}\n{row}\n"

        logger.info(f"writing affirm webhook filePath : {file_path}\ncontent : {content}")

        # convert content into byte‑stream
        content_bytes = content.encode("utf-8")
        content_stream = io.BytesIO(content_bytes)

        # if file doesn’t exist → write new, else append
        if not blob_utils.exists(file_path):
            blob_utils.write(file_path, content_stream)
        else:
            append_bytes = f"{row}\n".encode("utf-8")
            append_stream = io.BytesIO(append_bytes)
            blob_utils.append(file_path, append_stream)
