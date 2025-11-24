from dotenv import load_dotenv
import os
from azure.storage.blob import BlobServiceClient, BlobClient
from BlobStorageUtils import BlobStorageUtils
from CommonUtils import CommonUtils
from azure.identity import DefaultAzureCredential


import json
import logging
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from dataclasses import dataclass

import requests


def main():

    load_dotenv()
    container = os.getenv("BLOB_CONTAINER_NAME")
    blob_name_input = os.getenv("BLOB_NAME_INPUT")
    blob_name_output = os.getenv("BLOB_NAME_OUTPUT")
    local_file = os.getenv("LOCAL_JSON_FILE")
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service.get_container_client(container)

    blob_utils = BlobStorageUtils(connection_string=conn_str, container_name=container)
    base_path = blob_name_input

    commonUtils = CommonUtils()

    # 下载到内存 bytes
    prefix = '4503600076332091-2025-11-04-182104.mp4'
    data_bytes = commonUtils.read_video_to_bytes(blob_utils, base_path, prefix)
#     print("Blob content as bytes:", data_bytes)

    # Get config settings
    load_dotenv()
    ai_svc_endpoint = os.getenv('ENDPOINT')
    ai_svc_key = os.getenv('KEY')
    key = os.getenv('KEY')
    analyzer = os.getenv('ANALYZER_NAME')
#     schema_json = os.getenv('SCHEMA_JSON')
    version = os.getenv('API_VERSION')
    file_path = os.getenv('FILE_LOCATION')

    settings = Settings(
        endpoint=ai_svc_endpoint,
        api_version=version,
        # Either subscription_key or aad_token must be provided. Subscription Key is more prioritized.
        subscription_key=key,
        aad_token="AZURE_CONTENT_UNDERSTANDING_AAD_TOKEN",
        # Insert the analyzer name.
        analyzer_id=analyzer,
        # Insert the supported file types of the analyzer.
        file_location=file_path,
    )
    client = AzureContentUnderstandingClient(
        settings.endpoint,
        settings.api_version,
        subscription_key=settings.subscription_key,
        token_provider=settings.token_provider,
    )
    response = client.begin_analyze(settings.analyzer_id, settings.file_location, data_bytes)
    result = client.poll_result(
        response,
        timeout_seconds=60 * 60,
        polling_interval_seconds=1,
    )
    json.dump(result, sys.stdout, indent=2)



    # 将 JSON 字符串解析为 Python 对象（字典）
    json_str = json.dumps(result)
    data = json.loads(json_str)

    # 导航到 contents -> 第一个元素 -> fields
    contents = data.get("result", {}).get("contents", [])
    if contents:
        first_content = contents[0]
        fields = first_content.get("fields", {})

        ShoppingCart = fields.get("ShoppingCart", {}).get("valueString")
        PlaceOrderwith = fields.get("PlaceOrderwith", {}).get("valueString")

        cart_number = ShoppingCart.strip()
        # 找到 “Place Order with” 之后的子串
        if "Place Order with" in PlaceOrderwith:
            payment_method = PlaceOrderwith.split("Place Order with", 1)[1].strip()
        else:
            payment_method = ""

        print("cart_number:", cart_number)
        print("payment_method:", payment_method)

    else:
        print("No contents found.")


############################################################################################################


    if not container or not blob_name_input or not local_file:
        raise ValueError("环境变量 BLOB_CONTAINER_NAME, BLOB_NAME_INPUT, LOCAL_JSON_FILE 必须都设置")

#     # 读取本地 json 文件
#     with open(local_file, "r", encoding="utf-8") as f:
#         file_data = json.load(f)
#
#     # 调用写入函数
#     result = write_to_blob(container_name=container,
#                            blob_name=blob_name,
#                            data=file_data)


    prefix = "QM Replay Analysis - Checkout Steps(Step5to6)"
    header = "Date,Replay ID,Assignee,Quantum Metric Session,Customer UID,Order number,Cart number,Payment method,What is the issue?,JIRA ID,Comments/Observation"
    row = (
        f"\"5/13/2025\",\"\",\"Rohan\",\"https://mybobs.quantummetric.com/#/replay/4503600076332091?segmentId=36894&teamID=86bf92e0-5967-11ee-837a-42010a800104&ts=1747108800-1747195199&sessionTs=1747192085\",\"chontaylp@gmail.com\",\"NA\",\"{cart_number}\",\"{payment_method}\",\"\"\"After click on \"\"place Order with Affirm\"\"\", LongRunningSpinner followed by Console Errors.\",\"BE-8137\",\"\"\n"
    )


    result = commonUtils.write_csv_to_blob(blob_utils, blob_name_output, prefix, header, row)

    print(result)


############################################################################################################


@dataclass(frozen=True, kw_only=True)
class Settings:
    endpoint: str
    api_version: str
    subscription_key: str | None = None
    aad_token: str | None = None
    analyzer_id: str
    file_location: str

    def __post_init__(self):
        key_not_provided = (
            not self.subscription_key
            or self.subscription_key == "AZURE_CONTENT_UNDERSTANDING_SUBSCRIPTION_KEY"
        )
        token_not_provided = (
            not self.aad_token
            or self.aad_token == "AZURE_CONTENT_UNDERSTANDING_AAD_TOKEN"
        )
        if key_not_provided and token_not_provided:
            raise ValueError(
                "Either 'subscription_key' or 'aad_token' must be provided"
            )

    @property
    def token_provider(self) -> Callable[[], str] | None:
        aad_token = self.aad_token
        if aad_token is None:
            return None

        return lambda: aad_token


class AzureContentUnderstandingClient:
    def __init__(
        self,
        endpoint: str,
        api_version: str,
        subscription_key: str | None = None,
        token_provider: Callable[[], str] | None = None,
        x_ms_useragent: str = "cu-sample-code",
    ) -> None:
        if not subscription_key and token_provider is None:
            raise ValueError(
                "Either subscription key or token provider must be provided"
            )
        if not api_version:
            raise ValueError("API version must be provided")
        if not endpoint:
            raise ValueError("Endpoint must be provided")

        self._endpoint: str = endpoint.rstrip("/")
        self._api_version: str = api_version
        self._logger: logging.Logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.INFO)
        self._headers: dict[str, str] = self._get_headers(
            subscription_key, token_provider and token_provider(), x_ms_useragent
        )

    def begin_analyze(self, analyzer_id: str, file_location: str, file_data: bytes):
        """
        Begins the analysis of a file or URL using the specified analyzer.

        Args:
            analyzer_id (str): The ID of the analyzer to use.
            file_location (str): The path to the file or the URL to analyze.

        Returns:
            Response: The response from the analysis request.

        Raises:
            ValueError: If the file location is not a valid path or URL.
            HTTPError: If the HTTP request returned an unsuccessful status code.
        """

        data = file_data
        headers = {"Content-Type": "application/octet-stream"}

        # if Path(file_location).exists():
        #     with open(file_location, "rb") as file:
        #         data = file.read()
        #     headers = {"Content-Type": "application/octet-stream"}
        # elif "https://" in file_location or "http://" in file_location:
        #     data = {"url": file_location}
        #     headers = {"Content-Type": "application/json"}
        # else:
        #     raise ValueError("File location must be a valid path or URL.")

        headers.update(self._headers)
        if isinstance(data, dict):
            response = requests.post(
                url=self._get_analyze_url(
                    self._endpoint, self._api_version, analyzer_id
                ),
                headers=headers,
                json=data,
            )
        else:
            response = requests.post(
                url=self._get_analyze_url(
                    self._endpoint, self._api_version, analyzer_id
                ),
                headers=headers,
                data=data,
            )

        response.raise_for_status()
        self._logger.info(
            f"Analyzing file {file_location} with analyzer: {analyzer_id}"
        )
        return response

    def poll_result(
        self,
        response: requests.Response,
        timeout_seconds: int = 120,
        polling_interval_seconds: int = 2,
    ) -> dict[str, Any]:  # pyright: ignore[reportExplicitAny]
        """
        Polls the result of an asynchronous operation until it completes or times out.

        Args:
            response (Response): The initial response object containing the operation location.
            timeout_seconds (int, optional): The maximum number of seconds to wait for the operation to complete. Defaults to 120.
            polling_interval_seconds (int, optional): The number of seconds to wait between polling attempts. Defaults to 2.

        Raises:
            ValueError: If the operation location is not found in the response headers.
            TimeoutError: If the operation does not complete within the specified timeout.
            RuntimeError: If the operation fails.

        Returns:
            dict: The JSON response of the completed operation if it succeeds.
        """
        operation_location = response.headers.get("operation-location", "")
        if not operation_location:
            raise ValueError("Operation location not found in response headers.")

        headers = {"Content-Type": "application/json"}
        headers.update(self._headers)

        start_time = time.time()
        while True:
            elapsed_time = time.time() - start_time
            self._logger.info(
                "Waiting for service response", extra={"elapsed": elapsed_time}
            )
            if elapsed_time > timeout_seconds:
                raise TimeoutError(
                    f"Operation timed out after {timeout_seconds:.2f} seconds."
                )

            response = requests.get(operation_location, headers=self._headers)
            response.raise_for_status()
            result = cast(dict[str, str], response.json())
            status = result.get("status", "").lower()
            if status == "succeeded":
                self._logger.info(
                    f"Request result is ready after {elapsed_time:.2f} seconds."
                )
                return response.json()  # pyright: ignore[reportAny]
            elif status == "failed":
                self._logger.error(f"Request failed. Reason: {response.json()}")
                raise RuntimeError("Request failed.")
            else:
                self._logger.info(
                    f"Request {operation_location.split('/')[-1].split('?')[0]} in progress ..."
                )
            time.sleep(polling_interval_seconds)

    def _get_analyze_url(self, endpoint: str, api_version: str, analyzer_id: str):
        return f"{endpoint}/contentunderstanding/analyzers/{analyzer_id}:analyze?api-version={api_version}&stringEncoding=utf16"

    def _get_headers(
        self, subscription_key: str | None, api_token: str | None, x_ms_useragent: str
    ) -> dict[str, str]:
        """Returns the headers for the HTTP requests.
        Args:
            subscription_key (str): The subscription key for the service.
            api_token (str): The API token for the service.
            enable_face_identification (bool): A flag to enable face identification.
        Returns:
            dict: A dictionary containing the headers for the HTTP requests.
        """
        headers = (
            {"Ocp-Apim-Subscription-Key": subscription_key}
            if subscription_key
            else {"Authorization": f"Bearer {api_token}"}
        )
        headers["x-ms-useragent"] = x_ms_useragent
        return headers


if __name__ == "__main__":
    main()
