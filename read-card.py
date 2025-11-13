from dotenv import load_dotenv
import os
import sys
import time
import requests
import json
from azure.storage.blob import BlobServiceClient
from BlobStorageUtils import BlobStorageUtils
from CommonUtils import CommonUtils

def main():

    # Clear the console
    os.system('cls' if os.name=='nt' else 'clear')

    try:

        # Get the business card
        image_file = 'biz-card-1.png'
        if len(sys.argv) > 1:
            image_file = sys.argv[1]

        # Get config settings
        load_dotenv()
        ai_svc_endpoint = os.getenv('ENDPOINT')
        ai_svc_key = os.getenv('KEY')
        analyzer = os.getenv('ANALYZER_NAME')

        # Analyze the business card
        analyze_card (image_file, analyzer, ai_svc_endpoint, ai_svc_key)

        print("\n")

    except Exception as ex:
        print(ex)



def analyze_card (image_file, analyzer, endpoint, key):

    # Use Content Understanding to analyze the image
    print (f"Analyzing {image_file}")

    # Set the API version
    CU_VERSION = "2025-05-01-preview"

    # Read the image data
    with open(image_file, "rb") as file:
        image_data = file.read()

    ## Use a POST request to submit the image data to the analyzer
    print("Submitting request...")
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Content-Type": "application/octet-stream"}
    url = f'{endpoint}/contentunderstanding/analyzers/{analyzer}:analyze?api-version={CU_VERSION}'
    response = requests.post(url, headers=headers, data=image_data)

    # Get the response and extract the ID assigned to the analysis operation
    print(response.status_code)
    response_json = response.json()
    id_value = response_json.get("id")

    # Use a GET request to check the status of the analysis operation
    print ('Getting results...')
    time.sleep(1)
    result_url = f'{endpoint}/contentunderstanding/analyzerResults/{id_value}?api-version={CU_VERSION}'
    result_response = requests.get(result_url, headers=headers)
    print(result_response.status_code)

    # Keep polling until the analysis is complete
    status = result_response.json().get("status")
    while status == "Running":
        time.sleep(1)
        result_response = requests.get(result_url, headers=headers)
        status = result_response.json().get("status")

    # Process the analysis results
    if status == "Succeeded":
        print("Analysis succeeded:\n")
        result_json = result_response.json()
        output_file = "results.json"
        with open(output_file, "w") as json_file:
            json.dump(result_json, json_file, indent=4)
            print(f"Response saved in {output_file}\n")

        # Iterate through the fields and extract the names and type-specific values
        contents = result_json["result"]["contents"]
        for content in contents:
            if "fields" in content:
                fields = content["fields"]
                for field_name, field_data in fields.items():
                    if field_data['type'] == "string":
                        print(f"{field_name}: {field_data['valueString']}")
                    elif field_data['type'] == "number":
                        print(f"{field_name}: {field_data['valueNumber']}")
                    elif field_data['type'] == "integer":
                        print(f"{field_name}: {field_data['valueInteger']}")
                    elif field_data['type'] == "date":
                        print(f"{field_name}: {field_data['valueDate']}")
                    elif field_data['type'] == "time":
                        print(f"{field_name}: {field_data['valueTime']}")
                    elif field_data['type'] == "array":
                        print(f"{field_name}: {field_data['valueArray']}")


    # 从环境变量读取参数
    container = os.getenv("BLOB_CONTAINER_NAME")
    blob_name = os.getenv("BLOB_NAME")
    local_file = os.getenv("LOCAL_JSON_FILE")

    if not container or not blob_name or not local_file:
        raise ValueError("环境变量 BLOB_CONTAINER_NAME, BLOB_NAME, LOCAL_JSON_FILE 必须都设置")

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
    "\"5/13/2025\",\"\",\"Rohan\",\"https://mybobs.quantummetric.com/#/replay/4503600076332091?segmentId=36894&teamID=86bf92e0-5967-11ee-837a-42010a800104&ts=1747108800-1747195199&sessionTs=1747192085\",\"chontaylp@gmail.com\",\"NA\",\"3199328951\",\"Affirm\",\"\"\"After click on \"\"place Order with Affirm\"\"\", LongRunningSpinner followed by Console Errors.\",\"BE‑8137\",\"\"\n"
    "\"5/13/2025\",\"\",\"Rohan\",\"https://mybobs.quantummetric.com/#/replay/4503600076317519?segmentId=36894&teamID=86bf92e0-5967-11ee-837a-42010a800104&ts=1747108800-1747195199&sessionTs=1747191045\",\"madisonswaney5@gmail.com\",\"2002060034\",\"3199366690\",\"Credit Card\",\"Password mismatch error while creating account. Order was placed successfully.\",\"\",\"\"\n"
    "\"5/13/2025\",\"\",\"Rohan\",\"https://mybobs.quantummetric.com/#/replay/4503600076316856?segmentId=36894&teamID=86bf92e0-5967-11ee-837a-42010a800104&ts=1747108800-1747195199&sessionTs=1747190998\",\"zack.visker@gmail.com\",\"2002060025\",\"3199374028\",\"Credit Card\",\"Initially there was an add to cart failure but user persisted and eventually placed an order successfully.\",\"\",\"\"\n"
    )

    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service.get_container_client(container)

    blob_utils = BlobStorageUtils(connection_string=conn_str, container_name=container)
    base_path = blob_name

    commonUtils = CommonUtils()
    result = commonUtils.write_csv_to_blob(blob_utils, base_path, prefix, header, row)

    print(result)

def write_to_blob(container_name: str, blob_name: str, data: dict) -> str:
    """
    将 data 写入指定容器的 blob。
    :param container_name: 容器名
    :param blob_name: 写入的 blob 名称（可带路径）
    :param data: 将写入 Blob 的字典数据
    :return: 返回写入结果或 URL
    """
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service.get_container_client(container_name)
    # 确保容器存在
    try:
        container_client.create_container()
    except Exception:
        pass
    blob_client = container_client.get_blob_client(blob_name)
    content = json.dumps(data)
    blob_client.upload_blob(content, overwrite=True)
    return f"Blob uploaded: {container_name}/{blob_name}"



if __name__ == "__main__":
    main()
