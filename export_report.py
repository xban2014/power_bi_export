import argparse
from concurrent.futures import ThreadPoolExecutor
import os
import threading
import time
import requests
import json

# plug in the host you need (can take from dev tools in browser).
host = "https://api.powerbi.com"

# Replace with your workspace id, if you want to export a report from a workspace,
# otherwise leave it as None to export a report from MyWorkspace
# workspaceId = None #'2660c69a-f46c-4e67-808a-b5dbad33e6a5'
workspaceId = '2660c69a-f46c-4e67-808a-b5dbad33e6a5'

# when no workspace id is specified, MyWorkspace is assumed.
groupPath = f"groups/{workspaceId}/" if workspaceId else ""

rdlReportId = '941e04d5-b50c-40f4-8141-d1ffc6c25bc9'
pbixReportId = 'f67a27f3-468e-49c9-a42c-3bb8d269733b'
reportId = pbixReportId
#reportId = rdlReportId

# plug in your access token, or pick it from the environment:
accessToken = os.getenv("PBI_ACCESS_TOKEN")
if not accessToken:
    raise ValueError("Access token not found. Please set the PBI_ACCESS_TOKEN environment variable.")

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {accessToken}"
}

def startExport():
    body = {
        "format": "PDF"  # You can change the format to PPTX or PNG
    }
    createUrl = f"{host}/v1.0/myorg/{groupPath}reports/{reportId}/ExportTo"
    try:
        response = requests.post(createUrl, headers=headers, data=json.dumps(body))
        requestId = response.headers.get("RequestId")
        trace(f"Export started at {time.strftime('%Y-%m-%d %H:%M:%S')} for {createUrl}", requestId)
        if response.status_code == 202:
            exportId = response.json().get("id")
            trace(f"Export id: {exportId} started successfully", requestId)
            return exportId
        else:
            trace(f"Failed to start export. Status code: {response.status_code}", requestId)
            trace(f"Response: {response.text}", requestId)
            return None
    except requests.exceptions.RequestException as e:
        trace(f"An error occurred: {e}", requestId)
        return None

def pollExportStatus(exportId):
    statusUrl = f"{host}/v1.0/myorg/{groupPath}reports/{reportId}/exports/{exportId}"
    status = None
    response = None
    while status != "Succeeded" and status != "Failed":
        try:
            response = requests.get(statusUrl, headers=headers)
            requestId = response.headers.get("RequestId")
            if response.status_code in [200, 202]:
                rjson = response.json()
                status = rjson.get("status")
                pctComplete = rjson.get("percentComplete")
                trace(f"Export status: {status} ({pctComplete}%)", requestId)
                time.sleep(1)
            else:
                trace(f"Failed to get export status. Status code: {response.status_code}, url: {statusUrl}", requestId)
                trace(f"Response: {response.text}", requestId)
                return "Failed", response
        except requests.exceptions.RequestException as e:
            trace(f"An error occurred: {e}", requestId)
            return "Failed", response
    
    return status, response

def downloadFile(response, exportId):
    downloadUrl = response.json().get("resourceLocation")
    response = requests.get(downloadUrl, headers=headers)
    requestId = response.headers.get("RequestId")
    if response.status_code == 200:
        # append the current timestamp to the file name:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        os.makedirs("downloads", exist_ok=True)
        filename = f"downloads/export_{reportId}_{exportId[:20]}_{timestamp}.pdf"
        with open(filename, "wb") as file:
            file.write(response.content)
        trace(f"File downloaded successfully to {filename}", requestId)
    else:
        trace(f"Failed to download file. Status code: {response.status_code}, url: {downloadUrl}", requestId)
        trace(f"Response: {response.text}")

def fullExport():
    exportId = startExport()
    if exportId:
        status, response = pollExportStatus(exportId)
        if status == "Succeeded" and response.status_code == 200:
            downloadFile(response, exportId)

def trace(msg, requestId=None):
    '''Prints a message with a timestamp, thread and the current request id'''
    now = int(time.time()) # unfortunately this is only at the second level.
    threadId = threading.current_thread().ident
    print(f"[{now}] [Thr:{threadId}] [RAID:{requestId}] {msg}")

def main():
    parser = argparse.ArgumentParser(description="Export reports concurrently.")
    parser.add_argument('--concurrency', type=int, default=1, help='Number of concurrent exports')
    parser.add_argument('--numExports', type=int, default=1, help='Total number of exports to perform')
    parser.add_argument('--skipDownload', type=bool, default=0, help='Do not download the results')
    args = parser.parse_args()

    concurrency = args.concurrency
    numExports = args.numExports

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(fullExport) for _ in range(numExports)]
        for future in futures:
            future.result()

if __name__ == "__main__":
    main()
