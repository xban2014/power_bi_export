import argparse
from concurrent.futures import ThreadPoolExecutor
import os
import threading
import time
import requests
import json
from azure.identity import InteractiveBrowserCredential

class ExportContext:
    def __init__(self, accessToken, workspaceId, reportId, host, headers, exportRequest):
        self.accessToken = accessToken
        self.workspaceId = workspaceId
        self.reportId = reportId
        self.host = host
        self.headers = headers
        self.exportRequest = exportRequest
        self.groupPath = f"groups/{workspaceId}/" if workspaceId else ""

def startExport(context):
    createUrl = f"{context.host}/v1.0/myorg/{context.groupPath}reports/{context.reportId}/ExportTo"
    try:
        response = requests.post(createUrl, headers=context.headers, data=json.dumps(context.exportRequest))
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

def pollExportStatus(context, exportId):
    statusUrl = f"{context.host}/v1.0/myorg/{context.groupPath}reports/{context.reportId}/exports/{exportId}"
    status = None
    response = None
    while status != "Succeeded" and status != "Failed":
        try:
            response = requests.get(statusUrl, headers=context.headers)
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

def downloadFile(context, response, exportId):
    downloadUrl = response.json().get("resourceLocation")
    response = requests.get(downloadUrl, headers=context.headers)
    requestId = response.headers.get("RequestId")
    if response.status_code == 200:
        # append the current timestamp to the file name:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        os.makedirs("downloads", exist_ok=True)
        filename = f"downloads/export_{context.reportId}_{exportId[:20]}_{timestamp}.pdf"
        with open(filename, "wb") as file:
            file.write(response.content)
        trace(f"File downloaded successfully to {filename}", requestId)
    else:
        trace(f"Failed to download file. Status code: {response.status_code}, url: {downloadUrl}", requestId)
        trace(f"Response: {response.text}")

def fullExport(context):
    exportId = startExport(context)
    if exportId:
        status, response = pollExportStatus(context, exportId)
        if status == "Succeeded" and response.status_code == 200:
            downloadFile(context, response, exportId)

def trace(msg, requestId=None):
    '''Prints a message with a timestamp, thread and the current request id'''
    now = int(time.time()) # unfortunately this is only at the second level.
    threadId = threading.current_thread().ident
    print(f"[{now}] [Thr:{threadId}] [RAID:{requestId}] {msg}")

def main():

    # PBI_ACCESS_TOKEN environment variable if defined as an environment variable will override the interactive login
    accessToken = os.getenv("PBI_ACCESS_TOKEN")
    if not accessToken:
        app = InteractiveBrowserCredential()
        scope = 'https://analysis.windows.net/powerbi/api/user_impersonation'
        accessToken = app.get_token(scope)
        if not accessToken:
            raise ValueError("Access token could not be obtained. Please set the PBI_ACCESS_TOKEN environment variable.")
        accessToken = accessToken.token

    parser = argparse.ArgumentParser(description="Export reports concurrently.")
    parser.add_argument('--cluster', type=str,  choices=['daily', 'dxt', 'msit'], default='daily', help='Cluster to use: daily, dxt or msit')
    parser.add_argument('--workspaceId', type=str, help='Workspace ID to export from')
    parser.add_argument('--reportId', type=str, help='Report ID to export')
    parser.add_argument('--concurrency', type=int, default=1, help='Number of concurrent exports')
    parser.add_argument('--numExports', type=int, default=1, help='Total number of exports to perform')
    parser.add_argument('--skipDownload', type=bool, default=0, help='Do not download the results')
    args = parser.parse_args()

    workspaceId = args.workspaceId
    reportId = args.reportId
    concurrency = args.concurrency
    numExports = args.numExports
    if not reportId:
        raise ValueError("Report ID is required.")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {accessToken}"
    }

    # customize the export request here
    exportRequest = {
        "format": "PDF"  
    }

    # you can get the host from the service in web tools
    if args.cluster == 'daily':
        host = "https://wabi-daily-us-east2-redirect.analysis.windows.net"
    elif args.cluster == 'dxt':
        host = "https://wabi-staging-us-east-redirect.analysis.windows.net"
    elif args.cluster == 'msit':
        host = "https://df-msit-scus-redirect.analysis.windows.net"
    else:
        raise ValueError("Invalid cluster specified.")

    
    context = ExportContext(accessToken, workspaceId, reportId, host, headers, exportRequest)

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(fullExport, context) for _ in range(numExports)]
        for future in futures:
            future.result()

if __name__ == "__main__":
    main()
