"""
This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <https://unlicense.org/>
"""

"""
Power BI Report Export Client

This script provides functionality to export Power BI reports concurrently.
It handles authentication, export request submission, status polling, and file downloads.
"""

# Standard library imports
import argparse
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, Optional, Tuple

# Third-party imports
import urllib3
from azure.identity import InteractiveBrowserCredential

# start of the program
epoch = int(time.time())


class ExportContext:
    """
    Container for export operation context and configuration.

    This class holds all the necessary information and resources needed for
    exporting Power BI reports, including authentication tokens, request parameters,
    and HTTP client instance.
    """

    def __init__(
        self,
        exportNumber: int,
        http: urllib3.PoolManager,
        accessToken: str,
        workspaceId: Optional[str],
        reportId: str,
        host: str,
        headers: Dict[str, str],
        exportRequest: Dict[str, Any],
        discardDownload: bool,
    ):
        """
        Initialize the export context with the provided parameters.

        Args:
            exportNumber (int): The number of the export operation.
            http (urllib3.PoolManager): The HTTP client instance.
            accessToken (str): The authentication token for Power BI API access.
            workspaceId (str): The ID of the Power BI workspace.
            reportId (str): The ID of the report to be exported.
            host (str): The Power BI API host URL.
            headers (dict): HTTP headers to be included in API requests.
            exportRequest (dict): The export configuration parameters.
            discardDownload (bool): Flag indicating whether to discard the downloaded export result.
        """
        self.exportNumber = exportNumber
        self.http = http
        self.accessToken = accessToken
        self.workspaceId = workspaceId
        self.reportId = reportId
        self.host = host
        self.headers = headers
        self.exportRequest = exportRequest
        self.groupPath = f"groups/{workspaceId}/" if workspaceId else ""
        self.discardDownload = discardDownload
        self.phase = "init"
        self.requestId = None  # Will be set from response headers

    def trace(self, msg: str):
        """
        Print a message with the seconds after the program start, the timestamp, thread ID, and request ID.

        Args:
            msg (str): The message to print.
        """
        t = time.time()
        now = int(t)  # Unfortunately this is only at the second level.
        delta = now - epoch
        strTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
        threadId = threading.current_thread().ident
        print(
            f"[{delta}s] [{strTime}] [Thr:{threadId}] [{self.exportNumber}:{self.phase}] [RAID:{self.requestId}] {msg}"
        )

    def setRequestId(self, response: urllib3.HTTPResponse) -> None:
        """
        Extract and set the request ID from an HTTP response object.

        Args:
            response: The HTTP response object containing headers.
        """
        self.requestId = response.headers.get("RequestId")

    def requestWithRetry(
        self, httpMethod: str, url: str, maxInterval: int = 16, baseInterval: int = 1, **request_kwargs: Any
    ) -> urllib3.HTTPResponse:
        """Issue an HTTP request with automatic 429/back-off handling."""

        delay = baseInterval

        while True:
            response = self.http.request(httpMethod, url, **request_kwargs)
            self.setRequestId(response)

            if response.status != 429:
                return response  # caller must close/release via ResponseContextManager

            self.trace(f"Rate limited (status 429) on {url}")

            retry_after = response.headers.get("Retry-After")

            # release connection before looping again.
            response.release_conn()

            if retry_after:
                try:
                    delay = int(retry_after)
                    self.trace(f"Sleeping {delay} seconds per Retry-After header")
                except ValueError:
                    self.trace(f"Ignoring invalid Retry-After value '{retry_after}'")

            if delay:
                self.trace(f"Sleeping {delay} seconds before retrying…")
                time.sleep(delay)

            if retry_after is None and delay < maxInterval:
                delay = min(delay * 2, maxInterval)


class ResponseContextManager:
    """Context manager for urllib3 response objects."""

    def __init__(self, response: urllib3.HTTPResponse):
        self.response = response

    def __enter__(self) -> urllib3.HTTPResponse:
        return self.response

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[Any]) -> None:
        if hasattr(self.response, "release_conn"):
            self.response.release_conn()


def startExport(context: ExportContext) -> Optional[str]:
    """
    Start a new export job for a Power BI report.

    Args:
        context (ExportContext): The context containing export configuration.

    Returns:
        str or None: The export ID if successful, None otherwise.
    """
    context.phase = "start"
    createUrl = f"{context.host}/v1.0/myorg/{context.groupPath}reports/{context.reportId}/ExportTo"
    try:
        context.trace(f"Export started at {time.strftime('%Y-%m-%d %H:%M:%S')} for {createUrl}")

        with ResponseContextManager(
            context.requestWithRetry(
                httpMethod="POST", url=createUrl, headers=context.headers, body=json.dumps(context.exportRequest)
            )
        ) as response:
            if response.status == 202:
                exportId = response.json().get("id")
                context.trace(f"Export id: {exportId} started successfully")
                return exportId
            else:
                context.trace(f"Failed to start export. Status code: {response.status}")
                context.trace(f"Response: {response.data.decode('utf-8')}")
                return None

    except Exception as e:
        context.trace(f"An error occurred: {e}")
        return None


def pollExportStatus(context: ExportContext, exportId: str) -> Tuple[str, urllib3.HTTPResponse]:
    """
    Poll the status of an export job until it completes or fails.

    Args:
        context (ExportContext): The context containing export configuration.
        exportId (str): The ID of the export job to poll.

    Returns:
        tuple: (status, response) where status is "Succeeded" or "Failed" and response is the HTTP response.
    """
    context.phase = "poll"
    statusUrl = f"{context.host}/v1.0/myorg/{context.groupPath}reports/{context.reportId}/exports/{exportId}"
    status = None
    response = None
    pollIntervalSeconds = 1

    while status != "Succeeded" and status != "Failed":
        try:
            context.trace(f"Polling export status for {statusUrl}")

            with ResponseContextManager(
                context.requestWithRetry(httpMethod="GET", url=statusUrl, headers=context.headers)
            ) as response:

                if response.status in [200, 202]:
                    rjson = response.json()
                    status = rjson.get("status")
                    pctComplete = rjson.get("percentComplete")
                    context.trace(f"Export status: {status} ({pctComplete}%)")
                else:
                    context.trace(f"Failed to get export status. Status code: {response.status}, url: {statusUrl}")
                    context.trace(f"Response: {response.data.decode('utf-8')}")
                    return "Failed", response

                if pctComplete is None or pctComplete < 100 or status == "Running":
                    context.trace(f"Sleeping {pollIntervalSeconds} seconds...")
                    time.sleep(pollIntervalSeconds)

        except Exception as e:
            context.trace(f"An error occurred: {e}, {response.data.decode('utf-8') if response else ''}")
            return "Failed", response

    if status == "Failed":
        context.trace(f"Export failed: {response.data.decode('utf-8')}")

    return status, response


def downloadFile(context: ExportContext, response: urllib3.HTTPResponse, exportId: str) -> None:
    """
    Download the exported file if the export was successful.

    Args:
        context (ExportContext): The context containing export configuration.
        response (HTTPResponse): The HTTP response from the status polling.
        exportId (str): The ID of the export job.
    """
    context.phase = "download"
    downloadUrl = response.json().get("resourceLocation")
    context.setRequestId(response)
    context.trace(f"Download URL: {downloadUrl}")

    try:
        start_time = time.time()

        with ResponseContextManager(
            context.requestWithRetry(httpMethod="GET", url=downloadUrl, headers=context.headers, preload_content=False)
        ) as response:
            if response.status != 200:
                context.trace(f"Failed to download file. Status code: {response.status}, url: {downloadUrl}")
                context.trace(f"Response: {response.data.decode('utf-8')}")
                return

            # consume the response stream, but do not write to disk:
            if context.discardDownload:
                context.trace(f"Downloading file to /dev/null...")
                for chunk in response.stream(8192):
                    pass

                end_time = time.time()
                duration = end_time - start_time
                context.trace(f"Downloaded file to /dev/null in {duration:.2f} seconds")
                return

            # write the response stream to a file:
            os.makedirs("downloads", exist_ok=True)
            fileExtension = context.exportRequest.get("format", "pdf").lower()
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"downloads/export_{context.reportId}_{exportId[:20]}_{timestamp}.{fileExtension}"
            with open(filename, "wb") as file:
                for chunk in response.stream(8192):
                    file.write(chunk)

            end_time = time.time()
            duration = end_time - start_time
            file_size = os.path.getsize(filename)
            context.trace(f"Downloaded file to {filename} in {duration:.2f} seconds, size: {file_size} bytes")

    except Exception as e:
        context.trace(f"An error occurred: {e}")


def fullExport(context: ExportContext) -> None:
    """
    Execute the full export workflow: start export, poll status, and download the file.

    Args:
        context (ExportContext): The context containing export configuration.
    """
    exportId = startExport(context)
    if exportId:
        status, response = pollExportStatus(context, exportId)
        if status == "Succeeded" and response.status == 200:
            # for i in range(10):
            downloadFile(context, response, exportId)


def main() -> None:
    """
    Main function to parse arguments and execute the export process.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Export reports concurrently. "
            "Must set env var PBI_ACCESS_TOKEN for ppe environments. "
            "Files will be saved in the downloads folder relative to the current directory."
        )
    )
    parser.add_argument(
        "--cluster",
        type=str,
        choices=["localhost", "devbox", "onebox", "edog", "daily", "dxt", "msit", "prod"],
        default="prod",
        help="Cluster to use: (localhost,onebox,devbox), edog, daily, dxt, msit, prod (default: prod)",
    )
    parser.add_argument("--workspaceId", type=str, help="Workspace ID to export from")
    parser.add_argument("--reportId", type=str, help="Report object ID to export")
    parser.add_argument("--concurrency", type=int, default=1, help="Max number of concurrent exports")
    parser.add_argument("--numExports", type=int, default=1, help="Total number of exports to perform")
    parser.add_argument("--discardDownload", action="store_true", help="Stream in the results but throw away the data")
    parser.add_argument("--exportRequestFile", type=str, help="Path to the export request JSON file")
    parser.add_argument("--httpDebug", action="store_true", help="Enable detailed HTTP request/response logging")
    args = parser.parse_args()

    if args.httpDebug:
        logging.basicConfig(level=logging.DEBUG)
        urllib3_logger = logging.getLogger("urllib3.connectionpool")
        urllib3_logger.setLevel(logging.DEBUG)

    workspaceId = args.workspaceId
    reportId = args.reportId
    concurrency = args.concurrency
    numExports = args.numExports
    discardDownload = args.discardDownload

    if not reportId:
        raise ValueError("Report ID is required.")

    # PBI_ACCESS_TOKEN environment variable if defined as an environment variable will override the interactive login
    accessToken = os.getenv("PBI_ACCESS_TOKEN")
    if not accessToken:
        app = InteractiveBrowserCredential()
        scope = "https://analysis.windows.net/powerbi/api/user_impersonation"
        accessToken = app.get_token(scope)
        if not accessToken:
            raise ValueError(
                "Access token could not be obtained. Please set the PBI_ACCESS_TOKEN environment variable."
            )
        accessToken = accessToken.token

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {accessToken}"}

    # Read export request from file if provided, otherwise use default
    if args.exportRequestFile:
        with open(args.exportRequestFile, "r") as file:
            exportRequest = json.load(file)
    else:
        exportRequest = {
            "format": "PDF",
            # "powerBIReportConfiguration": {
            #     "settings": {},
            #     "powerBIReportConfiguration": {
            #         "reportLevelFilters": [
            #             {
            #                 "filter": "Table1/CategoryName eq 'Condiments'"
            #             }
            #         ]
            #     }
            # }
        }

    # You can get the host from the service in web tools
    if args.cluster == "localhost" or args.cluster == "onebox" or args.cluster == "devbox":
        host = "https://onebox-redirect.analysis.windows-int.net"
    elif args.cluster == "edog":
        host = "https://biazure-int-edog-redirect.analysis-df.windows.net"
    elif args.cluster == "daily":
        host = "https://wabi-daily-us-east2-redirect.analysis.windows.net"
    elif args.cluster == "dxt":
        host = "https://wabi-staging-us-east-redirect.analysis.windows.net"
    elif args.cluster == "msit":
        host = "https://df-msit-scus-redirect.analysis.windows.net"
    elif args.cluster == "prod":
        host = "https://api.powerbi.com"
    else:
        raise ValueError("Invalid cluster. Choose from one of: localhost, onebox, devbox, edog, daily, dxt, msit, or prod.")

    # one pool manager for all threads
    with urllib3.PoolManager(retries=False) as http:

        with ThreadPoolExecutor(max_workers=concurrency) as executor:

            # pump all requests into the thread pool queue
            futures = []
            for i in range(numExports):
                # each request has its own context:
                future = executor.submit(
                    fullExport,
                    ExportContext(
                        i + 1, http, accessToken, workspaceId, reportId, host, headers, exportRequest, discardDownload
                    ),
                )

                futures.append(future)

            # wait for all futures to complete
            for future in futures:
                future.result()


if __name__ == "__main__":
    main()
