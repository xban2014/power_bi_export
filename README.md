# Power BI Export API client

This python script can export a report.
Relevant documentation: 
- [export from some workspace](https://learn.microsoft.com/en-us/rest/api/power-bi/reports/export-to-file-in-group)
- [export from my workspace](https://learn.microsoft.com/en-us/rest/api/power-bi/reports/export-to-file)

## Prerequisites

- Python
- Urllib3 library (`pip install urllib3`)
- Azure identity (`pip install azure.identity`)

## Usage

`python export_report.py [--host <host>] --workspaceId <workspaceId> --reportId <reportId> [--numExports <number of exports>] [--concurrency <thread count] [--exportRequestFile <file>] [--skipDownload]`

where:
- `host` is one of `daily`, `dxt` or `msit`. If not specified, defaults to `daily`
- `workspaceId` is the workspace/group id for your report
- `reportId` is the report id
- `numExports` can be set to a number higher than 1 if you want the exports to run in a loop
- `concurrency` can be set to a number higher than 1 if you want them to run in parallel for load testing
- `exportRequestFile` can be the path to a JSON file that holds the request parameters.
- `skipDownload` skips the download phase (for load testing)

Authentication will be done interactively. You can bypass the interactive auth if you have a particular
token you want to use by setting the `PBI_ACCESS_TOKEN` environment variable to a valid JWT
value. 

All result files will be stored in the `downloads` folder relative to where you run the script.
