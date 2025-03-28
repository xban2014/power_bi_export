# Power BI Export API client

This python script can export a report.
Relevant documentation: 
- [export from some workspace](https://learn.microsoft.com/en-us/rest/api/power-bi/reports/export-to-file-in-group)
- [export from my workspace](https://learn.microsoft.com/en-us/rest/api/power-bi/reports/export-to-file)

## Prerequisites

- Python
- Requests library (`pip install requests`)
- Azure identity (`pip install azure.identity`)

## Usage

`python export_report.py [--host <host>] --workspaceId <workspaceId> --reportId <reportId> [--numExports <number of exports>] [--concurrency <thread count]`

where:
- host is one of `daily`, `dxt` or `msit`. If not specified, defaults to `daily`
- workspaceId is the workspace/group id for your report
- reportId is the report id
- numExports can be set to a number higher than 1 if you want the exports to run in a loop
- concurrency can be set to a number higher than 1 if you want them to run in parallel for load testing

Authentication will be done interactively. You can bypass that if you have a particular
test token you want to use by setting the PBI_ACCESS_TOKEN environment variable to a valid
value. 

All result files will be stored in the `downloads` folder relative to where you run the script.
