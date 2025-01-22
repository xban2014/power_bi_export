# Power BI Export API client

This python script can export a report.
Relevant documentation: 
- [export from some workspace](https://learn.microsoft.com/en-us/rest/api/power-bi/reports/export-to-file-in-group)
- [export from my workspace](https://learn.microsoft.com/en-us/rest/api/power-bi/reports/export-to-file)

## Prerequisites

- Python
- Requests library (`pip install requests`)

## Usage

- check the `host` variable in the script and set to the tenant you want to access.
- configure the `workspaceId` and `reportId` with the GUIDs you want.
- set an environment variable `PBI_ACCESS_TOKEN` with the powerBiAccessToken value
- run `python export_report.py`
    - without arguments it runs once
    - run multiple times: add parameter `--numExports <number of exports>`
    - run from multiple threads: add parameters `--concurrency <thread count> --numExports <number of exports>`
- all files will be stored in the `downloads` folder relative to where you run the script.
