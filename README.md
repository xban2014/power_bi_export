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

`python export_report.py [--cluster <cluster>] --workspaceId <workspaceId> --reportId <reportId> [--numExports <number of exports>] [--concurrency <thread count>] [--exportRequestFile <file>] [--discardDownload] [--httpDebug]`

where:
- `cluster` is one of `localhost` (or `onebox` or `devbox`), `edog`, `daily`, `dxt`, `msit` or `prod`. If not specified, defaults to `prod`
- `workspaceId` is the workspace/group id for your report
- `reportId` is the report id
- `numExports` can be set to a number higher than 1 if you want the exports to run in a loop
- `concurrency` can be set to a number higher than 1 if you want them to run in parallel for load testing
- `exportRequestFile` can be the path to a JSON file that holds the request parameters.
- `discardDownload` stream down the results, but don't store them in any file, just discard them as they come(for load testing)
- `httpDebug` turns on DEBUG level logging for the python urllib3 library.

Authentication will be done interactively. You can bypass the interactive auth if you have a particular
token you want to use by setting the `PBI_ACCESS_TOKEN` environment variable to a valid JWT
value. 

All result files will be stored in the `downloads` folder relative to where you run the script.

Example: run export with the parameters stored in a .json file:

```
python .\export_report.py --cluster daily  --workspaceId bf744e93-ec4b-43bc-b55f-ba881250e2d1 --reportId da19a7ee-15e9-42ab-95c8-d0bb5e7a3b31 --exportRequestFile .\medication_adherence_rdl_request.json
```
