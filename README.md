# Project

> This repo has been populated by an initial template to help get you started. Please
> make sure to update the content to build a great experience for community-building.

As the maintainer of this project, please make a few updates:

- Improving this README.MD file to provide a great experience
- Updating SUPPORT.MD with content about this project's support experience
- Understanding the security reporting process in SECURITY.MD
- Remove this section from the README

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit [Contributor License Agreements](https://cla.opensource.microsoft.com).

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.




# UpdatePurviewMetadata Azure Function

This Azure Function updates asset metadata in Azure Purview using information from a CSV file stored in Azure Blob Storage.

## Overview
- **Trigger:** Timer (runs once per day)
- **Purpose:** Reads a CSV file (`assetinfo.csv`) from Azure Blob Storage and updates the description and owner of assets in Azure Purview based on the CSV content.
- **Authentication:** Uses Azure AD (DefaultAzureCredential or ManagedIdentityCredential) for both Purview and Blob Storage access, depending on the environment.

## How It Works
1. **Trigger:**
   - The function is triggered by a timer (scheduled to run once per day).
2. **CSV Download:**
   - Downloads `assetinfo.csv` from the configured Azure Blob Storage container.
3. **CSV Parsing:**
   - Parses the CSV file into a list of `AssetInfo` objects.
4. **Purview Update:**
   - For each asset in the CSV, retrieves the asset from Purview by GUID.
   - Updates the asset's description and owner if found.
   - Logs the result of each update.

## Environment Variables
Set these in your `local.settings.json` or Azure Function App settings:
- `PURVIEW_ENDPOINT`: Azure Purview account endpoint (e.g., `https://<your-purview>.purview.azure.com`)
- `PURVIEW_RESOURCE_URL`: Azure Purview resource URL (default: `https://purview.azure.net`)
- `STORAGE_ACCOUNT_NAME`: Azure Storage account name
- `STORAGE_CONTAINER_NAME`: Blob container name
- `STORAGE_BLOB_PATH`: Path to the CSV blob (e.g., `Metadata/assetinfo.csv`)
- `env`: Set to `Local` for local development (uses DefaultAzureCredential)

## Main Classes

### UpdateMetadata (public)
- **Purpose:** Main Azure Function class. Handles timer trigger, reads CSV, and updates Purview assets.
- **Key Methods:**
  - `Run`: Entry point for the function.
  - `UpdateAssetsFromCsvAsync`: Reads and processes the CSV.
  - `UpdateAssetByGuidAsync`: Updates a single asset in Purview.
  - `GetAssetInfoCsvStreamFromBlobAsync`: Downloads the CSV from Blob Storage.
  - `GetAccessTokenAsync`: Gets an Azure AD token for Purview API calls.

### AssetInfo (public)
- **Purpose:** Model representing a row in the CSV file, containing asset metadata fields.
- **Properties:** CollectionName, AssetFQN, AssetName, AssetDescription, OwnerId, ParentAssetFQN, IsColumn, Guid.

### AssetInfoMap (private)
- **Purpose:** Maps CSV columns to `AssetInfo` properties for CsvHelper.
- **Usage:** Used internally to parse the CSV file.

## Example CSV Format
CollectionName,AssetFQN,AssetName,AssetDescription,OwnerId,ParentAssetFQN,IsColumn,Guid
ExampleCollection,example-fqn,ExampleAsset,Description,owner@example.com,parent-fqn,false,1234-5678

## Notes
- Make sure the Azure Function App has the necessary permissions to access both Azure Purview and Blob Storage.
- The function logs all operations for diagnostics.
