# PurviewDataMapMetadata


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
