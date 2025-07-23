using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;
using CsvHelper;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

public class AssetInfo
{
    public string CollectionName { get; set; } = string.Empty;
    public string AssetFQN { get; set; } = string.Empty;
    public string AssetName { get; set; } = string.Empty;
    public string AssetDescription { get; set; } = string.Empty;
    public string OwnerId { get; set; } = string.Empty;
    public string ParentAssetFQN { get; set; } = string.Empty;
    public string IsColumn { get; set; } = string.Empty;
    public string Guid { get; set; } = string.Empty;
}

public class Function1
{
    private readonly ILogger _logger;
    private readonly string _csvPath = Path.Combine(AppContext.BaseDirectory, "assetinfo.csv");
    // Replace with your actual Purview account endpoint from Azure Portal
    private readonly string _purviewEndpoint = "https://<PurviewID>.purview.azure.com";
    private readonly HttpClient _httpClient;

    public Function1(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<Function1>();
        _httpClient = new HttpClient();
    }

    [Function("Function1")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestData req)
    {
        await UpdateAssetsFromCsvAsync();
        var response = req.CreateResponse(System.Net.HttpStatusCode.OK);
        await response.WriteStringAsync("Assets updated successfully.");
        return response;
    }

    private async Task<string?> GetAccessTokenAsync()
    {
        var credential = new DefaultAzureCredential();
        var tokenRequestContext = new TokenRequestContext(new[] { "https://purview.azure.net/.default" });
        var token = await credential.GetTokenAsync(tokenRequestContext);
        return token.Token;
    }

    private async Task<string?> GetCollectionIdAsync(string collectionName)
    {
        var token = await GetAccessTokenAsync();
        // Use the correct API version for the collections endpoint
        var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"{_purviewEndpoint}/account/collections?api-version=2023-10-01-preview");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        var response = await _httpClient.SendAsync(request);

        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync();
            _logger.LogError($"Failed to get collections. Status: {response.StatusCode}, Content: {errorContent}");
            throw new HttpRequestException($"Failed to get collections. Status: {response.StatusCode}, Content: {errorContent}");
        }

        var content = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(content);
        foreach (var collection in doc.RootElement.GetProperty("value").EnumerateArray())
        {
            var friendlyName = collection.TryGetProperty("friendlyName", out var fn) ? fn.GetString() : null;
            if (!string.IsNullOrEmpty(friendlyName) && friendlyName.Equals(collectionName, StringComparison.OrdinalIgnoreCase))
            {
                return collection.TryGetProperty("name", out var nameProp) ? nameProp.GetString() : null;
            }
        }
        return null;
    }

    private async Task<List<JsonElement>> GetAssetsInCollectionAsync(string collectionId, List<string> qualifiedNames)
    {
        var token = await GetAccessTokenAsync();
        var allAssets = new List<JsonElement>();
        // Purview API may have a limit on the number of values in a filter, so batch if needed
        const int batchSize = 25; // Adjust as needed for API limits
        for (int i = 0; i < qualifiedNames.Count; i += batchSize)
        {
            var batch = qualifiedNames.Skip(i).Take(batchSize).ToList();
            var request = new HttpRequestMessage(
                HttpMethod.Post,
                $"{_purviewEndpoint}/datamap/api/search/query?api-version=2023-09-01");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

            var body = new
            {
                keywords = (string?)null,
                limit = batchSize,
                filter = new
                {
                    collectionId = collectionId,
                    attributeName = "qualifiedName",
                    attributeValue = batch
                }
            };
            request.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");

            var response = await _httpClient.SendAsync(request);
            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                _logger.LogError($"Failed to get assets for collectionId '{collectionId}'. Status: {response.StatusCode}, Content: {errorContent}");
                throw new HttpRequestException($"Failed to get assets for collectionId '{collectionId}'. Status: {response.StatusCode}, Content: {errorContent}");
            }

            var content = await response.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(content);
            if (doc.RootElement.TryGetProperty("value", out var value))
            {
                allAssets.AddRange(value.EnumerateArray().Select(e => e.Clone()));
            }
        }
        return allAssets;
    }

    private async Task<JsonElement?> GetAssetByGuidAsync(string guid)
    {
        var token = await GetAccessTokenAsync();
        var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"{_purviewEndpoint}/datamap/api/atlas/v2/entity/guid/{guid}");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        var response = await _httpClient.SendAsync(request);
        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync();
            _logger.LogError($"Failed to get asset by guid {guid}. Status: {response.StatusCode}, Content: {errorContent}");
            return null;
        }
        var content = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(content);
        if (doc.RootElement.TryGetProperty("entity", out var entity))
        {
            return entity.Clone();
        }
        return null;
    }

    private async Task<JsonElement?> GetAssetByQualifiedNameAsync(string qualifiedName)
    {
        var token = await GetAccessTokenAsync();
        var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"{_purviewEndpoint}/catalog/api/atlas/v2/entity/uniqueAttribute/type/azure_datalake_gen2_resource_set?attr:qualifiedName={Uri.EscapeDataString(qualifiedName)}&api-version=2023-02-01-preview");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        var response = await _httpClient.SendAsync(request);
        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync();
            _logger.LogError($"Failed to get asset by qualifiedName {qualifiedName}. Status: {response.StatusCode}, Content: {errorContent}");
            return null;
        }
        var content = await response.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(content);
        if (doc.RootElement.TryGetProperty("entity", out var entity))
        {
            return entity.Clone();
        }
        return null;
    }

    private async Task<bool> UpdateAssetByGuidAsync(string guid, string description, AssetInfo assetInfo)
    {
        var asset = await GetAssetByGuidAsync(guid);
        if (asset == null) return false;
        var typeName = asset.Value.GetProperty("typeName").GetString();
        var attributes = asset.Value.GetProperty("attributes");
        var attributesDict = new Dictionary<string, object>();
        foreach (var prop in attributes.EnumerateObject())
        {
            attributesDict[prop.Name] = prop.Value.ValueKind == JsonValueKind.Null ? null : prop.Value.Deserialize<object>();
        }
        // Overwrite or add values from CSV
        attributesDict["userDescription"] = assetInfo.AssetDescription;
        if (!string.IsNullOrWhiteSpace(assetInfo.AssetFQN))
            attributesDict["qualifiedName"] = assetInfo.AssetFQN;
        if (!string.IsNullOrWhiteSpace(assetInfo.AssetName))
            attributesDict["name"] = assetInfo.AssetName;
        if (!string.IsNullOrWhiteSpace(assetInfo.OwnerId))
            attributesDict["owner"] = assetInfo.OwnerId;
        // Add more mappings as needed from assetInfo

        var updateEntity = new
        {
            entity = new
            {
                typeName = typeName,
                guid = guid,
                attributes = attributesDict
            }
        };
        var token = await GetAccessTokenAsync();
        var updateRequest = new HttpRequestMessage(
            HttpMethod.Post,
            $"{_purviewEndpoint}/datamap/api/atlas/v2/entity")
        {
            Content = new StringContent(JsonSerializer.Serialize(updateEntity), Encoding.UTF8, "application/json")
        };
        updateRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        var updateResponse = await _httpClient.SendAsync(updateRequest);
        if (!updateResponse.IsSuccessStatusCode)
        {
            var errorContent = await updateResponse.Content.ReadAsStringAsync();
            _logger.LogError($"Failed to update asset by guid {guid}: {errorContent}");
            return false;
        }
        _logger.LogInformation($"Updated description for asset guid: {guid}");
        return true;
    }

    public async Task UpdateAssetsFromCsvAsync()
    {
        if (!File.Exists(_csvPath))
        {
            _logger.LogError($"CSV file not found at path: {_csvPath}");
            return;
        }

        List<AssetInfo> assetInfos;
        using (var reader = new StreamReader(_csvPath))
        using (var csv = new CsvReader(reader, new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture)
        {
            MissingFieldFound = null // Ignore missing fields
        }))
        {
            csv.Context.RegisterClassMap<AssetInfoMap>();
            assetInfos = csv.GetRecords<AssetInfo>().ToList();
        }

        foreach (var assetInfo in assetInfos)
        {
            if (string.IsNullOrWhiteSpace(assetInfo.Guid))
            {
                _logger.LogWarning($"No Guid found for asset: {assetInfo.AssetName}");
                continue;
            }
            _logger.LogInformation($"Updating asset by Guid: {assetInfo.Guid} ({assetInfo.AssetName})");
            var success = await UpdateAssetByGuidAsync(assetInfo.Guid, assetInfo.AssetDescription, assetInfo);
            if (success)
            {
                _logger.LogInformation($"Successfully updated asset: {assetInfo.AssetName} (Guid: {assetInfo.Guid})");
            }
            else
            {
                _logger.LogError($"Failed to update asset: {assetInfo.AssetName} (Guid: {assetInfo.Guid})");
            }
        }
    }
}

public sealed class AssetInfoMap : CsvHelper.Configuration.ClassMap<AssetInfo>
{
    public AssetInfoMap()
    {
        Map(m => m.CollectionName).Name("CollectionName");
        Map(m => m.AssetName).Name("AssetName");
        Map(m => m.AssetFQN).Name("AssetFQN");
        Map(m => m.AssetDescription).Name("AssetDescription");
        Map(m => m.IsColumn).Name("IsColumn");
        Map(m => m.ParentAssetFQN).Name("ParentAssetFQN");
        Map(m => m.OwnerId).Name("OwnerId");
        Map(m => m.Guid).Name("Guid");
    }
}

public class LocalSettings
{
    public bool IsEncrypted { get; set; }
    public Dictionary<string, string> Values { get; set; } = new Dictionary<string, string>
    {
        { "AzureWebJobsStorage", "UseDevelopmentStorage=true" },
        { "FUNCTIONS_WORKER_RUNTIME", "dotnet" }
    };
}
