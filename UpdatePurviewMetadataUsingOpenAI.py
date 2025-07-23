from azure.purview.catalog import PurviewCatalogClient
from azure.purview.administration.account import PurviewAccountClient
from azure.identity import DefaultAzureCredential
import pandas as pd
import math
import openai
import os
from typing import Optional
from dotenv import load_dotenv
import argparse
import sys

# Load environment variables from .env file
load_dotenv()

reference_name_purview = ''
purview_account_name = ""
purview_endpoint = f"https://{purview_account_name}.purview.azure.com"

# OpenAI Configuration - Set your API key as environment variable: OPENAI_API_KEY
openai.api_key = os.getenv('OPENAI_API_KEY')

def generate_asset_description_with_openai(asset_name: str, asset_fqn: str, existing_description: str = "") -> Optional[str]:
    """
    Generate an enhanced asset description using OpenAI API
    Supports both older openai library (0.28.x) and newer versions (1.x+)
    """
    try:
        # Create a prompt based on asset information
        prompt = f"""
        You are a data governance expert. Please generate a clear, professional description for the following data asset:
        
        Asset Name: {asset_name}
        Asset Qualified Name: {asset_fqn}
        Current Description: {existing_description}
        
        Please provide a comprehensive description that includes:
        1. What type of data this asset likely contains based on its name
        2. Its potential business purpose or use case
        3. Any relevant technical details inferred from the qualified name
        
        Keep the description concise but informative (2-3 sentences maximum).
        """
        
        # Try newer OpenAI client first (v1.x+)
        try:
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a data governance expert who creates clear, professional asset descriptions."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=200,
                temperature=0.3
            )
            generated_description = response.choices[0].message.content.strip()
            
        except ImportError:
            # Fall back to older OpenAI library (0.28.x)
            response = openai.ChatCompletion.create(
                model="gpt-4",  # or "gpt-3.5-turbo" for cost optimization
                messages=[
                    {"role": "system", "content": "You are a data governance expert who creates clear, professional asset descriptions."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=200,
                temperature=0.3
            )
            generated_description = response.choices[0].message.content.strip()
        
        print(f"Generated description for {asset_name}: {generated_description}")
        return generated_description
        
    except Exception as e:
        print(f"Error generating description for {asset_name}: {str(e)}")
        print(f"Falling back to existing description: {existing_description}")
        return existing_description if existing_description else f"Data asset: {asset_name}"

def update_csv_with_openai_descriptions(csv_path: str, dry_run: bool = False) -> pd.DataFrame:
    """
    Update the CSV file with OpenAI-generated descriptions
    """
    df = pd.read_csv(csv_path)
    
    print("Generating enhanced descriptions using OpenAI...")
    for index, row in df.iterrows():
        asset_name = row['AssetName']
        asset_fqn = row['AssetFQN']
        current_description = row.get('AssetDescription', '')
        
        # Skip if description is already comprehensive (you can adjust this logic)
        if pd.isna(current_description) or len(str(current_description)) < 50:
            print(f"Generating description for: {asset_name}")
            if not dry_run:
                new_description = generate_asset_description_with_openai(asset_name, asset_fqn, str(current_description))
                df.at[index, 'AssetDescription'] = new_description
            else:
                print(f"[DRY RUN] Would generate description for: {asset_name}")
        else:
            print(f"Skipping {asset_name} - already has comprehensive description")
    
    if not dry_run:
        # Save the updated CSV
        backup_path = csv_path.replace('.csv', '_backup.csv')
        df.to_csv(backup_path, index=False)
        print(f"Backup saved to: {backup_path}")
        
        df.to_csv(csv_path, index=False)
        print(f"Updated CSV saved to: {csv_path}")
    
    return df

def replace_nan_with_none(obj):
    if isinstance(obj, dict):
        return {k: replace_nan_with_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_with_none(v) for v in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    else:
        return obj

def get_credentials():
    return DefaultAzureCredential()

def get_catalog_client(reference_name_purview):
    credential = get_credentials()
    client = PurviewCatalogClient(endpoint=f"https://{reference_name_purview}.purview.azure.com/", credential=credential, logging_enable=True)
    return client

def get_admin_client(reference_name_purview):
    credential = get_credentials()
    client = PurviewAccountClient(endpoint=f"https://{reference_name_purview}.purview.azure.com/", credential=credential, logging_enable=True)
    return client

def get_collection_Id(collection_name):
    client = get_admin_client(reference_name_purview)
    collection_name_unique_id = ''
    collection_list = client.collections.list_collections()
    for collection in collection_list:
        if collection["friendlyName"].lower() == collection_name.lower():
            collection_name_unique_id = collection["name"]
            print('Collection ID found:', collection_name_unique_id)
    return collection_name_unique_id

def queryCollection(collection_name, reference_name_purview):
    payload = {
        "keywords": "*",
        "filter": {
            "and": [
                {
                    "or": [
                        {
                            "collectionId": get_collection_Id(collection_name)
                        }
                    ]
                }
            ]
        }
    }
    catalog_client = get_catalog_client(reference_name_purview)
    json_results = catalog_client.discovery.query(payload)
    return json_results

# Read the assets information from the csv file that needs to be updated
def main():
    """Main function to run the Purview metadata update process"""
    parser = argparse.ArgumentParser(description='Update Purview metadata with OpenAI-generated descriptions')
    parser.add_argument('--csv-file', default=r"assetinfo.csv", 
                       help='Path to the CSV file containing asset information')
    parser.add_argument('--skip-openai', action='store_true', 
                       help='Skip OpenAI description generation and only update Purview with existing descriptions')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be updated without making actual changes')
    
    args = parser.parse_args()
    
    csv_file_path = args.csv_file
    
    # Check if CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"❌ CSV file not found: {csv_file_path}")
        sys.exit(1)
    
    # Check OpenAI API key if not skipping
    if not args.skip_openai and not os.getenv('OPENAI_API_KEY'):
        print("❌ OPENAI_API_KEY not found in environment variables")
        print("Either set the environment variable or use --skip-openai flag")
        sys.exit(1)
    
    print("🚀 Starting Purview Metadata Update Process")
    print(f"📁 CSV File: {csv_file_path}")
    print(f"🤖 OpenAI Generation: {'Disabled' if args.skip_openai else 'Enabled'}")
    print(f"🧪 Dry Run: {'Yes' if args.dry_run else 'No'}")
    print("-" * 60)

    if not args.skip_openai:
        # First, update the CSV with OpenAI-generated descriptions
        print("=== STEP 1: Updating CSV with OpenAI-generated descriptions ===")
        df = update_csv_with_openai_descriptions(csv_file_path, dry_run=args.dry_run)
    else:
        print("=== STEP 1: Loading existing CSV (skipping OpenAI generation) ===")
        df = pd.read_csv(csv_file_path)
        print(f"Loaded {len(df)} rows from CSV")

    print("\n=== STEP 2: Updating Purview with enhanced descriptions ===")
    
    for collection_name in df['CollectionName'].unique():
        df_subset = df[df['CollectionName'] == collection_name]
        
        if args.dry_run:
            print(f"[DRY RUN] Would query collection: {collection_name}")
            print(f"[DRY RUN] Would update {len(df_subset)} assets")
            continue
            
        response = queryCollection(collection_name, reference_name_purview)
        assets = response['value']
        print(f"Found {len(assets)} assets in collection: {collection_name}")
        print('assets:', assets)
        assets_to_update = [asset for asset in assets if asset['qualifiedName'] in df_subset['AssetFQN'].tolist()]

        print(f"Found {len(assets_to_update)} assets to update in collection: {collection_name}")

        for asset in assets_to_update:
            catalog_client = get_catalog_client(reference_name_purview)
            asset_name = asset['name']
            asset_id = asset['id']
            print(f"Updating metadata for asset: {asset_name}, id: {asset_id}")
            entity_response = catalog_client.entity.get_by_guid(asset_id)
            _entity_response = entity_response
            print(f"Entity response: {_entity_response}")

            # Remove the userDescription field if present
            _entity_response['entity']['attributes'].pop('userDescription', None)
            # Add the enhanced userDescription field from the csv file
            enhanced_description = df[df['AssetName'] == asset_name]['AssetDescription'].values[0]
            _entity_response['entity']['attributes']['userDescription'] = enhanced_description
            print(f"Setting description: {enhanced_description}")

            # Get new owner id from CSV
            new_owner_id = df[df['AssetName'] == asset_name]['OwnerId'].values[0]
            # Only update if OwnerId is valid
            if pd.isna(new_owner_id) or str(new_owner_id).lower() == 'nan':
                print(f"Skipping owner update for asset '{asset_name}' due to missing OwnerId.")
            else:
                # Get or create the contacts dictionary
                contacts = _entity_response['entity'].get('contacts', {})
                # Always set the Owner list to the new owner
                contacts['Owner'] = [{'id': new_owner_id}]
                # Assign back to the entity
                _entity_response['entity']['contacts'] = contacts
                print(f"Setting owner: {new_owner_id}")

            # Clean NaN values before sending to Purview
            _entity_response = replace_nan_with_none(_entity_response)
            
            try:
                catalog_client.entity.create_or_update(_entity_response)
                print(f"Successfully updated asset: {asset_name}")
            except Exception as e:
                print(f"Error updating asset {asset_name}: {str(e)}")
                continue

            # Update columns if needed
            try:
                response_referredEntities = _entity_response.get('referredEntities', {})
                if response_referredEntities:
                    columns_guids = response_referredEntities.keys()
                    df_subset_columns = df_subset[(df_subset['ParentAssetFQN'] == asset['qualifiedName']) & (df_subset['IsColumn'] == "Yes")]
                    if not df_subset_columns.empty:
                        for column_guid in columns_guids:
                            column_metadata = response_referredEntities[column_guid]
                            if column_metadata['attributes']['name'] in df_subset_columns['AssetName'].tolist():
                                qualifiedName = column_metadata['attributes']['qualifiedName']
                                print(f"Updating metadata for column: {qualifiedName}")
                                column_metadata['attributes'].pop('userDescription', None)
                                column_desc = df_subset_columns[df_subset_columns['AssetName'] == column_metadata['attributes']['name']]['AssetDescription'].values[0]
                                column_metadata['attributes']['userDescription'] = column_desc
                                print(f"Set column description: {column_desc}")
                    else:
                        print(f"No columns in scope for asset: {asset_name}")
                else:
                    print(f"No referred entities found for asset: {asset_name}")
            except Exception as e:
                print(f"Error updating columns for asset {asset_name}: {str(e)}")

    print("\n=== UPDATE COMPLETE ===")
    print("Summary:")
    print("1. CSV file updated with OpenAI-generated descriptions" if not args.skip_openai else "1. CSV file loaded (OpenAI generation skipped)")
    print("2. Purview assets updated with enhanced descriptions" if not args.dry_run else "2. Purview updates simulated (dry run)")
    print("3. Asset owners updated where provided" if not args.dry_run else "3. Asset owner updates simulated (dry run)")
    print("4. Column descriptions updated where applicable" if not args.dry_run else "4. Column description updates simulated (dry run)")

if __name__ == "__main__":
    main()
