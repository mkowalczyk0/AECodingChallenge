"""

debated between:
1. Handle clean/process in python, load into DWH to save messy SQL (easier to build our models)
2. Dump raw data into DHW, handle clean using messy SQL

basically debating between ETL or ELT (assuming this would be our extraction step and this raw data comes this way, then we can just schedule this script)

Thinking:
    assess and clean raw -> 
    save to cleaned -> 
    load into DWH -> 
    write SQL to model -> 
    write SQL unit tests and DQ checks in case I missed anything ->
    answer q's ->
    QA model ->
    answer q's ->
    QA model 

"""

import json
import pandas as pd

"""
we could probably use polars but this is a good proof of concept. 
polars would give us better performance, more memory efficient, more explicit operations, better handling of large datasets

"""

import unicodedata
import re
from typing import Optional
from dotenv import load_dotenv
import os

load_dotenv()

CSV_PATH = os.getenv("CSV_PATH")
CLEANED_JSON_PATH = os.getenv("CLEANED_JSON_PATH")
RAW_JSON_PATH = os.getenv("RAW_JSON_PATH")


def clean_text(text: Optional[str]) -> Optional[str]:
    # bunch of silly characters that need handling in brands, replacing & with and for consistency
    if not text:
        return None
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode()
    text = text.replace("&", "and")
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    return text.strip()

# will handle the receipts and items with this function
def clean_receipts(input_path: str) -> None:
    print("Starting receipts cleaning")
    
    with open(input_path, 'r') as f:
        data = [json.loads(line) for line in f]
    
    receipts = []
    
    for entry in data:
        receipt = {'_id': entry['_id']['$oid']}
        
        # loop through date and num, i dont like handling nulls with dates 
        date_fields = ['createDate', 'dateScanned', 'finishedDate', 'modifyDate', 'pointsAwardedDate', 'purchaseDate']
        for field in date_fields:
            if entry.get(field):
                receipt[field] = pd.to_datetime(entry[field]['$date'], unit='ms')
            else:
                receipt[field] = pd.NaT

        numeric_fields = ['bonusPointsEarned', 'pointsEarned', 'purchasedItemCount', 'totalSpent']
        for field in numeric_fields:
            value = entry.get(field)
            receipt[field] = pd.to_numeric(value, errors='coerce') if value not in (None, '') else None

        receipt['rewardsReceiptStatus'] = entry.get('rewardsReceiptStatus')
        receipt['userId'] = entry.get('userId')
        receipt['bonusPointsEarnedReason'] = entry.get('bonusPointsEarnedReason', 'UNKNOWN')
        
        receipts.append(receipt)
    
    # clean rewardsReceiptItemList and split into separate df, this will be part of the model
    items = [] 
    for entry in data:
        receipt_id = entry['_id']['$oid']
        for item in entry.get('rewardsReceiptItemList', []):
            cleaned_item = {
                'receipt_id': receipt_id, 
                'barcode': item.get('barcode'),  # will leave this a string and leave nulls to prevent false matches if ever used, will need some more information
                'description': item.get('description', 'ITEM NOT FOUND'), # was thinking about cleaning the text but might be better to leave it raw because of volumes like 8.00oz or 3.2oz, considering this is testing data, prod should be better with the junk like '=-Cheddar'
                'finalPrice': pd.to_numeric(item.get('finalPrice'), errors='coerce'),
                'itemPrice': pd.to_numeric(item.get('itemPrice'), errors='coerce'),
                'quantityPurchased': pd.to_numeric(item.get('quantityPurchased'), errors='coerce'),
                'partnerItemId': item.get('partnerItemId')
            }
            items.append(cleaned_item)
    
    receipts_df = pd.DataFrame(receipts)
    
    items_df = pd.DataFrame(items)
    
    # adding offline export to each function
    receipts_df.to_csv(f'{CSV_PATH}cleaned_receipts.csv', index=False)
    receipts_df.to_json(f'{CLEANED_JSON_PATH}receipts.json', orient='records', date_format='iso')
    
    items_df.to_csv(f'{CSV_PATH}cleaned_items.csv', index=False)
    items_df.to_json(f'{CLEANED_JSON_PATH}items.json', orient='records', date_format='iso')
    
    print("Finished cleaning receipts and items")

def clean_brands(input_path: str) -> None:
    print("Starting brands cleaning")
    
    with open(input_path, 'r') as f:
        data = [json.loads(line) for line in f]
    
    brands = []
    for entry in data:
        brand = {'_id': entry['_id']['$oid']}
        
        brand['barcode'] = entry.get('barcode')
        
        if 'brandCode' in entry and entry['brandCode']:
            if not isinstance(entry['brandCode'], str) or entry['brandCode'].isdigit():
                brand['brandCode'] = clean_text(entry.get('name')).upper()
            else:
                brand['brandCode'] = clean_text(entry['brandCode'])
        else:
            brand['brandCode'] = clean_text(entry.get('name')).upper() if 'name' in entry else None
        
        brand['category'] = clean_text(entry.get('category'))
        if 'categoryCode' in entry and entry['categoryCode']:
            brand['categoryCode'] = entry['categoryCode'].replace(" ", "_")
        else:
            brand['categoryCode'] = clean_text(entry.get('category')).upper().replace(" ", "_") if 'category' in entry else None
        
        cpg = entry.get('cpg', {})
        brand['cpg_id'] = cpg.get('$id', {}).get('$oid')
        brand['cpg_ref'] = 'Cogs' if cpg.get('$ref') and cpg.get('$ref') != 'Cogs' else cpg.get('$ref')
        
        brand['topBrand'] = entry.get('topBrand', False)
        brand['name'] = clean_text(entry.get('name'))  # bunch of goofy characters in here
        
        brands.append(brand)
    
    brands_df = pd.DataFrame(brands)
    brands_df = brands_df.fillna('UNKNOWN')
    brands_df = brands_df.replace('', 'UNKNOWN') # since there is no time/num in brands we can handle null with fillna and replace for '' edge cases
    
    brands_df.to_csv(f'{CSV_PATH}cleaned_brands.csv', index=False)
    brands_df.to_json(f'{CLEANED_JSON_PATH}brands.json', orient='records', date_format='iso')
    
    print("Finished cleaning brands")

def clean_users(input_path: str) -> None:
    print("Starting users cleaning")
    
    with open(input_path, 'r') as f:
        data = [json.loads(line) for line in f]
    
    # duping in the table, think I'll handle it when modeling.
    users = []
    for entry in data:
        user = {'_id': entry['_id']['$oid']}
        
        date_fields = ['createdDate', 'lastLogin'] # using same logic from receipts
        for field in date_fields:
            if entry.get(field):
                user[field] = pd.to_datetime(entry[field]['$date'], unit='ms')
            else:
                user[field] = pd.NaT
        
        user['state'] = entry.get('state', 'UNKNOWN')  # defaulting this to NA would be silly so this one just leave it as 'UNKNOWN'
        user['role'] = 'CONSUMER' if str(entry.get('role')).upper() != 'CONSUMER' else str(entry.get('role')).upper() # constant as consumer but I see fetch-staff, wonder if this is intentional or there is a need to distinguish. for the sake of instructions, i will convert to consumer
        user['active'] = entry.get('active')  # wondering what the scope of this is, how is a user defined as active or not. based on last login? scanned a receipt in the last x days?  
        
        users.append(user)
    
    users_df = pd.DataFrame(users)
    
    users_df.to_csv(f'{CSV_PATH}cleaned_users.csv', index=False)
    users_df.to_json(f'{CLEANED_JSON_PATH}users.json', orient='records', date_format='iso')
    
    print("Finished cleaning users")

def main():
    
    receiptsPath = f'{RAW_JSON_PATH}receipts.json'
    brandsPath = f'{RAW_JSON_PATH}brands.json'
    usersPath = f'{RAW_JSON_PATH}users.json'

    
    try:
        clean_receipts(receiptsPath)
        clean_brands(brandsPath)
        clean_users(usersPath)
        print("--------------------DONE--------------------")
    except Exception as e:
        print(f"Error during cleaning process: {str(e)}")
        raise
    # error handling could be improved throughout but for time sake I think this cleaning is sufficient at least for the business Q's


# would kill this if I was using Pub/Sub for a trigger or scheduling some other way using some sort of entry point function
if __name__ == "__main__":
    main()