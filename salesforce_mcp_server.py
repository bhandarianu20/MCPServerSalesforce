import os
import json
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio

from fastmcp import FastMCP
from simple_salesforce import Salesforce, SalesforceError
from pydantic import BaseModel

# Salesforce Configuration
SF_USERNAME = "anu.bhandari659@agentforce.com"
SF_PASSWORD = "Anu@2253"
SF_SECURITY_TOKEN = "sofSGJaOoDnWyiFnGcvxtggl0"
SF_CONSUMER_KEY = "3MVG9rZjd7MXFdLjf7TZnBg37ONcEmhx3aNEn2kXJ3zNzB282_FNavUk03w7QehlZ3DSIZ1ywHmqWvzfPDrXC"
SF_CONSUMER_SECRET = "092B950458E99061391AE8561E623C06DBC531341E52B068E714377978024A83"
SF_INSTANCE_URL = "https://orgfarm-e4bd384b9b-dev-ed.develop.lightning.force.com"

# Initialize FastMCP
mcp = FastMCP("Salesforce Data Manager")

# Global Salesforce connection
sf_connection = None

def get_salesforce_connection():
    """Get or create Salesforce connection"""
    global sf_connection
    if sf_connection is None:
        try:
            sf_connection = Salesforce(
                username=SF_USERNAME,
                password=SF_PASSWORD,
                security_token=SF_SECURITY_TOKEN,
                consumer_key=SF_CONSUMER_KEY,
                consumer_secret=SF_CONSUMER_SECRET,
                instance_url=SF_INSTANCE_URL,
                version="59.0"
            )
            print(f"Connected to Salesforce: {sf_connection.instance_url}")
        except Exception as e:
            print(f"Salesforce connection error: {e}")
            raise
    return sf_connection

# Pydantic models for type validation
class QueryRequest(BaseModel):
    soql_query: str
    limit: Optional[int] = 100

class UpdateRequest(BaseModel):
    object_name: str
    record_id: str
    fields: Dict[str, Any]

class CreateRequest(BaseModel):
    object_name: str
    fields: Dict[str, Any]

class BulkUpdateRequest(BaseModel):
    object_name: str
    records: List[Dict[str, Any]]
    external_id_field: Optional[str] = None

@mcp.tool()
def query_salesforce(soql_query: str, limit: int = 100) -> Dict[str, Any]:
    """
    Execute a SOQL query against Salesforce
    
    Args:
        soql_query: The SOQL query to execute (e.g., "SELECT Id, Name FROM Account")
        limit: Maximum number of records to return (default: 100, max: 2000)
    
    Returns:
        Dictionary containing query results and metadata
    """
    try:
        sf = get_salesforce_connection()
        
        # Add LIMIT if not already present and limit is specified
        if limit and "LIMIT" not in soql_query.upper():
            soql_query += f" LIMIT {min(limit, 2000)}"
        
        result = sf.query_all(soql_query)
        
        return {
            "success": True,
            "total_size": result['totalSize'],
            "done": result['done'],
            "records": result['records'],
            "query": soql_query
        }
        
    except SalesforceError as e:
        return {
            "success": False,
            "error": f"Salesforce error: {str(e)}",
            "query": soql_query
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "query": soql_query
        }

@mcp.tool()
def get_account_summary() -> Dict[str, Any]:
    """
    Get a summary of Account records with key metrics
    
    Returns:
        Summary statistics of Account records
    """
    try:
        sf = get_salesforce_connection()
        
        # Get total count
        total_query = "SELECT COUNT() FROM Account"
        total_result = sf.query(total_query)
        total_count = total_result['totalSize']
        
        # Get recent accounts
        recent_query = """
        SELECT Id, Name, Industry, Type, KYC_Status_c__c, 
               Monthly_Transaction_Volume_c__c, Product_Usage_c__c,
               CreatedDate
        FROM Account 
        ORDER BY CreatedDate DESC 
        LIMIT 10
        """
        recent_result = sf.query(recent_query)
        
        # Get KYC status breakdown
        kyc_query = """
        SELECT KYC_Status_c__c, COUNT(Id) cnt 
        FROM Account 
        WHERE KYC_Status_c__c != NULL 
        GROUP BY KYC_Status_c__c
        """
        kyc_result = sf.query(kyc_query)
        
        return {
            "success": True,
            "total_accounts": total_count,
            "recent_accounts": recent_result['records'],
            "kyc_breakdown": kyc_result['records']
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error getting account summary: {str(e)}"
        }

@mcp.tool()
def update_record(object_name: str, record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update a single Salesforce record
    
    Args:
        object_name: Salesforce object API name (e.g., 'Account', 'Contact')
        record_id: Salesforce record ID (18-character ID)
        fields: Dictionary of field names and values to update
    
    Returns:
        Result of the update operation
    """
    try:
        sf = get_salesforce_connection()
        
        # Get the object
        sobject = getattr(sf, object_name)
        
        # Update the record
        result = sobject.update(record_id, fields)
        
        return {
            "success": True,
            "record_id": record_id,
            "object_name": object_name,
            "updated_fields": fields,
            "result": result
        }
        
    except SalesforceError as e:
        return {
            "success": False,
            "error": f"Salesforce error: {str(e)}",
            "record_id": record_id,
            "object_name": object_name
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "record_id": record_id,
            "object_name": object_name
        }

@mcp.tool()
def create_record(object_name: str, fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create a new Salesforce record
    
    Args:
        object_name: Salesforce object API name (e.g., 'Account', 'Contact')
        fields: Dictionary of field names and values for the new record
    
    Returns:
        Result of the create operation including new record ID
    """
    try:
        sf = get_salesforce_connection()
        
        # Get the object
        sobject = getattr(sf, object_name)
        
        # Create the record
        result = sobject.create(fields)
        
        return {
            "success": True,
            "object_name": object_name,
            "new_record_id": result['id'],
            "created_fields": fields,
            "result": result
        }
        
    except SalesforceError as e:
        return {
            "success": False,
            "error": f"Salesforce error: {str(e)}",
            "object_name": object_name,
            "attempted_fields": fields
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "object_name": object_name,
            "attempted_fields": fields
        }

@mcp.tool()
def bulk_upsert_records(object_name: str, records: List[Dict[str, Any]], external_id_field: str = None) -> Dict[str, Any]:
    """
    Bulk upsert multiple records to Salesforce
    
    Args:
        object_name: Salesforce object API name
        records: List of dictionaries containing record data
        external_id_field: External ID field for upsert (if None, will insert)
    
    Returns:
        Results of the bulk operation
    """
    try:
        sf = get_salesforce_connection()
        
        if not records:
            return {
                "success": False,
                "error": "No records provided"
            }
        
        # Limit to reasonable batch size
        if len(records) > 200:
            records = records[:200]
        
        # Get the bulk object
        bulk_object = getattr(sf.bulk, object_name)
        
        if external_id_field:
            # Upsert operation
            results = bulk_object.upsert(records, external_id_field)
            operation = "upsert"
        else:
            # Insert operation
            results = bulk_object.insert(records)
            operation = "insert"
        
        # Count successes and errors
        successes = sum(1 for r in results if r.get('success'))
        errors = len(results) - successes
        
        error_details = [r for r in results if not r.get('success')]
        
        return {
            "success": True,
            "operation": operation,
            "object_name": object_name,
            "total_processed": len(results),
            "successes": successes,
            "errors": errors,
            "error_details": error_details[:10],  # First 10 errors
            "external_id_field": external_id_field
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Bulk operation error: {str(e)}",
            "object_name": object_name,
            "record_count": len(records)
        }

@mcp.tool()
def get_object_schema(object_name: str) -> Dict[str, Any]:
    """
    Get schema information for a Salesforce object
    
    Args:
        object_name: Salesforce object API name
    
    Returns:
        Object schema including fields and their properties
    """
    try:
        sf = get_salesforce_connection()
        
        # Get object description
        sobject = getattr(sf, object_name)
        description = sobject.describe()
        
        # Extract key information
        fields_info = []
        for field in description['fields']:
            fields_info.append({
                'name': field['name'],
                'label': field['label'],
                'type': field['type'],
                'required': not field['nillable'] and not field['defaultedOnCreate'],
                'updateable': field['updateable'],
                'createable': field['createable'],
                'length': field.get('length'),
                'picklistValues': [pv['value'] for pv in field.get('picklistValues', [])]
            })
        
        return {
            "success": True,
            "object_name": object_name,
            "label": description['label'],
            "fields": fields_info,
            "createable": description['createable'],
            "updateable": description['updateable'],
            "deletable": description['deletable']
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error getting schema: {str(e)}",
            "object_name": object_name
        }

@mcp.tool()
def search_records(search_term: str, objects: List[str] = None) -> Dict[str, Any]:
    """
    Search for records across Salesforce objects using SOSL
    
    Args:
        search_term: Term to search for
        objects: List of object names to search in (default: Account, Contact)
    
    Returns:
        Search results from specified objects
    """
    try:
        sf = get_salesforce_connection()
        
        if objects is None:
            objects = ['Account', 'Contact']
        
        # Build SOSL query
        object_clauses = []
        for obj in objects:
            if obj == 'Account':
                object_clauses.append("Account(Id, Name, Industry, Type, Phone)")
            elif obj == 'Contact':
                object_clauses.append("Contact(Id, Name, Email, Phone, AccountId)")
            else:
                object_clauses.append(f"{obj}(Id, Name)")
        
        sosl_query = f"FIND '{search_term}' IN ALL FIELDS RETURNING {', '.join(object_clauses)}"
        
        results = sf.search(sosl_query)
        
        return {
            "success": True,
            "search_term": search_term,
            "objects_searched": objects,
            "results": results,
            "query": sosl_query
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Search error: {str(e)}",
            "search_term": search_term
        }

if __name__ == "__main__":
    # Test connection on startup
    try:
        sf = get_salesforce_connection()
        print("✅ Salesforce connection successful!")
        print(f"Instance URL: {sf.instance_url}")
    except Exception as e:
        print(f"❌ Salesforce connection failed: {e}")
    
    # Run the MCP server
    mcp.run()
