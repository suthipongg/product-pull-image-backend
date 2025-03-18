import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from dotenv import load_dotenv

from pydantic import BaseModel, Field
from typing import Union, Mapping

from mongodb_controller import MGFuncs
from bson import ObjectId

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Adjust if frontend runs on another host
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

class BodyList(BaseModel):
    include: list[str] = Field([], description='List of fields to include in response')
    exclude: list[str] = Field([], description='List of fields to exclude from response')
    page: int = Field(1, ge=1, description='Page number for pagination (default: 1)')
    pageSize: int = Field(25, ge=1, description='Number of items per page (default: 12)')
    sort: Mapping[str, Union[int, float, str]] = Field({}, description='Sort criteria for results')
    filter: dict = Field({}, description='Filter criteria for results')

    def __init__(self, **data):
        super().__init__(**data)

        if self.include and self.exclude:
            raise ValueError('Include and exclude fields cannot be used together')

@app.post("/products")
def get_products(
    body: BodyList
):
    selected = body.filter.pop('selected', None)
    if selected is None:
        query_selected = {}
    elif selected:
        query_selected = {'selected_values': True}
    else:
        query_selected = {'selected_values': {'$ne': True}}
    pipeline = lambda match: [
        {'$match': match},
        {
            '$group': {
                '_id': '$product_id', 
                'document': {'$first': '$$ROOT'},
                'selected_values': {'$addToSet': '$selected'}
            }
        }, 
        {
            '$match': query_selected
        },
        {
            '$replaceRoot': {
                'newRoot': {
                    'product_id': '$document.product_id', 
                    'product_name': '$document.product_name', 
                    'active': '$document.active', 
                    'url': '$document.url', 
                    'url_preview_image': '$document.url_preview_image', 
                    'brand_name': '$document.brand_name', 
                    'category_name': '$document.category_name', 
                    'subcategory_name': '$document.subcategory_name'
                }
            }
        }
    ]
    for key, value in body.filter.items():
        if key == "product_name":
            body.filter[key] = {"regex": value}
    
    items, pagination = MGFuncs.aggregate_collection(body.model_dump(), collection, pipeline)
    result = []
    for item in items:
        if '_id' in item:
            item['id'] = str(item.pop('_id'))
        result.append(item)
    
    return {
        "data": result,
        "meta": pagination,
        "status": True,
        "detail": "Products fetched successfully"
    }

@app.get("/product/{product_id}")
def get_product(product_id: int):
    product = collection.find({"product_id": product_id}, {"title": 1, "link":1, "context":1, "selected":1})
    if not product:
        return {"error": "Product not found"}
    result = []
    for item in product:
        if '_id' in item:
            item['id'] = str(item.pop('_id'))
        result.append(item)
    return result


class UpdateSelected(BaseModel):
    id: str
    selected: bool

class UpdateSelectedRequest(BaseModel):
    ids: list[UpdateSelected]

@app.post("/update-selected")
async def update_selected(data: UpdateSelectedRequest):
    for item in data.ids:
        oid = ObjectId(item.id)
        collection.update_one({"_id": oid}, {"$set": {"selected": item.selected}})
    return {"status": True, "detail": "Selected items updated successfully"}