import sys
import pyodbc
import json
from fastapi import APIRouter, Request, Response
from pydantic import BaseModel
from fastapi.responses import PlainTextResponse
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse
sys.path.insert(1, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/herramientas')
sys.path.insert(2, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/data')
from connection import sapConnection, dbConnection


router = APIRouter(
        prefix='/reproceso'
)
dbobj = dbConnection()
sapobj = sapConnection()


fake_item_db = [{"item_name": "Foo"}, {"item_name": "Bar"}, {"item_name": "Baz"}]

class Item(BaseModel):
    name: str
    descr: str | None = None

 
@router.get("/param", response_class=PlainTextResponse, status_code = 201, tags=["reportes"])
def read_root():
    json_compatible_item_data = jsonable_encoder({"Nombre": "Ivan", "Apellidos": "Rojas Carrasco", "Notas": {"Matematica": 12, "Historia": 20}})
    return JSONResponse(content=json_compatible_item_data)


@router.get("/item/", status_code=200)
async def read_item(skip: int = 0, limit: int = 10):
    return fake_item_db[skip : skip + limit]


@router.post("/items/", status_code=201)
async def create_item(name: str):
    return {"name": name}

#Aqui falta enviar un status personalizado en caso no encuentre resultado
@router.get("/data/", status_code=307)
async def api_data(request: Request, response: Response):
    # try:
    params = request.query_params._dict
    url = f'https://http.cat/status/{params["status"]}'
    response = RedirectResponse(url=url)
    return response
    # except:
    #     response.status_code = status.HTTP_404_CREATED
    #     return response


@router.get("/subareas/")
# async def mirror(item: Item):
async def getareas():
    data = dbobj.getAllSubAreas()
    newData = list(map(getSubAreas, data))
    return {
        "data": newData
    }

@router.get("/material_repro/")
# async def mirror(item: Item):
async def getOdMaterials(odParameter: str, ofParameter: str):
    data = sapobj.getMaterialsByAD(odParameter, ofParameter);
    newData = list(map(getMaterialDatabyOf, data))
    newData.sort(key=lambda x: (int(x['order'].split(".")[1])+int(x['order'].split(".")[0])))
    newData[0]["baseCost"] = newData[-1]["costbyOne"]
    newData[0]["costbyOne"] = newData[-1]["costbyOne"]
    newData[0]["qtyRejected"] = newData[-1]["qtyRejected"]
    return {
        "data": newData
    }

@router.get("/material/")
async def getOdMaterials(odParameter: str, ofParameter: str):
    data = sapobj.getMaterialsByAD(odParameter, ofParameter);
    newData = list(map(getMaterialDatabyOf, data))
    newData.sort(key=lambda x: (int(x['order'].split(".")[1])+int(x['order'].split(".")[0])))
    newData[0]["baseCost"] = newData[-1]["costbyOne"]
    newData[0]["costbyOne"] = newData[-1]["costbyOne"]
    newData[0]["qtyRejected"] = newData[-1]["qtyRejected"]
    return {
        "data": newData
    }

@router.get("/material_pattern/")
async def getMaterialByPattern(pattern: str = None):
    data = sapobj.getMaterialsByPattern(pattern);
    newData = list(map(getMaterialDatabyPattern, data))
    return {
        "data": newData
    }

@router.get("/materials/")
async def getMaterial():
    data = sapobj.getMaterials();
    newData = list(map(getMaterialData, data))
    return {
        "data": newData
    }


def getSubAreas(x):
    return {
    "codigo": x[0],
    "nombre": x[1],
    }


def getMaterialDatabyOf(x):
    return {
    "order": x[0],
    "type": x[1],
    "detailCode": x[2],
    "detail": x[3],
    "und": x[4],
    "baseQty": x[5],
    "baseCost": x[6],
    "costbyOne": x[7],
    "qtyRejected": x[8]
    }

def getMaterialDatabyPattern(x):
    return {
    "mergedResponse": x[0],
    "baseQty": x[1],
    "baseCost": x[2],
    "und": x[3],
    "type": [4]
    }

def getMaterialData(x):
    return {
    "code": x[0],
    "itemName": x[1],
    "baseQty": x[2],
    "baseCost": x[3],
    "und": x[4],
    "type": x[5],
    "isSelected": False,
    }





