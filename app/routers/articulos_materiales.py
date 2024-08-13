import sys
import json
from fastapi import APIRouter
from typing import List
from pydantic import BaseModel
sys.path.insert(1, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/herramientas')
sys.path.insert(2, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/data')
from app.herramientas.connection import sapConnection, dbConnection

class SaMembers(BaseModel):
    members: List[str]


articulos_router = APIRouter(
    prefix='/reproceso/articulo'
)
sapobj = sapConnection()
dbobj = dbConnection()

@articulos_router.post("/")
async def get_articulo(subarea: SaMembers):
    if bool(len(subarea.members)):
        data = sapobj.get_Articulo_Area(subarea.members)
        newData = list(map(return_getArticulo, enumerate(data)))
        return {
            "data": newData
        }
    else:
        return {
            "data": []
        }

def return_getArticulo(data):
    index, value = data
    return {
    "id": index,
    "itemId": value[0],
    "itemName": value[1],
    }

@articulos_router.get("/material_repro/")
async def getOdMaterials(odParameter: str = 'PP1117000001', subareaName: str = 'ENSAMBLE DE CERRADURAS', isAll: bool = True):
    data = sapobj.getMaterialsByAD(odParameter)
    newData = list(map(getMaterialDatabyOd, data))
    newData.sort(key=lambda x: (int(x['order'].split(".")[1])+int(x['order'].split(".")[0])))
    newData[0]["baseCost"] = newData[0]["costbyOne"]
    newData[0]["qtyRejected"] = newData[-1]["qtyRejected"]
    newData[0]["whName"] = newData[-1]["whName"]
    return ({"data": newData[0]} if not isAll else {"data": newData[1:]})

def getMaterialDatabyOd(x):
    return {
    "order": x[0],
    "type": x[1],
    "materialCode": x[2],
    "materialName": x[3],
    "und": x[4],
    "baseQty": x[5],
    "baseCost": x[6],
    "costbyOne": x[7],
    "qtyRejected": x[8],
    "whName": x[9],
    }

@articulos_router.post("/materials/")
async def getOdMaterials(prcCode: SaMembers):
    if bool(len(prcCode.members)):
        data = sapobj.get_material_subarea(prcCode.members)
        newData = getMaterialDatabySubArea(data)
        return {"data": newData}
    else:
        return {
            "data": []
        }

def getMaterialDatabySubArea(x):
    myDict = {}
    for material in x:
        if material[0] in myDict:
            myDict[material[0]]["baseQty"].append(material[2])
            myDict[material[0]]["baseCost"].append(material[3])
        else:
            myDict[material[0]] = {
                "order": "-",
                "type": material[5],
                "materialCode" : material[0],
                "materialName": material[1],
                "und": material[4],
                "baseQty": [material[2]],
                "baseCost": [material[3]],
            }
    return list(myDict.values())

@articulos_router.get("/materialsbyWare/")
# async def getMaterialfromWare(Ware: str = 'ALM03', offset: int = 0):
async def getMaterialfromWare(Ware: str = 'ALM03', searchPattern: str = '', offset: int = 0):
    data = sapobj.getMaterialfromWare(ware = Ware, searchPattern = searchPattern, offset = offset)
    newData = list(map(getMaterialDatabyWare,data))
    return ({"data": newData})

def getMaterialDatabyWare(x):
    return {
        "order": x[0],
        "type": x[1],
        "materialCode" : x[2],
        "materialName": x[3],
        "und": x[4],
        "baseQty": '' if x[5] is None else x[5],
        "baseCost": x[6],
    }

@articulos_router.get("/failarticles/")
async def getMaterialfromWare(odArticle: str = 'PP2219000012', subarea: str = 'TALADROS'):
    data = sapobj.get_fail_product_by_subarea(odArticle, subarea)
    newData = list(map(getFailStock,data))
    return ({"data": newData})

def getFailStock(x):
    return {
        "stock": x[0],
        "WareCod": x[1],
    }

@articulos_router.get("/motivo_causa/")
async def getMaterialfromWare(saPattern: str = ""):
    data = dbobj.get_motivo_causa_areas(saPattern)
    newData = list(map(get_motivo_causa_pair,data))
    return ({"data": newData})

def get_motivo_causa_pair(x):
    return {
        "cod_reason": x[0],
        "reason": x[1],
    }






