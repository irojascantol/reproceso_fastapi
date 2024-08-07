import sys
import pyodbc
import json
from fastapi import APIRouter, Request, Response
from fastapi.responses import PlainTextResponse
from fastapi.encoders import jsonable_encoder
sys.path.insert(1, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/herramientas')
sys.path.insert(2, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/data')
from app.herramientas.connection import sapConnection

almacen_router = APIRouter(
    prefix='/reproceso/almacen'
)
sapobj = sapConnection()


@almacen_router.get("/subarea/")
async def getWareHousebyArea(odSubArea: str = 'PINTURA'):
    odSubArea = 'CALIDAD' if 'CONTROL DE CALIDAD' in odSubArea else odSubArea
    data = sapobj.get_warehouse_by_subarea(odSubArea)
    newData = list(map(get_warehouse_subarea, enumerate(data)))
    return (
        {
        "data": newData
        }
    )

def get_warehouse_subarea(data):
    index, value = data
    return {
    "WareCode": value[0],
    "WareName": value[1].replace('ALMACEN DE','').strip() if 'ALMACEN DE' in value[1] else value[1].replace('ALMACEN','').strip(),
    }