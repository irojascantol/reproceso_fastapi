import sys
import pyodbc
import json
from fastapi import APIRouter, Request, Response
from pydantic import BaseModel
from fastapi.responses import PlainTextResponse
from fastapi.encoders import jsonable_encoder
from app.herramientas.connection import sapConnection

areas_router = APIRouter(
    prefix='/reproceso/area'
)

sapobj = sapConnection()

@areas_router.get("/")
async def get_areas():
    data = sapobj.get_areas()
    newData = list(map(do_format_sa, enumerate(data)))
    return {
        "data": newData
    }

def do_format_sa(data):
    index, value = data
    if value[1] == 'FORJA Y TALADROS':
        return {
        "id": index,
        "prcCode": value[0],
        "subareaName": value[1],
        "members": ['2000103', '2000104'],
        }
    else:
        return {
        "id": index,
        "prcCode": value[0],
        "subareaName": value[1],
        "members": [value[0]],
        }





