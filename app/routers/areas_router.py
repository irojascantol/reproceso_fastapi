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
    newData = list(map(return_getArticulo, enumerate(data)))
    return {
        "data": newData
    }

def return_getArticulo(data):
    index, value = data
    return {
    "id": index,
    "prcCode": value[0],
    "subareaName": value[1],
    }





