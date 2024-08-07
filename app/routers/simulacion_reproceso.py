import sys
from fastapi import APIRouter, Request
from pydantic import BaseModel
sys.path.insert(1, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/herramientas')
sys.path.insert(2, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/data')
from app.herramientas.connection import dbConnection

class Simulacion(BaseModel):
    ItemCode: str
    ProdName: str
    Type: str | None = 'P'
    PlannedQty: int
    SimDate: str
    PostDate: str | None = None
    DueDate: str | None = None
    StartDate: str | None = None
    Warehouse: str
    OriginType: str | None = 'M'
    PrcCode: str
    Usuario_Codigo_SIM: str
    Usuario_Codigo_VAL: str | None = None
    cod_motivo_causa: int | None = None
    isChecked: bool | None = False

simulacion_router = APIRouter(
    prefix='/reproceso/simulacion'
)

dbobj = dbConnection()

@simulacion_router.post("/")
async def post_simulacion(simulacion: Simulacion):
    isOk, response = dbobj.postSimulation(simulacion)
    return {
        "status": isOk,
        "response": response
    }

@simulacion_router.get("/")
async def get_simulation_by_userid(usuarioid: str = 'XXXX'):
    #usuario XXXX para obtener todo
    response = dbobj.get_whole_simulations(usuarioid)
    format_response = list(map(format_response_,response))
    return format_response

def format_response_(x):
    return {
        "SimId": x[0],
        "ItemCode": x[1],
        "ProdName": x[2],
        "PlannedQty": x[3],
        "SimDate": x[4],
        "PostDate": x[5],
        "DueDate": x[6],
        "StartDate": x[7],
        "Warehouse": x[8],
        "Usuario_Nombre": x[9],
        "Motivo_causa": x[10],
        "isOk": x[11],
    }
