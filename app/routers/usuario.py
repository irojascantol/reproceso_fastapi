import sys
from fastapi import APIRouter, Request
from pydantic import BaseModel
sys.path.insert(1, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/herramientas')
sys.path.insert(2, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/data')
from app.herramientas.connection import dbConnection

usuario_router = APIRouter(
    prefix='/reproceso/usuario'
)

dbobj = dbConnection()

@usuario_router.get("/validarol")
async def validateRol(usuarioid: str = 'XXXX', rol_id: int = 4):
    isOk = dbobj.validateRol(usuarioid,rol_id)
    returnedValue = (True if not(isOk is None) else False)
    return {
        "data": returnedValue,
    }


#aqui tiene que traer la lista de simulaciones que le pertenecen
# def get_format_cost_center(x):





