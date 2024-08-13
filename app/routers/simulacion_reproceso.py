import sys
from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from typing import List
sys.path.insert(1, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/herramientas')
sys.path.insert(2, 'C:/Users/irojas/Desktop/Pruebas/repro_proyecto/repro_api/app/data')
from app.herramientas.connection import dbConnection
from app.herramientas.service_layer_sap import SLSConnection

# Noitificacion: im_Backflush
# Noitificacion: im_Manual

class ProductionOrderLine(BaseModel):
    ItemNo: str
    PlannedQuantity: float
    ProductionOrderIssueType: str | None = 'im_Manual'
    Warehouse: str
    DistributionRule: str | None = '20001'
    DistributionRule2: str
    ItemType: str
    StartDate: str
    EndDate: str
    ItemName: str
    baseCost: float

class Simulacion(BaseModel):
    ItemCode: str
    ProdName: str
    Type: str | None = 'P'
    PlannedQty: float
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
    ProductionOrderLines: List[ProductionOrderLine]

simulacion_router = APIRouter(
    prefix='/reproceso/simulacion'
)

dbObj = dbConnection()
slsObj = SLSConnection()


@simulacion_router.post("/")
async def post_simulation(simulacion: Simulacion):
    if (simulacion.isChecked):
        #AGREGA PRIMERO SAP
        status, SAPResponse = slsObj.Post_ProductionOrder(simulacion)
    else:
        status, SAPResponse = (200, {"DocumentNumber": "Pendiente de aprobación"})
    
    if (status == 201 or status == 200):
        #AGREGA SEGUNDO EN SWC DATA BASE
        status_swc, SWC_ID_OBJ = dbObj.postSimulation(simulacion, str(SAPResponse["DocumentNumber"]), simulacion.isChecked)
        if (status_swc == 201 or status_swc == 200):
            status_response =  201 if (status == 201 and status_swc==201) else (200 if (status != 201 and status_swc==201) else 500)
            raise HTTPException(status_code=status_response, detail= {
                    "DocumentNumber": SAPResponse["DocumentNumber"],
                    "SWC_ID": SWC_ID_OBJ["SWC_ID"]
                })
        else:
            raise HTTPException(status_code=500, detail= {
                "DocumentNumber": "XXXXXXXX",
                "SWC_ID": "XXXXXXXX"
                })
    else:
        raise HTTPException(status_code=500, detail= {
        "DocumentNumber": "XXXXXXXX",
        "SWC_ID": "XXXXXXXX"
        })

@simulacion_router.get("/")
async def get_simulation_by_userid(usuarioid: str = 'XXXX'):
    #usuario XXXX para obtener todo
    response = dbObj.get_whole_simulations(usuarioid)
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
        "DocumentNumber": x[12],
        "initialCost": int(x[13]),
        "additionalCost": int(x[14]),
        "finalCost": int(x[13] + x[14]) if (x[13] is not None and x[14] is not None) else None,
        "percentage": int(100 * float(x[14])/float(x[13])) if (x[13] is not None and x[14] is not None) else None,
    }

@simulacion_router.put("/validar")
async def validate_sim(usuarioid: str = 'XXXX', id_cab: int = ""):
    db_model, status = dbObj.get_database_model(id_cab)
    if status:
        status, SAPResponse = slsObj.Post_ProductionOrder(db_model)
        if status == 201:
            response_set_validate_sim = dbObj.set_validate_sim(usuarioid, id_cab, SAPResponse["DocumentNumber"])
            if response_set_validate_sim:
                raise HTTPException(status_code=201, detail= {
                "DocumentNumber": SAPResponse["DocumentNumber"]
                })
            else:
                raise HTTPException(status_code=500, detail= {
                "DocumentNumber": "XXXXXXXX"
                })
        else:
            raise HTTPException(status_code=500, detail= {
                "DocumentNumber": "XXXXXXXX"
                })
    else:
        raise HTTPException(status_code=201, detail= {
                "DocumentNumber": "XXXXXXXX"
                })
    raise HTTPException(status_code=200, detail= {"message": "This method is not built yet"})
