from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers.articulos_materiales import articulos_router
import uvicorn
from app.routers import reporte_rechazados, areas_router, almacen, simulacion_reproceso, usuario
 
origins = ["*"]


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(reporte_rechazados.router)
app.include_router(articulos_router)
app.include_router(areas_router.areas_router)
app.include_router(almacen.almacen_router)
app.include_router(simulacion_reproceso.simulacion_router)
app.include_router(usuario.usuario_router)
