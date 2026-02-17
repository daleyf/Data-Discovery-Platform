from fastapi import FastAPI
from upload import router as upload_router
from browse import router as browse_router
from ui import router as ui_router
from db import init_db

app = FastAPI()

init_db()

app.include_router(upload_router)
app.include_router(browse_router)
app.include_router(ui_router)