from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles


app = FastAPI()
project_folder = Path(__file__).resolve().parent

app.mount("/static", StaticFiles(directory=project_folder/"static"), name="static")


@app.get("/")
def show_home_page():
    """Send the HTML page to the browser."""
    return FileResponse(project_folder / "index.html")


@app.post("/events/order")
def receive_order_event():
    """Receive and log an order event."""
    message = "Order event received"
    print(message, flush=True)
    return {"message": message}


@app.post("/events/navigation")
def receive_navigation_event():
    """Receive and log a navigation event."""
    message = "Navigation event received"
    print(message, flush=True)
    return {"message": message}
