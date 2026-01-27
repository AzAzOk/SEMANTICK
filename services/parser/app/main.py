from fastapi import FastAPI

app = FastAPI(title="Parser Service")

@app.get("/health")
def health():
    return {"status": "ok"}
