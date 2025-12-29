from fastapi import FastAPI, UploadFile, File
from pydantic import BaseModel
from contextlib import asynccontextmanager
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from app.database import init_qdrant, create_document_collection, reserch_similar_chunks
from app.tasks.tasks_parsing import generate_embedding, celery_app

class SearchRequest(BaseModel):
    text: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_qdrant()
    create_document_collection()
    print("✅ FastAPI startup completed")
    yield
    print("🛑 FastAPI shutdown")


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.get("/")
async def root():
    return {
        "message": "Semantic Search API",
        "endpoints": [
            "/index - Main page",
            "/docs - Swagger UI",
            "/redoc - ReDoc",
            "/tasks/active - Active tasks",
            "/semantic - Semantic UI"
        ]
    }

@app.get("/semantic", response_class=HTMLResponse)
async def css_styles():
    return Path("server/semantic.html").read_text(encoding="utf-8")

@app.get("/index", response_class=HTMLResponse)
async def index():
    return Path("server/index.html").read_text(encoding="utf-8")

@app.post("/select-file")
async def select_file(file: list[UploadFile] = File(...)):
    task_ids = []
    uploaded_files = []
    total_size = 0
    
    for only_file in file:
        contents: bytes = await only_file.read()
        file_size = len(contents)
        total_size += file_size

        uploads_dir = Path("uploads")
        uploads_dir.mkdir(exist_ok=True)
        save_path = Path(f"{uploads_dir}/{only_file.filename}")
        save_path.write_bytes(contents)
        
        task = generate_embedding.delay(only_file.filename)
        task_ids.append(task.id)
        
        uploaded_files.append({
            "filename": only_file.filename,
            "size": file_size,
            "content_type": only_file.content_type,
            "task_id": task.id
        })

        print(f"Создана задача {task.id} для файла {only_file.filename}")

    return {
        "status": "accepted",
        "message": f"Принято {len(file)} файл(ов) в обработку",
        "files": uploaded_files,
        "task_ids": task_ids,
        "total_size": total_size,
        "count": len(file)
    }

@app.post("/select-folder")
async def select_file(file: list[UploadFile] = File(...)):
    task_ids = []
    uploaded_files = []
    total_size = 0
    
    for only_file in file:
        contents: bytes = await only_file.read()
        file_size = len(contents)
        total_size += file_size

        uploads_dir = Path("uploads")
        uploads_dir.mkdir(exist_ok=True)
        save_path = Path(f"{uploads_dir}/{only_file.filename}")
        save_path.write_bytes(contents)
        
        task = generate_embedding.delay(only_file.filename)
        task_ids.append(task.id)
        
        uploaded_files.append({
            "filename": only_file.filename,
            "size": file_size,
            "content_type": only_file.content_type,
            "task_id": task.id
        })

        print(f"Создана задача {task.id} для файла {only_file.filename}")

    return {
        "status": "accepted",
        "message": f"Принято {len(file)} файл(ов) в обработку",
        "files": uploaded_files,
        "task_ids": task_ids,
        "total_size": total_size,
        "count": len(file)
    }

@app.post("/message")
async def message_input(request: SearchRequest):
    """Поиск по семантическому запросу"""
    
    if not request.text or not request.text.strip():
        return {
            "status": "error",
            "message": "Текст запроса не может быть пустым",
            "results": []
        }
    
    try:
        search_result = reserch_similar_chunks(request.text)
        
        if not search_result or len(search_result) == 0:
            return {
                "status": "no_results",
                "message": "По вашему запросу ничего не найдено",
                "results": []
            }
        
        top_results = search_result[:5]
        formatted_results = []
        
        for i, result in enumerate(top_results, 1):
            metadata = result.get('metadata', {})
            
            formatted_results.append({
                "rank": i,
                "id": result.get('id', f'result_{i}'),
                "score": result['score'] * 100,
                "text": result['text'],
                "file_name": metadata.get('file_name', 'Неизвестно'),
                "file_path": metadata.get('file_path', 'Неизвестно'),
                "file_extension": metadata.get('file_extension', 'Неизвестно'),
                "chunk_index": metadata.get('chunk_index', 0)
            })
        
        return {
            "status": "success",
            "message": f"Найдено результатов: {len(search_result)}",
            "count": len(formatted_results),
            "results": formatted_results
        }
        
    except Exception as e:
        print(f"Ошибка поиска: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "status": "error",
            "message": f"Ошибка при выполнении поиска: {str(e)}",
            "results": []
        }


@app.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """Проверка статуса задачи по ID"""
    
    task = celery_app.AsyncResult(task_id)
    
    if task.state == 'PENDING':
        return {
            "task_id": task_id,
            "status": "pending",
            "progress": 0,
            "message": "Задача в очереди..."
        }
    
    elif task.state == 'PROGRESS':
        info = task.info or {}
        return {
            "task_id": task_id,
            "status": "processing",
            "progress": info.get('progress', 0),
            "current_step": info.get('current_step', 0),
            "total_steps": info.get('total_steps', 5),
            "message": info.get('status', 'Обработка...'),
            "filename": info.get('filename', '')
        }
    
    elif task.state == 'SUCCESS':
        return {
            "task_id": task_id,
            "status": "completed",
            "progress": 100,
            "result": task.result,
            "message": "Обработка завершена"
        }
    
    elif task.state == 'FAILURE':
        return {
            "task_id": task_id,
            "status": "failed",
            "progress": 0,
            "error": str(task.info),
            "message": "Ошибка обработки"
        }
    
    else:
        return {
            "task_id": task_id,
            "status": task.state.lower(),
            "message": str(task.info)
        }


@app.get("/tasks/active")
async def get_active_tasks():
    """Получить список активных задач"""
    inspect = celery_app.control.inspect()
    active_tasks = inspect.active()
    
    if not active_tasks:
        return {"active_tasks": [], "count": 0}
    
    tasks_list = []
    for worker, tasks in active_tasks.items():
        for task in tasks:
            tasks_list.append({
                "task_id": task.get('id'),
                "name": task.get('name'),
                "worker": worker
            })
    
    return {
        "active_tasks": tasks_list,
        "count": len(tasks_list)
    }