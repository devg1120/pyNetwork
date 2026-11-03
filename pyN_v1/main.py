import asyncio
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import networkx as nx
import uvicorn

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# NetworkXを使ってグラフデータをVis.jsが読めるフォーマットに変換する関数
def get_graph_data():
    G = nx.erdos_renyi_graph(n=10, p=0.3)
    nodes = [{"id": int(node), "label": f"Node {node}"} for node in G.nodes()]
    edges = [{"from": int(u), "to": int(v)} for u, v in G.edges()]
    return {"nodes": nodes, "edges": edges}

# 初期画面をJinja2でレンダリング（修正箇所）
@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    # 最新の FastAPI/Starlette 仕様に合わせた安全な記述
    return templates.TemplateResponse(request, "index.html")

# WebSocketエンドポイント
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            graph_data = get_graph_data()
            await websocket.send_text(json.dumps(graph_data))
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        print("Client disconnected")

def main():
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)

if __name__ == "__main__":
    main()

