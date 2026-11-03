import asyncio
import json
import os
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import networkx as nx
import uvicorn
from contextlib import asynccontextmanager

# ファイル監視用ライブラリのインポート
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# --- 設定 ---
DATA_FILE = "graph_data.json"
connected_clients: set[WebSocket] = set()
G = nx.Graph()
COLORS = ["#57a3f3", "#f35757", "#57f37d", "#f3f357", "#b557f3"]

# ファイル書き込み中による読み込み競合を防ぐロック
file_lock = asyncio.Lock()


def load_graph_from_file():
    """外部のJSONファイルからグラフ構造、ノード位置、ラベルを読み込む"""
    global G
    G.clear()
    
    if not os.path.exists(DATA_FILE):
        print(f"[{DATA_FILE}] が見つからないため初期生成します。")
        initial_G = nx.erdos_renyi_graph(n=5, p=0.4)
        for node in initial_G.nodes():
            G.add_node(int(node), label=f"Node {node}", x=random.randint(-200, 200), y=random.randint(-200, 200))
        for u, v in initial_G.edges():
            G.add_edge(int(u), int(v))
        save_graph_to_file()
        return

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        for node_data in data.get("nodes", []):
            node_id = int(node_data["id"])
            # ファイルに "label" が指定されていればそれを使い、なければデフォルトを割り当て
            label = node_data.get("label", f"Node {node_id}")
            G.add_node(node_id, label=label, x=node_data.get("x", 0), y=node_data.get("y", 0))
            
        for edge_data in data.get("edges", []):
            G.add_edge(int(edge_data["from"]), int(edge_data["to"]))
            
        print(f"[{DATA_FILE}] からデータを再読み込みしました。")
    except Exception as e:
        print(f"ファイル読み込みエラー: {e}")


def save_graph_to_file():
    """現在のグラフ構造をJSONへ永続化"""
    nodes = []
    for node in G.nodes():
        nodes.append({
            "id": int(node),
            "label": G.nodes[node].get("label", f"Node {node}"),
            "x": G.nodes[node].get("x", 0),
            "y": G.nodes[node].get("y", 0)
        })
    edges = [{"from": int(u), "to": int(v)} for u, v in G.edges()]
    
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump({"nodes": nodes, "edges": edges}, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"ファイル保存エラー: {e}")


def get_graph_data():
    """Vis.js形式へのデータ変換"""
    nodes = []
    for node in G.nodes():
        if 'color' not in G.nodes[node]:
            G.nodes[node]['color'] = random.choice(COLORS)
        nodes.append({
            "id": int(node),
            "label": G.nodes[node].get("label", f"Node {node}"), # 外部ファイルのカスタムラベルを反映
            "x": G.nodes[node]['x'],
            "y": G.nodes[node]['y'],
            "color": G.nodes[node]['color']
        })
    edges = []
    for u, v in G.edges():
        if 'color' not in G.edges[u, v]:
            G.edges[u, v]['color'] = "#848484"
        edges.append({
            "from": int(u),
            "to": int(v),
            "color": {"color": G.edges[u, v]['color']}
        })
    return {"nodes": nodes, "edges": edges}


async def broadcast_graph_data():
    if not connected_clients:
        return
    data = json.dumps(get_graph_data())
    await asyncio.gather(
        *[client.send_text(data) for client in connected_clients],
        return_exceptions=True
    )


# --- ファイル変更の検知イベントハンドラー ---
class GraphFileHandler(FileSystemEventHandler):
    def __init__(self, loop):
        self.loop = loop

    def on_modified(self, event):
        # 指定したJSONファイルが変更された場合のみ処理
        if event.src_path.endswith(DATA_FILE):
            # メインの非同期ループにタスクをスケジュールする
            asyncio.run_coroutine_threadsafe(self.reload_and_broadcast(), self.loop)

    async def reload_and_broadcast(self):
        async with file_lock:
            # ドラッグ直後の自己保存による無限ループを避けるため一瞬だけ待つ
            await asyncio.sleep(0.5) 
            load_graph_from_file()
            await broadcast_graph_data()


async def background_color_updater():
    try:
        while True:
            await asyncio.sleep(3)
            for node in G.nodes():
                G.nodes[node]['color'] = random.choice(COLORS)
            for u, v in G.edges():
                G.edges[u, v]['color'] = random.choice(["#848484", "#ff4d4d", "#1e90ff"])
            await broadcast_graph_data()
    except asyncio.CancelledError:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_graph_from_file()
    
    # ファイル監視(watchdog)の起動
    loop = asyncio.get_running_loop()
    event_handler = GraphFileHandler(loop)
    observer = Observer()
    observer.schedule(event_handler, path=".", recursive=False)
    observer.start()
    
    updater_task = asyncio.create_task(background_color_updater())
    yield
    updater_task.cancel()
    observer.stop()
    observer.join()


app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    await websocket.send_text(json.dumps(get_graph_data()))
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "node_dragged":
                node_id = message["id"]
                if node_id in G:
                    G.nodes[node_id]['x'] = message["x"]
                    G.nodes[node_id]['y'] = message["y"]
                    
                    # ドラッグ時はファイル監視の重複反応を防ぐロックを取得して保存
                    async with file_lock:
                        save_graph_to_file()
                        
                    await broadcast_graph_data()
    except WebSocketDisconnect:
        pass
    finally:
        connected_clients.remove(websocket)


def main():
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)


if __name__ == "__main__":
    main()
