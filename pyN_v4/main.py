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

# --- 設定 ---
DATA_FILE = "graph_data.json"
connected_clients: set[WebSocket] = set()
G = nx.Graph()

# 定期更新用の共通カラーリスト
COLORS = ["#57a3f3", "#f35757", "#57f37d", "#f3f357", "#b557f3"]


def load_graph_from_file():
    """外部のJSONファイルからグラフ構造とノード位置を読み込む関数"""
    global G
    G.clear()
    
    # ファイルが存在しない場合は、初期データ（ランダム）を作って保存する
    if not os.path.exists(DATA_FILE):
        print(f"[{DATA_FILE}] が見つからないため、初期データを自動生成します。")
        initial_G = nx.erdos_renyi_graph(n=10, p=0.3)
        
        for node in initial_G.nodes():
            G.add_node(int(node), x=random.randint(-300, 300), y=random.randint(-300, 300))
        for u, v in initial_G.edges():
            G.add_edge(int(u), int(v))
            
        save_graph_to_file()
        return

    # ファイルがある場合は読み込む
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        for node_data in data.get("nodes", []):
            node_id = int(node_data["id"])
            G.add_node(node_id, x=node_data.get("x", 0), y=node_data.get("y", 0))
            
        for edge_data in data.get("edges", []):
            G.add_edge(int(edge_data["from"]), int(edge_data["to"]))
            
        print(f"[{DATA_FILE}] からデータを正常に読み込みました。 (ノード: {G.number_of_nodes()}, エッジ: {G.number_of_edges()})")
    except Exception as e:
        print(f"ファイル読み込みエラー: {e}")


def save_graph_to_file():
    """現在のNetworkXグラフ構造とノード位置を外部JSONファイルに永続化する関数"""
    nodes = [{"id": int(node), "x": G.nodes[node].get("x", 0), "y": G.nodes[node].get("y", 0)} for node in G.nodes()]
    edges = [{"from": int(u), "to": int(v)} for u, v in G.edges()]
    
    data = {"nodes": nodes, "edges": edges}
    
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"ファイル保存エラー: {e}")


def get_graph_data():
    """現在のNetworkXグラフの状態（動的カラーを含む）をVis.jsフォーマットに変換"""
    nodes = []
    for node in G.nodes():
        # カラー属性がまだなければ初期値を割り当て
        if 'color' not in G.nodes[node]:
            G.nodes[node]['color'] = random.choice(COLORS)
            
        nodes.append({
            "id": int(node),
            "label": f"Node {node}",
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


def update_statuses():
    """定期的にノードとエッジのカラー（ステータス）を変更する（位置は触らない）"""
    for node in G.nodes():
        G.nodes[node]['color'] = random.choice(COLORS)
    
    edge_colors = ["#848484", "#ff4d4d", "#1e90ff"]
    for u, v in G.edges():
        G.edges[u, v]['color'] = random.choice(edge_colors)


async def broadcast_graph_data():
    if not connected_clients:
        return
    data = json.dumps(get_graph_data())
    await asyncio.gather(
        *[client.send_text(data) for client in connected_clients],
        return_exceptions=True
    )


async def background_color_updater():
    """3秒ごとのカラー更新配信ループ（ファイル書き込みは不要）"""
    try:
        while True:
            await asyncio.sleep(3)
            update_statuses()
            await broadcast_graph_data()
    except asyncio.CancelledError:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # サーバー起動時にファイルを読み込む
    load_graph_from_file()
    updater_task = asyncio.create_task(background_color_updater())
    yield
    updater_task.cancel()


app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    
    # 接続直後に最新状態を送信
    await websocket.send_text(json.dumps(get_graph_data()))
    
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "node_dragged":
                node_id = message["id"]
                if node_id in G:
                    # 1. 内部データ構造の座標を更新
                    G.nodes[node_id]['x'] = message["x"]
                    G.nodes[node_id]['y'] = message["y"]
                    
                    # 2. 外部ファイルへ永続化（即時書き込み）
                    save_graph_to_file()
                    
                    # 3. 他の全員に即座に新しい位置を同期
                    await broadcast_graph_data()
                    
    except WebSocketDisconnect:
        pass
    finally:
        connected_clients.remove(websocket)


def main():
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)


if __name__ == "__main__":
    main()
