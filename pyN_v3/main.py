import asyncio
import json
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import networkx as nx
import uvicorn
from contextlib import asynccontextmanager

# --- 接続中の全クライアントを管理するセット ---
connected_clients: set[WebSocket] = set()

# --- サーバー側でグラフの状態を永続的に保持する ---
G = nx.erdos_renyi_graph(n=10, p=0.3)

# 初期位置（座標）をランダムに割り当て
for node in G.nodes():
    G.nodes[node]['x'] = random.randint(-300, 300)
    G.nodes[node]['y'] = random.randint(-300, 300)

COLORS = ["#57a3f3", "#f35757", "#57f37d", "#f3f357", "#b557f3"]
for node in G.nodes():
    G.nodes[node]['color'] = COLORS

for u, v in G.edges():
    G.edges[u, v]['color'] = "#848484"


def get_graph_data():
    """現在のNetworkXグラフの状態（位置・カラー）をVis.jsフォーマットで取得"""
    nodes = []
    for node in G.nodes():
        nodes.append({
            "id": int(node),
            "label": f"Node {node}",
            "x": G.nodes[node]['x'],
            "y": G.nodes[node]['y'],
            "color": G.nodes[node]['color']
        })
    
    edges = []
    for u, v in G.edges():
        edges.append({
            "from": int(u),
            "to": int(v),
            "color": {"color": G.edges[u, v]['color']}
        })
    
    return {"nodes": nodes, "edges": edges}


def update_statuses():
    """定期的にノードとエッジのカラー（ステータス）を変更する関数"""
    for node in G.nodes():
        G.nodes[node]['color'] = random.choice(COLORS)
    
    edge_colors = ["#848484", "#ff4d4d", "#1e90ff"]
    for u, v in G.edges():
        G.edges[u, v]['color'] = random.choice(edge_colors)


async def broadcast_graph_data():
    """現在接続されているすべてのクライアントへ一斉に最新データを送信する"""
    if not connected_clients:
        return
    
    data = json.dumps(get_graph_data())
    # 接続中の全クライアントに送信（切断検知時は安全に無視する）
    await asyncio.gather(
        *[client.send_text(data) for client in connected_clients],
        return_exceptions=True
    )


async def background_color_updater():
    """バックグラウンドで一意に動き続ける、3秒ごとのカラー更新・一斉配信ループ"""
    try:
        while True:
            await asyncio.sleep(3)
            # 1. サーバーが管理するグラフのカラーを更新
            update_statuses()
            # 2. 全クライアントにブロードキャスト
            await broadcast_graph_data()
    except asyncio.CancelledError:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # アプリ起動時にバックグラウンドタスクを1つだけ開始
    updater_task = asyncio.create_task(background_color_updater())
    yield
    # アプリ終了時にタスクを破棄
    updater_task.cancel()

# lifespanをFastAPIに登録
app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    # クライアントを管理リストに追加
    connected_clients.add(websocket)
    
    # 接続した瞬間に、現在の最新のグラフ状態を一度すぐに送る
    await websocket.send_text(json.dumps(get_graph_data()))
    
    try:
        while True:
            # クライアントからの位置情報送信を待機
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message.get("type") == "node_dragged":
                node_id = message["id"]
                if node_id in G:
                    G.nodes[node_id]['x'] = message["x"]
                    G.nodes[node_id]['y'] = message["y"]
                    print(f"Node {node_id} moved to: x={message['x']}, y={message['y']}")
                    
                    # 【重要】誰かがノードを動かしたら、その瞬間に最新座標を全員に即時ブロードキャストする
                    await broadcast_graph_data()
                    
    except WebSocketDisconnect:
        print("Client disconnected")
    finally:
        # 切断されたら管理リストから除外
        connected_clients.remove(websocket)


def main():
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)


if __name__ == "__main__":
    main()
