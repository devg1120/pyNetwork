import asyncio
import json
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import networkx as nx
import uvicorn

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# --- サーバー側でグラフの状態を永続的に保持する ---
# 初期グラフの生成 (ノード数10、エッジ確率0.3)
G = nx.erdos_renyi_graph(n=10, p=0.3)

# 1. 初期位置（座標）をランダムに割り当て、固定値として保持する
# Vis.jsの座標系に合わせて適度に散らす（例: -300 〜 300）
for node in G.nodes():
    G.nodes[node]['x'] = random.randint(-300, 300)
    G.nodes[node]['y'] = random.randint(-300, 300)

# 2. 初期カラーの設定
COLORS = ["#57a3f3", "#f35757", "#57f37d", "#f3f357", "#b557f3"]
for node in G.nodes():
    G.nodes[node]['color'] = COLORS[0]

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
            "color": {"color": G.edges[u, v]['color']}  # Vis.jsのエッジカラー指定フォーマット
        })
    
    return {"nodes": nodes, "edges": edges}


def update_statuses():
    """定期的にノードとエッジのカラー（ステータス）を変更する関数"""
    # ノードカラーをランダムに変更
    for node in G.nodes():
        G.nodes[node]['color'] = random.choice(COLORS)
    
    # エッジカラーをランダムに変更（通常色、赤、青）
    edge_colors = ["#848484", "#ff4d4d", "#1e90ff"]
    for u, v in G.edges():
        G.edges[u, v]['color'] = random.choice(edge_colors)


@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    # クライアントからの位置情報受信タスク
    async def receive_positions():
        try:
            while True:
                data = await websocket.receive_text()
                message = json.loads(data)
                
                # クライアント側からドラッグ完了時の位置情報が届いた場合
                if message.get("type") == "node_dragged":
                    node_id = message["id"]
                    # サーバー側のNetworkXに位置を記憶させる
                    if node_id in G:
                        G.nodes[node_id]['x'] = message["x"]
                        G.nodes[node_id]['y'] = message["y"]
                        print(f"Node {node_id} moved to: x={message['x']}, y={message['y']}")
        except WebSocketDisconnect:
            pass
        except Exception as e:
            print(f"Receive error: {e}")

    # クライアントへの定期データ送信タスク
    async def send_updates():
        try:
            while True:
                # 1. 状態（カラー）を更新
                update_statuses()
                # 2. 現在の全データを取得して送信
                graph_data = get_graph_data()
                await websocket.send_text(json.dumps(graph_data))
                # 3. 3秒待機
                await asyncio.sleep(3)
        except WebSocketDisconnect:
            pass
        except Exception as e:
            print(f"Send error: {e}")

    # 受信と送信の非同期ループを並行して走らせる
    await asyncio.gather(receive_positions(), send_updates())


def main():
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)


if __name__ == "__main__":
    main()
