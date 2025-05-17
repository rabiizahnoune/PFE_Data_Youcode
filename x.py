import websocket
import json

def on_message(ws, message):
    print("Received:", message)

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected")
    ws.send(json.dumps({
        'type': 'subscribe',
        'symbol': 'GLD'  # ETF qui reflète le prix de l'or
    }))

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=d0362tpr01qvvb92ethgd0362tpr01qvvb92eti0",
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()



# streamlit run gold_trading_dashboard.py --server.port=8050 --server.address=0.0.0.0