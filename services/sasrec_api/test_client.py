import requests

API_URL = "http://127.0.0.1:8000"

def main():
    # 健康检查
    try:
        r = requests.get(f"{API_URL}/health")
        print("/health:", r.status_code, r.text)
    except Exception as e:
        print("健康检查失败:", e)
        return

    # 示例推荐请求（如有需要可替换为更常见的 ASIN）
    payload = {
        "item_sequence": [
            "015600206X",
            "6300215695",
            "0446673145"
        ],
        "top_k": 5,
        "exclude_history": True
    }

    try:
        r = requests.post(f"{API_URL}/recommend", json=payload)
        print("/recommend:", r.status_code)
        print(r.text)
    except Exception as e:
        print("推荐接口调用失败:", e)


if __name__ == "__main__":
    main()
