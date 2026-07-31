"""
测试推荐系统多Agent工作流
使用方法：
1. 先启动 SASRec 推荐服务: cd ../sasrec_api && python api_server.py
2. 再启动推荐服务: python app.py
3. 运行此测试: python test_recommendation.py
"""

import requests
import json

FLASK_URL = "http://127.0.0.1:5001"
SASREC_URL = "http://127.0.0.1:8000"

def check_sasrec_service():
    """检查SASRec服务是否可用"""
    print("检查SASRec推荐服务...")
    try:
        r = requests.get(f"{SASREC_URL}/health", timeout=5)
        if r.status_code == 200:
            data = r.json()
            print(f"  状态: {data.get('status')}")
            print(f"  模型已加载: {data.get('model_loaded')}")
            return data.get('model_loaded', False)
        else:
            print(f"  服务响应异常: {r.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("  无法连接到SASRec服务")
        print("  请先运行: cd ../sasrec_api && python api_server.py")
        return False
    except Exception as e:
        print(f"  检查失败: {e}")
        return False

def check_flask_service():
    """检查Flask服务是否可用"""
    print("\n检查Flask服务...")
    try:
        r = requests.get(f"{FLASK_URL}/recommend/health", timeout=5)
        if r.status_code == 200:
            data = r.json()
            print(f"  推荐系统状态: {data.get('recommendation_system')}")
            print(f"  SASRec服务状态: {data.get('sasrec_service')}")
            return True
        else:
            print(f"  服务响应异常: {r.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("  无法连接到Flask服务")
        print("  请先运行: python app.py")
        return False
    except Exception as e:
        print(f"  检查失败: {e}")
        return False

def test_recommendation():
    """测试推荐接口"""
    print("\n" + "="*60)
    print("测试推荐多Agent系统")
    print("="*60)
    
    test_payload = {
        "item_sequence": [
            "015600206X",
            "6300215695",
            "0446673145"
        ],
        "top_k": 5
    }
    
    print(f"\n请求数据:")
    print(f"  商品序列: {test_payload['item_sequence']}")
    print(f"  推荐数量: {test_payload['top_k']}")
    
    print("\n发送推荐请求...")
    try:
        r = requests.post(
            f"{FLASK_URL}/recommend",
            json=test_payload,
            timeout=120
        )
        
        print(f"响应状态码: {r.status_code}")
        
        if r.status_code == 200:
            data = r.json()
            print("\n推荐结果:")
            print("-"*40)
            
            if data.get('success'):
                rec = data.get('recommendation', {})
                print(f"  推荐商品ID: {rec.get('recommended_product')}")
                print(f"  商品标题: {rec.get('product_title')}")
                print(f"  推荐置信度: {rec.get('confidence')}")
                print(f"\n  推荐原因:")
                print(f"    {rec.get('recommendation_reason')}")
                
                print("\n对话记录:")
                print("-"*40)
                conversation = data.get('conversation', {})
                for agent, content in conversation.items():
                    print(f"\n  [{agent}]:")
                    content_preview = content[:200] + "..." if len(content) > 200 else content
                    print(f"    {content_preview}")
            else:
                print(f"  推荐失败: {data.get('message')}")
        else:
            print(f"请求失败: {r.text}")
            
    except requests.exceptions.Timeout:
        print("请求超时，推荐过程可能需要较长时间")
    except Exception as e:
        print(f"测试失败: {e}")

def main():
    print("="*60)
    print("商品推荐多Agent系统测试")
    print("="*60)
    
    sasrec_ok = check_sasrec_service()
    if not sasrec_ok:
        print("\n⚠️  SASRec服务未就绪，推荐功能可能不可用")
    
    flask_ok = check_flask_service()
    if not flask_ok:
        print("\n❌ Flask服务未启动，无法进行测试")
        return
    
    if sasrec_ok:
        test_recommendation()
    else:
        print("\n跳过推荐测试（SASRec服务未就绪）")

if __name__ == "__main__":
    main()
