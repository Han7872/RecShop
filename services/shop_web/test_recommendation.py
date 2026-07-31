"""
测试 SASRec 推荐 API
为用户 user_demo_001 获取推荐结果
"""
import requests
import json
import sys
import io

# 设置输出编码为 UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 配置
BACKEND_API_URL = 'http://127.0.0.1:5000'
USER_TOKEN = 'user_demo_001'

def test_recommendation():
    """测试推荐功能"""
    print("=" * 80)
    print("测试 SASRec 推荐 API")
    print("=" * 80)
    print(f"用户: {USER_TOKEN}")
    print(f"后端 API: {BACKEND_API_URL}")
    print("-" * 80)
    
    # 1. 检查用户是否存在
    print("\n1. 检查用户信息...")
    try:
        response = requests.get(f"{BACKEND_API_URL}/api/users/{USER_TOKEN}")
        if response.status_code == 200:
            user = response.json()
            print(f"   [OK] 用户存在: {user.get('username', 'N/A')}")
        else:
            print(f"   [FAIL] 用户不存在: {response.text}")
            return
    except requests.exceptions.ConnectionError:
        print(f"   [FAIL] 无法连接到后端 API")
        print(f"   提示: 请先启动后端服务 (backend_api/app.py)")
        return
    except Exception as e:
        print(f"   [FAIL] 请求失败: {e}")
        return
    
    # 2. 检查用户历史交互
    print("\n2. 检查用户历史交互...")
    try:
        response = requests.get(f"{BACKEND_API_URL}/api/users/{USER_TOKEN}/history?limit=10")
        if response.status_code == 200:
            history = response.json()
            print(f"   [OK] 历史记录数: {len(history)}")
            if history:
                print(f"   最近交互的商品:")
                for i, item in enumerate(history[:5], 1):
                    print(f"     {i}. {item['item_id']} - {item['title'][:30]}... ({item['interaction_type']})")
            else:
                print(f"   [WARNING] 用户没有历史交互记录，无法生成推荐")
                return
        else:
            print(f"   [FAIL] 获取历史失败: {response.text}")
            return
    except Exception as e:
        print(f"   [FAIL] 请求失败: {e}")
        return
    
    # 3. 调用推荐 API
    print("\n3. 调用推荐 API...")
    try:
        payload = {
            'user_token': USER_TOKEN,
            'top_k': 10
        }
        print(f"   请求参数: {json.dumps(payload, ensure_ascii=False)}")
        
        response = requests.post(
            f"{BACKEND_API_URL}/api/recommend",
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            recommendations = result['recommendations']
            inference_time = result['inference_time']
            input_length = result['input_sequence_length']
            
            print(f"   [OK] 推荐成功!")
            print(f"   推理时间: {inference_time:.3f}s")
            print(f"   输入序列长度: {input_length}")
            print(f"   推荐商品数: {len(recommendations)}")
            print("\n" + "=" * 80)
            print("推荐结果:")
            print("=" * 80)
            
            for i, item in enumerate(recommendations, 1):
                print(f"\n{i}. [{item['item_id']}] {item['title']}")
                print(f"   分类: {item.get('category', 'N/A')}")
                print(f"   品牌: {item.get('brand', 'N/A')}")
                price = item.get('price')
                price_str = f"¥{float(price):.2f}" if price is not None else 'N/A'
                print(f"   价格: {price_str}")
                print(f"   推荐得分: {item['recommendation_score']:.4f}")
                print(f"   推荐排名: {item['recommendation_rank']}")
            
            print("\n" + "=" * 80)
            print("测试完成!")
            print("=" * 80)
            
            return True
            
        elif response.status_code == 400:
            error = response.json()
            print(f"   [FAIL] 请求错误: {error.get('error', 'Unknown error')}")
            return False
        elif response.status_code == 503:
            error = response.json()
            print(f"   [FAIL] 推荐服务不可用: {error.get('error', 'Service unavailable')}")
            print(f"   提示: 请确保 SASRec API (http://127.0.0.1:8000) 已启动")
            return False
        else:
            print(f"   [FAIL] 推荐失败: HTTP {response.status_code}")
            print(f"   响应: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"   [FAIL] 连接失败: 无法连接到后端 API ({BACKEND_API_URL})")
        print(f"   提示: 请确保后端服务已启动")
        return False
    except requests.exceptions.Timeout:
        print(f"   [FAIL] 请求超时: 推荐服务响应时间过长")
        return False
    except Exception as e:
        print(f"   [FAIL] 请求失败: {e}")
        return False

if __name__ == '__main__':
    test_recommendation()
