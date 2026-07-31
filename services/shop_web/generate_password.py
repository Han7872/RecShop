import os
from pathlib import Path
from dotenv import load_dotenv

# 加载项目根目录 .env 文件
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_PROJECT_ROOT / '.env', override=False)

from app import create_app
from werkzeug.security import generate_password_hash

app = create_app()

with app.app_context():
    password = "YOUR-PASSWORD-HERE"
    hash_value = generate_password_hash(password)
    
    print("=" * 80)
    print("密码哈希生成器")
    print("=" * 80)
    print(f"原始密码: {password}")
    print(f"哈希值: {hash_value}")
    print(f"哈希长度: {len(hash_value)} 字符")
    print("=" * 80)
    print("\n使用以下 SQL 更新数据库:")
    print(f"UPDATE users SET password_hash = '{hash_value}' WHERE email = 'demo@shopweb.com';")
    print("=" * 80)
