"""
从 RecBole 数据集导入数据到 MySQL 数据库
用于 Demo 网站展示
"""
import mysql.connector
import gzip
import json
import os
from tqdm import tqdm
import argparse

class DataImporter:
    """数据导入器"""
    
    def __init__(self, db_config):
        """初始化数据库连接"""
        self.conn = mysql.connector.connect(**db_config)
        self.cursor = self.conn.cursor()
        
    def __del__(self):
        """关闭数据库连接"""
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def import_items(self, item_file, meta_file=None, limit=None):
        """
        导入商品数据
        
        Args:
            item_file: electronics.item 文件路径
            meta_file: meta_Electronics.jsonl.gz 文件路径（可选，用于获取图片等额外信息）
            limit: 限制导入数量（用于测试）
        """
        print("=" * 60)
        print("导入商品数据...")
        print("=" * 60)
        
        # 先从 meta 文件构建商品元数据映射
        meta_dict = {}
        if meta_file and os.path.exists(meta_file):
            print(f"读取商品元数据: {meta_file}")
            with gzip.open(meta_file, 'rt', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if limit and i >= limit * 2:  # 读取更多以确保覆盖
                        break
                    try:
                        item = json.loads(line)
                        asin = item.get('asin') or item.get('parent_asin')
                        if asin:
                            meta_dict[asin] = {
                                'image_url': item.get('images', [{}])[0].get('large') if item.get('images') else None,
                                'description': item.get('description', [''])[0] if item.get('description') else None,
                                'rating': item.get('average_rating'),
                                'review_count': item.get('rating_number', 0)
                            }
                    except:
                        continue
            print(f"加载了 {len(meta_dict)} 个商品的元数据")
        
        # 读取 .item 文件并导入
        print(f"读取商品文件: {item_file}")
        imported = 0
        skipped = 0
        
        with open(item_file, 'r', encoding='utf-8') as f:
            # 跳过表头
            header = f.readline()
            
            for line in tqdm(f, desc="导入商品"):
                if limit and imported >= limit:
                    break
                
                try:
                    parts = line.strip().split('\t')
                    if len(parts) < 2:
                        continue
                    
                    item_id = parts[0]
                    title = parts[1] if len(parts) > 1 else f'Product {item_id}'
                    category = parts[2] if len(parts) > 2 else 'Electronics'
                    brand = parts[3] if len(parts) > 3 else None
                    price_str = parts[4] if len(parts) > 4 else None
                    
                    # 解析价格
                    price = None
                    if price_str and price_str not in ['N/A', '—', '']:
                        try:
                            price = float(price_str.replace('$', '').replace(',', ''))
                        except:
                            pass
                    
                    # 从 meta 获取额外信息
                    meta = meta_dict.get(item_id, {})
                    
                    # 插入数据库
                    sql = """
                        INSERT INTO items (item_id, title, category, brand, price, image_url, description, rating, review_count)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            title = VALUES(title),
                            category = VALUES(category),
                            brand = VALUES(brand),
                            price = VALUES(price),
                            image_url = VALUES(image_url),
                            description = VALUES(description),
                            rating = VALUES(rating),
                            review_count = VALUES(review_count)
                    """
                    
                    self.cursor.execute(sql, (
                        item_id,
                        title[:500],  # 限制长度
                        category[:200],
                        brand[:200] if brand else None,
                        price,
                        meta.get('image_url'),
                        meta.get('description'),
                        meta.get('rating'),
                        meta.get('review_count', 0)
                    ))
                    
                    imported += 1
                    
                    # 每1000条提交一次
                    if imported % 1000 == 0:
                        self.conn.commit()
                
                except Exception as e:
                    skipped += 1
                    if skipped <= 10:  # 只显示前10个错误
                        print(f"跳过商品 {item_id}: {e}")
        
        self.conn.commit()
        print(f"\n✅ 商品导入完成: 成功 {imported}, 跳过 {skipped}")
    
    def import_interactions(self, inter_file, limit=None, sample_users=None):
        """
        导入交互数据
        
        Args:
            inter_file: electronics.inter 文件路径
            limit: 限制导入数量
            sample_users: 只导入指定数量的用户数据（用于demo）
        """
        print("\n" + "=" * 60)
        print("导入交互数据...")
        print("=" * 60)
        
        imported = 0
        skipped = 0
        user_set = set()
        
        with open(inter_file, 'r', encoding='utf-8') as f:
            # 跳过表头
            header = f.readline()
            
            for line in tqdm(f, desc="导入交互"):
                if limit and imported >= limit:
                    break
                
                try:
                    parts = line.strip().split('\t')
                    if len(parts) < 3:
                        continue
                    
                    user_token = parts[0]
                    item_id = parts[1]
                    timestamp = int(float(parts[2]))
                    
                    # 如果限制用户数，检查是否已达到限制
                    if sample_users and len(user_set) >= sample_users and user_token not in user_set:
                        continue
                    
                    user_set.add(user_token)
                    
                    # 确保用户存在
                    self.cursor.execute(
                        "INSERT IGNORE INTO users (user_token, username) VALUES (%s, %s)",
                        (user_token, f"User_{len(user_set)}")
                    )
                    
                    # 插入交互记录
                    sql = """
                        INSERT INTO interactions (user_token, item_id, interaction_type, timestamp)
                        VALUES (%s, %s, %s, %s)
                    """
                    
                    self.cursor.execute(sql, (user_token, item_id, 'purchase', timestamp))
                    
                    imported += 1
                    
                    # 每1000条提交一次
                    if imported % 1000 == 0:
                        self.conn.commit()
                
                except Exception as e:
                    skipped += 1
                    if skipped <= 10:
                        print(f"跳过交互记录: {e}")
        
        self.conn.commit()
        print(f"\n✅ 交互导入完成: 成功 {imported}, 跳过 {skipped}")
        print(f"   涉及用户数: {len(user_set)}")
    
    def create_sample_data(self):
        """创建示例数据（用于快速测试）"""
        print("\n" + "=" * 60)
        print("创建示例数据...")
        print("=" * 60)
        
        # 创建示例用户
        sample_users = [
            ('demo_user_1', 'Alice', 'alice@example.com'),
            ('demo_user_2', 'Bob', 'bob@example.com'),
            ('demo_user_3', 'Charlie', 'charlie@example.com'),
        ]
        
        for user_token, username, email in sample_users:
            self.cursor.execute(
                "INSERT IGNORE INTO users (user_token, username, email) VALUES (%s, %s, %s)",
                (user_token, username, email)
            )
        
        self.conn.commit()
        print(f"✅ 创建了 {len(sample_users)} 个示例用户")

def main():
    parser = argparse.ArgumentParser(description="导入数据到 SASRec Demo 数据库")
    parser.add_argument("--host", default="localhost", help="数据库主机")
    parser.add_argument("--port", type=int, default=3306, help="数据库端口")
    parser.add_argument("--user", default="root", help="数据库用户名")
    parser.add_argument("--password", required=True, help="数据库密码")
    parser.add_argument("--database", default="shopify2", help="数据库名称")
    parser.add_argument("--item-file", default="../dataset/electronics/electronics.item", help=".item 文件路径")
    parser.add_argument("--inter-file", default="../dataset/electronics/electronics.inter", help=".inter 文件路径")
    parser.add_argument("--meta-file", default="../dataset/electronics/meta_Electronics.jsonl.gz", help="元数据文件路径")
    parser.add_argument("--limit-items", type=int, help="限制导入商品数量（测试用）")
    parser.add_argument("--limit-interactions", type=int, default=0, help="限制导入交互数量（默认0，不导入交互数据）")
    parser.add_argument("--sample-users", type=int, help="只导入指定数量的用户数据")
    parser.add_argument("--sample-only", action="store_true", help="只创建示例数据")
    
    args = parser.parse_args()
    
    # 数据库配置
    db_config = {
        'host': args.host,
        'port': args.port,
        'user': args.user,
        'password': args.password,
        'database': args.database
    }
    
    print("=" * 60)
    print("SASRec Demo 数据导入工具")
    print("=" * 60)
    print(f"数据库: {args.host}:{args.port}/{args.database}")
    print()
    
    try:
        importer = DataImporter(db_config)
        
        if args.sample_only:
            # 只创建示例数据
            importer.create_sample_data()
        else:
            # 导入商品数据
            if os.path.exists(args.item_file):
                importer.import_items(
                    args.item_file, 
                    args.meta_file if os.path.exists(args.meta_file) else None,
                    args.limit_items
                )
            else:
                print(f"⚠️  商品文件不存在: {args.item_file}")
            
            # 导入交互数据（仅当 limit_interactions > 0 时）
            if args.limit_interactions and args.limit_interactions > 0:
                if os.path.exists(args.inter_file):
                    importer.import_interactions(
                        args.inter_file,
                        args.limit_interactions,
                        args.sample_users
                    )
                else:
                    print(f"⚠️  交互文件不存在: {args.inter_file}")
        
        print("\n" + "=" * 60)
        print("✅ 数据导入完成！")
        print("=" * 60)
        
    except mysql.connector.Error as e:
        print(f"\n❌ 数据库错误: {e}")
    except Exception as e:
        print(f"\n❌ 导入失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
