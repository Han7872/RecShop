# shop_web - E-commerce Recommendation System with AI Assistant

## Project Overview

ShopWeb is a Flask-based e-commerce demonstration platform that integrates a **SASRec (Self-Attentive Sequential Recommendation)** model for personalized product recommendations and a **DeepSeek AI-powered chatbot** for explainable recommendations. The app is organized into three **Flask Blueprints** — **buyer** (`/`), **merchant** (`/merchant`), and **admin** (`/admin`) — all sharing a single database and model layer. The buyer storefront tracks interactions, serves personalized recommendations, and supports AI-driven product explanations; the merchant console provides catalog & order management with sales analytics; the admin backend covers user/merchant/item/order oversight, announcements, RBAC, and operational statistics.

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Technology Stack](#technology-stack)
3. [Project Structure](#project-structure)
4. [Core Features](#core-features)
5. [Database Design](#database-design)
6. [Backend Implementation](#backend-implementation)
7. [Frontend Implementation](#frontend-implementation)
8. [Recommendation System Integration](#recommendation-system-integration)
9. [AI Assistant Integration](#ai-assistant-integration)
10. [API Endpoints](#api-endpoints)
11. [Installation & Setup](#installation--setup)
12. [Running the Application](#running-the-application)
13. [Future Improvements](#future-improvements)

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           User Interface                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Homepage   │  │ Product Page │  │  Login Page  │  │  About Page  │ │
│  │  (index.html)│  │(product_     │  │ (login.html) │  │ (about.html) │ │
│  │              │  │ detail.html) │  │              │  │              │ │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────────────┘ │
│         │                 │                 │                            │
│  ┌──────▼─────────────────▼─────────────────▼──────────────────────────┐│
│  │              Flask Web Application (Port 3000)                        ││
│  │  buyer_bp `/` · merchant_bp `/merchant` · admin_bp `/admin`          ││
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ ││
│  │  │ Blueprints  │  │   Models    │  │  Templates  │  │   Static    │ ││
│  │  │ (per role)  │  │ (models.py) │  │ buyer/merch.│  │  (CSS/JS)   │ ││
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ ││
│  └──────────────────────────┬───────────────────────────────────────────┘│
└─────────────────────────────┼───────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
          ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│  MySQL Database │  │  Backend API    │  │  DeepSeek API   │
│                 │  │  (Port 5000)    │  │  (External)     │
│  - users (+status)   │  │                 │  │                 │
│  - items (+merchant) │  │  ┌───────────┐  │  │  - Chat API     │
│  - merchants, shops  │  │  │  SASRec   │  │  │  - deepseek-chat│
│  - interactions …  │  │  │  API      │  │  │                 │
│                 │  │  │ (Port 8000)│  │  │                 │
└─────────────────┘  │  └───────────┘  │  └─────────────────┘
                     └─────────────────┘
```

---

## Technology Stack

### Backend
| Technology | Version | Purpose |
|------------|---------|---------|
| Python | 3.9+ | Core programming language |
| Flask | 3.0.0 | Web framework |
| Flask-SQLAlchemy | 3.1.1 | ORM for database operations |
| Flask-Login | 0.6.3 | User session management |
| PyMySQL | 1.1.0 | MySQL database connector |
| OpenAI SDK | 1.0.0+ | DeepSeek API client (OpenAI-compatible) |
| Requests | 2.31.0+ | HTTP client for API calls |
| Werkzeug | 3.0.1 | Password hashing utilities |

### Frontend
| Technology | Purpose |
|------------|---------|
| HTML5 | Page structure |
| CSS3 | Styling and responsive design |
| Vanilla JavaScript | Interactive functionality |
| Jinja2 | Template engine |

### Database
| Technology | Purpose |
|------------|---------|
| MySQL | Relational database: buyers, items, orders, merchants/shops, admins (v2) |

### External Services
| Service | Purpose |
|---------|---------|
| SASRec API | Sequential recommendation model |
| DeepSeek API | AI-powered chat assistant |

---

## Project Structure

```
shop_web/
├── app/                              # Main application package
│   ├── __init__.py                   # Flask app factory, registers all Blueprints
│   ├── extensions.py                 # Shared extensions (db, login_manager)
│   ├── models.py                     # Shared SQLAlchemy models (User, Item, Order, Review, etc.)
│   │
│   ├── buyer/                        # Buyer platform Blueprint (url_prefix='/')
│   │   ├── __init__.py               #   buyer_bp
│   │   └── routes.py                 #   Storefront, cart, orders, AI chat, etc.
│   ├── merchant/                     # Merchant platform Blueprint (url_prefix='/merchant')
│   │   ├── __init__.py               #   merchant_bp
│   │   ├── decorators.py             #   merchant_required, own_product_required
│   │   └── routes.py                 #   Auth, dashboard, product CRUD, orders/ship, review reply
│   ├── admin/                        # Admin platform Blueprint (url_prefix='/admin')
│   │   ├── __init__.py               #   admin_bp
│   │   ├── decorators.py             #   admin_required, role_required
│   │   └── routes.py                 #   Auth, dashboard, user/merchant/item/order/review mgmt, stats, …
│   │
│   ├── static/                       # Shared static assets (buyer CSS/JS)
│   │   ├── css/
│   │   │   ├── base.css             # Global styles
│   │   │   ├── index.css            # Homepage styles
│   │   │   ├── login.css            # Login/Register page styles
│   │   │   ├── about.css            # About page styles
│   │   │   ├── product_detail.css   # Product detail styles
│   │   │   ├── cart.css             # Shopping cart styles
│   │   │   ├── search.css           # Search results page styles
│   │   │   ├── order.css            # Order & checkout styles
│   │   │   └── ai_picks.css         # AI Picks page styles
│   │   └── js/                       # JavaScript files
│   └── templates/                    # Jinja2 templates (organized by platform)
│       ├── buyer/                    #   Buyer platform templates
│       │   ├── base.html             #     Base template with buyer nav/search
│       │   ├── index.html            #     Homepage with recommendations
│       │   ├── login.html            #     User login page
│       │   ├── register.html         #     User registration page
│       │   ├── about.html            #     About page
│       │   ├── product_detail.html   #     Regular product detail
│       │   ├── recommended_product_detail.html  # Product with AI chat
│       │   ├── cart.html             #     Shopping cart page
│       │   ├── checkout.html         #     Checkout page
│       │   ├── orders.html           #     Order list page
│       │   ├── order_detail.html     #     Order detail page
│       │   ├── search.html           #     Search results page
│       │   └── ai_picks.html         #     AI-reranked recommendations page
│       ├── merchant/ (11 templates)  #   Merchant console
│       │   ├── base.html             #     Extends shared console shell + merchant nav/user area
│       │   ├── login.html / register.html
│       │   ├── dashboard.html        #     Stats cards + shortcuts
│       │   ├── products.html / product_form.html
│       │   ├── orders.html / order_detail.html
│       │   ├── reviews.html          #     Review management + reply
│       │   └── stats.html / shop.html / settings.html / finance.html
│       └── admin/   (16 templates)   #   Admin backend
│           ├── _console_shell.html   #     Shared console skeleton (sidebar/header/content/flash)
│           ├── base.html             #     Extends shared shell + admin nav/user area
│           ├── login.html
│           ├── dashboard.html        #     Platform overview + quick actions
│           ├── users.html / user_detail.html
│           ├── merchants.html / merchant_detail.html
│           ├── items.html
│           ├── orders.html / order_detail.html
│           └── stats.html / announcements.html / logs.html / admins.html / reviews.html
│
├── config.py                         # Flask configuration
├── config_ai.py                      # DeepSeek API configuration
├── requirements.txt                  # Python dependencies
├── run.py                            # Application entry point (port 3000)
├── generate_password.py              # Password hash generator utility
├── test_recommendation.py            # Recommendation API test script
├── .env.example                      # Environment variables template
└── README.md                         # Project documentation
```

### Blueprint Architecture

The application uses Flask Blueprint to organize three platforms under a single Flask app:

| Platform | Blueprint | URL Prefix | Status |
|----------|-----------|------------|--------|
| Buyer (买家端) | `buyer_bp` | `/` | ✅ Active |
| Merchant (商家端) | `merchant_bp` | `/merchant` | ✅ P0+P1：认证、仪表盘、商品 CRUD、订单发货、销售统计、店铺设置、收益概览、评论管理+回复 |
| Admin (管理端) | `admin_bp` | `/admin` | ✅ P0+P1：认证 RBAC、仪表盘、用户/商家/商品/订单/评论管理、数据统计、公告、日志、管理员账号 |

All three platforms share `models.py`, `extensions.py`, `config.py`, and `static/` assets.

---

## Core Features

### 1. User Authentication System
- **User Registration**: Email-based registration with password validation
- **Email/Password Login**: Secure authentication with bcrypt password hashing
- **Session Management**: Flask-Login for persistent user sessions
- **Remember Me**: Optional persistent login cookies
- **Protected Routes**: Login-required decorators for secure pages

### 2. Product Catalog & Search
- **Dynamic Product Listing**: Products loaded from MySQL database
- **Full-text Search**: FULLTEXT index + LIKE fallback for keyword search
- **Advanced Filtering**: Category filter and price range selector
- **Sorting Options**: Price, rating, relevance, and newest
- **Pagination**: Page-based navigation with smart ellipsis
- **Product Details**: Comprehensive product information display
- **Image Support**: Product images with fallback placeholders

### 3. Shopping Cart System
- **Persistent Cart**: Database-backed cart (survives logout)
- **Cart Management**: Add, update quantity, remove items
- **Real-time Updates**: AJAX operations without page refresh
- **Cart Badge**: Dynamic item count in navigation bar
- **Subtotal Calculation**: Automatic price computation
- **Interaction Logging**: add_to_cart/remove_from_cart events tracked

### 4. Order Management System
- **Address Management**: Add, edit, delete, set default shipping addresses
- **Checkout Flow**: Address selection, order confirmation, remark notes
- **Order Creation**: Generate unique order numbers with timestamp + random suffix
- **Snapshot Mechanism**: Save product/address snapshots to prevent historical data corruption
- **Order States**: 5-state lifecycle (pending → paid → shipped → completed / cancelled)
- **Status Timeline**: Visual progress tracker with timestamps for each state transition
- **Payment Simulation**: One-click mock payment (creates purchase interactions)
- **Order List**: Filter by status (All/Pending/Paid/Shipped/Completed) with pagination
- **Purchase Tracking**: Auto-generate `purchase` interactions for recommendation algorithm

### 5. Personalized Recommendations
- **SASRec Integration**: State-of-the-art sequential recommendation
- **AI Picks Page**: LLM-reranked top recommendations
- **"Recommended For You" Section**: Personalized product suggestions (`top_k=20` to compensate for filtering)
- **Data Quality Filter**: Homepage excludes placeholder items (`Product_` prefix, ~33% of DB) and untitled items — only real-titled `active` products are shown
- **Interaction Tracking**: Click/view/purchase behavior logging
- **Fallback Recommendations**: Random qualified products for new/guest users

### 6. AI-Powered Chat Assistant
- **Explainable Recommendations**: DeepSeek AI explains why products were recommended
- **Multi-turn Conversation**: Context-aware dialogue
- **Product Context Awareness**: AI knows current product details
- **User History Integration**: AI considers browsing history patterns

### 7. Rating & Review System
- **Review Submission**: Buyers can review purchased items from completed orders (1-5 star rating + text + image URLs + anonymous option)
- **Review Uniqueness**: Each order item can only be reviewed once (DB UNIQUE constraint)
- **Real-time Rating Sync**: `items.rating` and `items.review_count` auto-recalculated via `refresh_item_rating()` on every review change
- **Product Page Reviews**: Real review display with stats overview, star ratings, merchant replies, and AJAX "Load More" pagination
- **Review Status**: Order pages show review status ("Write Review" / "Reviewed") per item
- **Merchant Reply**: Merchants can reply to reviews on their products (one reply per review, with ownership validation)
- **Admin Moderation**: Admins can approve/reject/hide reviews; super_admin can permanently delete; all actions logged via `AdminLog`
- **Anonymous Reviews**: Buyers can choose to submit reviews anonymously

### 8. Responsive UI
- **Modern Design**: Clean, minimalist aesthetic
- **Mobile-Friendly**: Responsive CSS layouts for all screen sizes
- **Floating Chat Widget**: Non-intrusive AI assistant interface
- **Loading States**: Visual feedback during API calls
- **Search Bar**: Integrated in navigation for global access

### 9. Merchant Console (`/merchant`)
- **Separate auth**: Merchant login/register/logout; `session['_user_role']='merchant'` with Flask-Login loading `Merchant` in `user_loader`
- **Access control**: `merchant_required` (status=approved), `own_product_required` (caches product in `g.product`)
- **Dashboard**: Product counts, pending orders, sales summaries
- **Catalog CRUD**: List/search/filter/pagination; create & edit items; toggle active/draft; soft-delete (`removed`) via JSON APIs
- **Order management**: List orders containing merchant's SKUs; detail with address snapshot; ship action (paid -> shipped)
- **Review management** (`/merchant/reviews`): View all reviews for own products; filter by all/pending reply/replied; reply to reviews via modal; dashboard shows pending reply count
- **Sales stats** (`/merchant/stats`): 7/14/30-day sales trend table + TOP 10 products
- **Shop settings** (`/merchant/shop`): Edit shop name, logo, description
- **Account settings** (`/merchant/settings`): Update profile and change password
- **Finance overview** (`/merchant/finance`): Demo revenue summary with platform fee simulation
- **Dev convenience**: Set env `AUTO_APPROVE_MERCHANT=true` to auto-approve new merchants

### 10. Admin Backend (`/admin`)
- **Separate auth**: Admin login/logout; `session['_user_role']='admin'`; login records `last_login_at` and `admin_logs`
- **Access control**: `admin_required` (status=active), `role_required(*roles)` for super_admin-only actions
- **Dashboard**: User/merchant/order/GMV/review overview + pending merchant/item/review quick links
- **User management**: List with search + pagination; detail with recent orders; toggle ban/unban (logs action)
- **Merchant management**: List with status filter; approve/reject/ban/unban APIs (ban cascades item removal); detail with recent items
- **Item review**: List with status/merchant filter; approve/reject/remove APIs (all logged)
- **Order oversight**: Full-platform order list; detail view; super_admin force-cancel API
- **Review moderation** (`/admin/reviews`): Full-platform review list with status filter + search; approve/reject/hide actions (admin); permanent delete (super_admin only); all operations logged and trigger rating recalculation
- **Data statistics** (`/admin/stats`): User growth, order trend, recommendation volume (last 15 days)
- **Announcement management**: Create/publish/withdraw/delete announcements with status tracking
- **Operation logs** (`/admin/logs`): Filterable admin action log with IP recording
- **Admin account management** (super_admin only): Create admins with temp password, toggle enable/disable, reset password

---

## Database Design

### Users Table
```sql
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_token VARCHAR(100) UNIQUE NOT NULL,  -- Unique user identifier
    username VARCHAR(100),                     -- Display name
    email VARCHAR(255),                        -- Login email
    password_hash VARCHAR(255),                -- Bcrypt hashed password
    avatar_url VARCHAR(500),                   -- Profile picture URL
    status ENUM('active','banned') NOT NULL DEFAULT 'active',  -- v2: ban buyer accounts
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

### Items Table
```sql
CREATE TABLE items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    item_id VARCHAR(100) UNIQUE NOT NULL,     -- Product SKU/ID
    title VARCHAR(500),                        -- Product name
    category VARCHAR(255),                     -- Product category
    brand VARCHAR(255),                        -- Product brand
    price DECIMAL(10, 2),                      -- Product price
    description TEXT,                          -- Product description
    image_url VARCHAR(500),                    -- Product image
    review_count INT DEFAULT 0,                -- Number of reviews
    merchant_id INT NULL,                      -- v2: FK to merchants.id
    status ENUM('draft','pending','active','rejected','removed') NOT NULL DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Interactions Table
```sql
CREATE TABLE interactions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_token VARCHAR(100) NOT NULL,          -- User identifier
    item_id VARCHAR(100) NOT NULL,             -- Product identifier
    interaction_type ENUM('view', 'click', 'purchase', 'rating', 'add_to_cart'),
    rating DECIMAL(2, 1),                      -- Optional rating value
    duration INT,                              -- Time spent (seconds)
    source VARCHAR(50),                        -- Traffic source
    session_id VARCHAR(100),                   -- Browser session
    timestamp BIGINT,                          -- Unix timestamp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Recommendations Table
```sql
CREATE TABLE recommendations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_token VARCHAR(100) NOT NULL,
    item_id VARCHAR(100) NOT NULL,
    score FLOAT,                               -- Recommendation confidence
    `rank` INT,                                -- Position in recommendation list
    input_sequence JSON,                       -- Items used for prediction
    inference_time FLOAT,                      -- Model inference time (ms)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Cart Items Table
```sql
CREATE TABLE cart_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_token VARCHAR(100) NOT NULL,
    item_id VARCHAR(100) NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_item (user_token, item_id)
);
```

### User Addresses Table
```sql
CREATE TABLE user_addresses (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_token VARCHAR(100) NOT NULL,
    receiver_name VARCHAR(100) NOT NULL,
    receiver_phone VARCHAR(20) NOT NULL,
    province VARCHAR(50),
    city VARCHAR(50),
    district VARCHAR(50),
    address VARCHAR(500) NOT NULL,
    is_default TINYINT(1) DEFAULT 0,          -- 1 = default address
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

### Orders Table
```sql
CREATE TABLE orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_no VARCHAR(50) UNIQUE NOT NULL,      -- Order number (e.g., ORD1234567890)
    user_token VARCHAR(100) NOT NULL,
    address_id BIGINT NOT NULL,                -- FK to user_addresses
    
    -- Address snapshot (frozen at order creation)
    snapshot_receiver_name VARCHAR(100) NOT NULL,
    snapshot_receiver_phone VARCHAR(20) NOT NULL,
    snapshot_address VARCHAR(500) NOT NULL,
    
    total_amount DECIMAL(10, 2) NOT NULL,
    item_count INT NOT NULL DEFAULT 0,
    status ENUM('pending', 'paid', 'shipped', 'completed', 'cancelled') DEFAULT 'pending',
    remark TEXT,                               -- Customer note
    cancel_reason VARCHAR(200),
    
    -- Timestamps for each state transition
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    paid_at TIMESTAMP NULL,
    shipped_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    cancelled_at TIMESTAMP NULL
);
```

### Order Items Table
```sql
CREATE TABLE order_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT NOT NULL,                  -- FK to orders
    item_id VARCHAR(100) NOT NULL,
    
    -- Product snapshot (frozen at order creation)
    item_title VARCHAR(500) NOT NULL,
    item_image VARCHAR(500),
    item_price DECIMAL(10, 2) NOT NULL,
    
    quantity INT NOT NULL DEFAULT 1,
    subtotal DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Reviews Table (v3)
```sql
CREATE TABLE reviews (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_item_id BIGINT NOT NULL UNIQUE,        -- FK to order_items, one review per order item
    user_token VARCHAR(100) NOT NULL,             -- FK to users
    item_id VARCHAR(100) NOT NULL,                -- FK to items
    rating TINYINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    content TEXT,                                 -- Review text
    images JSON,                                  -- Array of image URLs
    is_anonymous TINYINT DEFAULT 0,               -- 1 = hide username
    status ENUM('pending','approved','rejected','hidden') DEFAULT 'approved',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

### Review Replies Table (v3)
```sql
CREATE TABLE review_replies (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    review_id BIGINT NOT NULL UNIQUE,             -- FK to reviews, one reply per review
    merchant_id INT NOT NULL,                     -- FK to merchants
    content TEXT NOT NULL,                        -- Merchant reply text
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Multi-role schema (v2) + Review schema (v3)

Buyer、商家、管理员共用同一 Flask 应用，数据库在基础订单表之上增加了多端字段与表。在包含 `scripts/` 的仓库根目录执行：

```bash
mysql -u root -p your_database < scripts/migrate_v2.sql
mysql -u root -p your_database < scripts/migrate_v3_reviews.sql
```

主要增量（与 `app/models.py` 对齐）：

**v2:**
- **`users.status`** — `active` / `banned`（买家封禁）
- **`items.merchant_id`**、**`items.status`** — `draft` / `pending` / `active` / `rejected` / `removed`
- **`merchants`** / **`shops`** — 商家账号与店铺档案
- **`admins`** / **`admin_logs`** / **`announcements`** — 管理端（ORM + 完整路由已实现）

**v3:**
- **`reviews`** — 商品评论（UNIQUE(order_item_id) 确保每个订单明细只评一次）
- **`review_replies`** — 商家回复（UNIQUE(review_id) 确保每条评论只回复一次）
- **`v_item_review_stats`** / **`v_merchant_review_stats`** — 评论统计视图
- `items.rating` + `items.review_count` 通过 `refresh_item_rating()` 自动同步

---

## Backend Implementation

### Flask Application Factory (`app/__init__.py`)

```python
from flask import Flask
from config import config

def create_app(config_name='default'):
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    from app.extensions import db, login_manager
    db.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = 'buyer.login'
    
    @login_manager.user_loader
    def load_user(user_id):
        from flask import session
        from app.models import User, Merchant, Admin
        role = session.get('_user_role', 'buyer')
        if role == 'merchant':
            return Merchant.query.get(int(user_id))
        if role == 'admin':
            return Admin.query.get(int(user_id))
        return User.query.get(int(user_id))
    
    # Register Blueprints
    from app.buyer import buyer_bp
    from app.merchant import merchant_bp
    from app.admin import admin_bp
    
    app.register_blueprint(buyer_bp)       # url_prefix='/'
    app.register_blueprint(merchant_bp)    # url_prefix='/merchant'
    app.register_blueprint(admin_bp)       # url_prefix='/admin'
    
    return app
```

### Data Models (`app/models.py`)

共享模型除 `User`、`Item`、`Order`、购物车与 AI 记忆外，还包括 **`Merchant`**、**`Shop`**、**`Admin`**、**`AdminLog`**、**`Announcement`**（详见源码）。

#### User Model
```python
class User(UserMixin, db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    user_token = db.Column(db.String(100), unique=True, nullable=False)
    username = db.Column(db.String(100))
    email = db.Column(db.String(255))
    password_hash = db.Column(db.String(255))
    avatar_url = db.Column(db.String(500))
    status = db.Column(db.Enum('active', 'banned', name='user_status_enum'),
                       nullable=False, server_default='active')
    
    def check_password(self, password):
        if not self.password_hash:
            return False
        return check_password_hash(self.password_hash, password)
    
    def get_avatar(self):
        return self.avatar_url or f"https://placehold.co/40x40?text={self.username[0]}"
```

#### Item Model
```python
class Item(db.Model):
    __tablename__ = 'items'
    
    id = db.Column(db.Integer, primary_key=True)
    item_id = db.Column(db.String(100), unique=True, nullable=False)
    title = db.Column(db.String(500))
    category = db.Column(db.String(255))
    brand = db.Column(db.String(255))
    price = db.Column(db.Numeric(10, 2))
    description = db.Column(db.Text)
    image_url = db.Column(db.String(500))
    review_count = db.Column(db.Integer, default=0)
    merchant_id = db.Column(db.Integer, db.ForeignKey('merchants.id'), default=None)
    status = db.Column(db.Enum('draft', 'pending', 'active', 'rejected', 'removed',
                               name='item_status_enum'), nullable=False, server_default='active')

    merchant = db.relationship('Merchant', backref=db.backref('items', lazy='dynamic'))
    
    def to_dict(self):
        return {
            'id': self.id,
            'item_id': self.item_id,
            'name': self.title,
            'category': self.category,
            'brand': self.brand,
            'price': float(self.price) if self.price else None,
            'description': self.description,
            'image': self.image_url or 'https://placehold.co/400x400?text=No+Image',
            'review_count': self.review_count or 0
        }
```

#### Interaction Model
```python
class Interaction(db.Model):
    __tablename__ = 'interactions'
    
    id = db.Column(db.Integer, primary_key=True)
    user_token = db.Column(db.String(100), nullable=False)
    item_id = db.Column(db.String(100), nullable=False)
    interaction_type = db.Column(db.String(50))
    session_id = db.Column(db.String(100))
    source = db.Column(db.String(50))
    timestamp = db.Column(db.BigInteger)
    
    @classmethod
    def create_click_interaction(cls, user_token, item_id, session_id, source='direct'):
        return cls(
            user_token=user_token,
            item_id=item_id,
            interaction_type='click',
            session_id=session_id,
            source=source,
            timestamp=int(time.time() * 1000)
        )
```

#### Order Model
```python
class Order(db.Model):
    __tablename__ = 'orders'
    
    id = db.Column(db.BigInteger, primary_key=True)
    order_no = db.Column(db.String(50), unique=True, nullable=False)
    user_token = db.Column(db.String(100), nullable=False)
    address_id = db.Column(db.BigInteger, nullable=False)
    
    # Address snapshot (frozen at order creation to prevent history corruption)
    snapshot_receiver_name = db.Column(db.String(100), nullable=False)
    snapshot_receiver_phone = db.Column(db.String(20), nullable=False)
    snapshot_address = db.Column(db.String(500), nullable=False)
    
    total_amount = db.Column(db.Numeric(10, 2), nullable=False)
    item_count = db.Column(db.Integer, default=0)
    status = db.Column(db.Enum('pending', 'paid', 'shipped', 'completed', 'cancelled'))
    
    # Timestamps for each state transition
    created_at = db.Column(db.TIMESTAMP, server_default=db.func.current_timestamp())
    paid_at = db.Column(db.TIMESTAMP)
    shipped_at = db.Column(db.TIMESTAMP)
    completed_at = db.Column(db.TIMESTAMP)
    cancelled_at = db.Column(db.TIMESTAMP)
    
    # Relationship: cascade delete order items when order is deleted
    items = db.relationship('OrderItem', backref='order', cascade='all, delete-orphan')
```

---

## Frontend Implementation

### Template Inheritance Structure

```
buyer/base.html (Buyer Navigation, Search Bar, Cart Badge)
    │
    ├── buyer/index.html (Homepage)
    │   ├── Hero Section
    │   ├── "Recommended For You" Section
    │   └── "Popular Products" Section
    │
    ├── buyer/login.html / register.html (Authentication)
    │
    ├── buyer/product_detail.html (Regular Product Page)
    │   ├── Product Gallery, Info, Action Buttons
    │   └── Customer Reviews
    │
    ├── buyer/recommended_product_detail.html (Product + AI Chat)
    │   ├── [Same as product_detail.html]
    │   └── Floating AI Chat Widget
    │
    ├── buyer/cart.html → buyer/checkout.html (Shopping Flow)
    │
    ├── buyer/orders.html → buyer/order_detail.html (Order Management)
    │
    ├── buyer/search.html (Search & Filter)
    │
    └── buyer/about.html (About Page)

admin/_console_shell.html (Shared console skeleton)
    │
    ├── admin/base.html (Admin nav + admin user header block)
    │   ├── admin/login.html (standalone)
    │   ├── admin/dashboard.html
    │   ├── admin/users.html → admin/user_detail.html
    │   ├── admin/merchants.html → admin/merchant_detail.html
    │   ├── admin/items.html
    │   ├── admin/orders.html → admin/order_detail.html
    │   ├── admin/stats.html / announcements.html / logs.html
    │   └── admin/admins.html (super_admin only)
    │
    └── merchant/base.html (Merchant nav + merchant user header block)
        ├── merchant/login.html / register.html
        ├── merchant/dashboard.html
        ├── merchant/products.html → merchant/product_form.html (new & edit)
        ├── merchant/orders.html → merchant/order_detail.html
        └── merchant/stats.html / shop.html / settings.html / finance.html

Buyer templates use `{% extends "buyer/base.html" %}` and `url_for('buyer.xxx')`.  
Merchant templates use `{% extends "merchant/base.html" %}` and `url_for('merchant.xxx')`.  
Admin templates use `{% extends "admin/base.html" %}` and `url_for('admin.xxx')`.

### Console Frontend Standardization (Admin + Merchant)

- Shared CSS baseline: `app/static/css/console_base.css`
- Shared shell template: `app/templates/admin/_console_shell.html`
- Console differences are injected through blocks:
  - `console_brand`
  - `console_nav`
  - `console_header_user`
- This keeps two role-specific base files in place (`admin/base.html`, `merchant/base.html`) while removing duplicated layout boilerplate.

### CSS Architecture

| File | Purpose |
|------|---------|
| `base.css` | Buyer global styles, navigation, footer, typography |
| `index.css` | Buyer homepage product grid, hero section, cards |
| `login.css` | Buyer auth form styling, validation states |
| `product_detail.css` | Buyer product layout, reviews, actions |
| `console_base.css` | Shared console layout/components for admin + merchant |

### Chat Widget Implementation

The AI chat widget is a floating component with:
- **Toggle Button**: Circular button in bottom-right corner
- **Chat Window**: Expandable conversation interface
- **Message Display**: Differentiated user/AI message bubbles
- **Input Area**: Text input with send button
- **Auto-open**: Opens automatically on page load
- **Loading States**: "Thinking..." indicator during API calls

```javascript
// Key Chat Widget Features
const productContext = {
    name: "{{ product.name }}",
    category: "{{ product.category }}",
    brand: "{{ product.brand }}",
    price: "{{ product.price }}",
    description: "{{ product.description }}"
};

const interactionHistory = {{ interaction_history|safe }};

// Auto-generate initial recommendation explanation
async function generateInitialMessage() {
    const initialPrompt = "Please briefly explain why this product is recommended to me.";
    // ... API call to /api/chat
}
```

---

## Recommendation System Integration

### SASRec Model Overview

**SASRec (Self-Attentive Sequential Recommendation)** is a deep learning model that:
1. Processes sequential user interaction history
2. Uses self-attention mechanism to capture item relationships
3. Predicts next items user is likely to interact with

### Integration Flow

```
1. User clicks product → Interaction logged to database
                              ↓
2. User visits homepage → Flask requests recommendations
                              ↓
3. Backend API receives request → Fetches user's interaction history
                              ↓
4. SASRec API called with item sequence → Returns ranked predictions
                              ↓
5. Recommendations saved to database → Product details fetched
                              ↓
6. Homepage displays personalized "Recommended For You" section
```

### API Request Example

```python
# Flask Web App → Backend API
response = requests.post(
    'http://localhost:5000/api/recommend',
    json={
        'user_token': current_user.user_token,
        'top_k': 3
    },
    timeout=5
)

# Backend API → SASRec API
sasrec_response = requests.post(
    'http://localhost:8000/recommend',
    json={
        'item_sequence': ['item_1', 'item_2', 'item_3'],
        'top_k': 10,
        'exclude_history': True
    }
)
```

### Fallback Strategy

When recommendations are unavailable:
- **Not logged in**: Display random popular products
- **No interaction history**: Display random popular products
- **API timeout/error**: Display random popular products

---

## AI Assistant Integration

### DeepSeek API Configuration

```python
# config_ai.py
DEEPSEEK_CONFIG = {
    'api_key': 'sk-xxxxxxxxxxxxxxxx',  # Your API key
    'base_url': 'https://api.deepseek.com/v1',
    'model': 'deepseek-chat',
    'temperature': 0.7,
    'max_tokens': 500
}
```

### System Prompt Design

The AI assistant uses a carefully crafted system prompt:

```
You are a professional product recommendation assistant. Your role is to 
explain why the system recommended the current product based on the user's 
browsing behavior patterns.

[Current Recommended Product]
Product Name: {name}
Category: {category}
Brand: {brand}
Price: {price}
Description: {description}

[User Browsing History]
- Product A (Category X)
- Product B (Category Y) - User viewed this product multiple times (6 times)

[Important Rules]
1. Explain why this product was recommended based on browsing behavior
2. Do not directly say "You browsed XXX before", use natural phrases like 
   "Based on your interest in XX category"
3. If user viewed certain categories multiple times, emphasize this interest
4. Keep responses concise and friendly
5. Provide professional answers for product-related questions
```

### Interaction History Processing

```python
# Filter duplicate interactions
interaction_counts = {}
for inter in user_interactions:
    if inter.item_id not in interaction_counts:
        interaction_counts[inter.item_id] = {'count': 0, 'type': inter.interaction_type}
    interaction_counts[inter.item_id]['count'] += 1

# Build processed history for AI
for item_id, data in interaction_counts.items():
    if data['count'] >= 5:
        # Highlight frequently viewed items
        note = f"User viewed this product multiple times ({data['count']} times)"
    else:
        note = None
```

### Multi-turn Conversation

The chat maintains conversation history for context:

```javascript
let conversationHistory = [];

// User sends message
conversationHistory.push({ role: 'user', content: userMessage });

// API call includes full history
fetch('/api/chat', {
    method: 'POST',
    body: JSON.stringify({
        messages: conversationHistory,
        product_context: productContext,
        interaction_history: interactionHistory
    })
});

// AI response added to history
conversationHistory.push({ role: 'assistant', content: aiResponse });
```

---

## API Endpoints

### Flask Web Application (Port 3000)

**Buyer Platform (`/`)**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Homepage with recommendations |
| `/login` | GET, POST | User authentication |
| `/register` | GET, POST | User registration |
| `/logout` | GET | End user session |
| `/product/<id>` | GET | Regular product detail page |
| `/recommended/<id>` | GET | Product page with AI chat |
| `/search` | GET | Product search with filters |
| `/cart` | GET | Shopping cart page |
| `/checkout` | GET | Checkout page |
| `/orders` | GET | Order list with status filter |
| `/order/<order_no>` | GET | Order detail page |
| `/api/chat` | POST | AI chat assistant endpoint |
| `/api/chat/extract-memory` | POST | Extract user insights from chat |
| `/api/cart/add` | POST | Add item to cart |
| `/api/cart/update/<id>` | POST | Update cart item quantity |
| `/api/cart/remove/<id>` | POST | Remove item from cart |
| `/api/cart/count` | GET | Get cart item count |
| `/api/order/create` | POST | Create order from cart |
| `/api/order/<no>/pay` | POST | Simulate payment |
| `/api/order/<no>/cancel` | POST | Cancel pending order |
| `/api/order/<no>/complete` | POST | Confirm receipt |
| `/api/address/save` | POST | Save shipping address |
| `/api/address/delete` | POST | Delete shipping address |
| `/api/memories` | GET | Get user's AI memories |
| `/api/memories/<id>` | DELETE | Delete an AI memory |
| `/api/review` | POST | Submit review for completed order item |
| `/api/reviews/<item_id>` | GET | Paginated reviews for a product (AJAX) |

**Merchant Platform (`/merchant`)**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/merchant/login` | GET, POST | Merchant login |
| `/merchant/register` | GET, POST | Register (default `pending` until approved) |
| `/merchant/logout` | GET | Clear merchant session |
| `/merchant/` | GET | Dashboard |
| `/merchant/products` | GET | Product list (search, status filter, pagination) |
| `/merchant/products/new` | GET, POST | Create product |
| `/merchant/products/<id>/edit` | GET, POST | Edit own product |
| `/merchant/api/products/<id>/toggle-status` | POST | Toggle listing visibility / status |
| `/merchant/api/products/<id>/delete` | POST | Soft-delete (`removed`) |
| `/merchant/orders` | GET | Orders containing merchant line items |
| `/merchant/orders/<order_no>` | GET | Order detail (merchant slice + snapshot) |
| `/merchant/api/orders/<order_no>/ship` | POST | Mark shipped when order is `paid` |
| `/merchant/reviews` | GET | Review list (filter: all/pending reply/replied) |
| `/merchant/api/reviews/<id>/reply` | POST | Reply to a review on own product |

**Admin Platform (`/admin`)**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/admin/login` | GET, POST | Admin login (records `last_login_at` + admin log) |
| `/admin/logout` | GET | Clear admin session |
| `/admin/` | GET | Dashboard (users, merchants, orders, GMV, reviews overview) |
| `/admin/users` | GET | User list with search + pagination |
| `/admin/users/<user_token>` | GET | User detail + recent orders |
| `/admin/api/users/<user_token>/toggle-ban` | POST | Toggle user ban/unban |
| `/admin/merchants` | GET | Merchant list with status filter |
| `/admin/merchants/<id>` | GET | Merchant detail + recent items |
| `/admin/api/merchants/<id>/approve` | POST | Approve merchant |
| `/admin/api/merchants/<id>/reject` | POST | Reject merchant |
| `/admin/api/merchants/<id>/ban` | POST | Ban merchant (cascades item removal) |
| `/admin/api/merchants/<id>/unban` | POST | Unban merchant |
| `/admin/items` | GET | Item list with status/merchant filter |
| `/admin/api/items/<item_id>/approve` | POST | Approve item |
| `/admin/api/items/<item_id>/reject` | POST | Reject item |
| `/admin/api/items/<item_id>/remove` | POST | Remove item |
| `/admin/orders` | GET | Full-platform order list |
| `/admin/orders/<order_no>` | GET | Order detail |
| `/admin/api/orders/<order_no>/force-cancel` | POST | Force cancel (super_admin only) |
| `/admin/reviews` | GET | Full-platform review list with status filter + search |
| `/admin/api/reviews/<id>/approve` | POST | Approve review |
| `/admin/api/reviews/<id>/reject` | POST | Reject review |
| `/admin/api/reviews/<id>/hide` | POST | Hide review |
| `/admin/api/reviews/<id>/delete` | POST | Permanently delete review (super_admin only) |
| `/admin/stats` | GET | User growth + order trend + recommendation stats |
| `/admin/announcements` | GET | Announcement list |
| `/admin/announcements/new` | POST | Create announcement |
| `/admin/api/announcements/<id>/publish` | POST | Publish announcement |
| `/admin/api/announcements/<id>/withdraw` | POST | Withdraw (archive) announcement |
| `/admin/api/announcements/<id>/delete` | POST | Delete announcement |
| `/admin/logs` | GET | Admin operation logs |
| `/admin/admins` | GET | Admin account list (super_admin) |
| `/admin/admins/new` | POST | Create admin account (super_admin) |
| `/admin/api/admins/<id>/toggle-status` | POST | Enable/disable admin (super_admin) |
| `/admin/api/admins/<id>/reset-password` | POST | Reset admin password (super_admin) |

**Merchant Platform P1 (additional endpoints)**

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/merchant/stats` | GET | Sales trend + TOP 10 products |
| `/merchant/shop` | GET, POST | Shop profile settings |
| `/merchant/settings` | GET, POST | Account profile + password change |
| `/merchant/finance` | GET | Revenue overview (demo) |

### Backend API (Port 5000)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/recommend` | POST | Get SASRec recommendations |
| `/api/users/<token>` | GET | Get user information |
| `/api/users/<token>/history` | GET | Get user interaction history |
| `/api/interaction` | POST | Record user interaction |

### SASRec API (Port 8000)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/recommend` | POST | Generate recommendations from item sequence |
| `/health` | GET | API health check |

---

## Installation & Setup

### Prerequisites

- Python 3.9+
- MySQL 8.0+
- Node.js (optional, for frontend tooling)

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd shop_web
```

### Step 2: Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Configure Database

1. Create MySQL database
2. From the **repository root** (directory that contains `scripts/`): import `scripts/database_schema.sql`, then `scripts/migrate_v2.sql` for merchants/admins/shops and extended `users`/`items` columns
3. Update `config.py` with database credentials

### Step 5: Configure AI Assistant

Edit `config_ai.py`:
```python
DEEPSEEK_CONFIG = {
    'api_key': 'your-api-key-here',  # Get from https://platform.deepseek.com/
    ...
}
```

### Step 6: Generate Password Hash (Optional)

```bash
python generate_password.py
```

---

## Running the Application

### Start All Services

```bash
# Terminal 1: SASRec API
cd ../sasrec_api
python api_server.py  # Runs on port 8000 (services/sasrec_api)

# Terminal 2: Backend API
cd ../backend_api
python app.py  # Runs on port 5000 (services/backend_api)

# Terminal 3: Flask Web App
python run.py  # Runs on port 3000
```

### Access the Application

- **Buyer storefront**: http://localhost:3000
- **Merchant console**: http://localhost:3000/merchant
- **Admin backend**: http://localhost:3000/admin
- **Backend API**: http://localhost:5000
- **SASRec API**: http://localhost:8000

---

## Future Improvements

### Completed
- ✅ **Merchant Platform P0+P1**: Auth, dashboard, product CRUD, order management, sales stats, shop settings, finance overview, review management + reply
- ✅ **Admin Platform P0+P1**: Auth + RBAC, dashboard, user/merchant/item/order/review management, data statistics, announcements, operation logs, admin account management
- ✅ **Rating & Review System P0+P1**: Cross-client review lifecycle — buyer submission (star rating + text + images + anonymous), real review display with AJAX pagination, merchant reply, admin moderation (approve/reject/hide/delete), auto rating sync

### P2 Ideas
1. **Payment Integration**: Real payment gateway (Stripe, PayPal, Alipay)
2. **Coupon System**: Discount codes and promotional campaigns
3. **Logistics Tracking**: Real-time shipping status and tracking numbers
4. **Refund System**: Return/refund workflow for cancelled orders
5. **Recommendation Diversity**: A/B testing for different recommendation algorithms
6. **Performance Optimization**: Redis caching, CDN for images, lazy loading
7. **Merchant Marketing Tools**: Coupons, flash sales
8. **AI System Monitor**: `/admin/ai-monitor` dashboard
9. **Financial Reconciliation**: Platform commission + merchant settlement
10. **Review Enhancements**: Image upload, append reviews, review reports, sentiment analysis for recommendations

---

## Contributors

- Project Developer

---

## License

This project is for educational/demonstration purposes.

---

*Last Updated: April 3, 2026 — Cross-client Review System (P0+P1) complete: buyer review submission + display, merchant reply, admin moderation*
