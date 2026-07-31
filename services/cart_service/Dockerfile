# cart_service Dockerfile — 薄镜像, FROM recweb-base
# ----------------------------------------------------------------------------
# cart_service(:5006) 购物车增改删计数; 深度链 cart→catalog 取价格 + 尽力写 interactions。
# 依赖全在 recweb-base(flask/flask-cors/mysql-connector/requests/opentelemetry-*), 无额外 pip。
#
# build(仓库根执行, 需先 build recweb-base:latest):
#   docker build -f services/cart_service/Dockerfile -t recweb-cart:latest .
# ----------------------------------------------------------------------------
FROM recweb-base:latest

WORKDIR /app

# cart_service app.py 用 parents[2] 定位项目根 + import shared.nacos_client(下游 catalog 解析)。
# 复刻仓库布局: /app/services/cart_service/app.py + /app/shared/。
COPY shared/ /app/shared/
COPY services/cart_service/ /app/services/cart_service/

WORKDIR /app/services/cart_service

# cart_service 监听 0.0.0.0:5006 (CART_SERVICE_PORT 默认 5006, 见 app.py __main__)
EXPOSE 5006

CMD ["python", "app.py"]
