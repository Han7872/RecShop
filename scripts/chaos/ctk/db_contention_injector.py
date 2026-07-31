"""安全只读 DB 争用注入器（共享库共因 / 形态B「只读传导预实验」用）。

================================ 安全声明 ================================
本注入器**只读**真实 shopify2 业务表，绝不修改任何业务数据。它在多个独立
MySQL 连接上反复跑「重只读查询 / SLEEP / 只读事务持锁」，制造 DB 端 CPU/IO/
连接/行锁争用，用来观测争用是否「传导」到上层只读 victim 服务的延迟。

硬安全约束（逐条实现，见下方代码 [SAFE-n] 标注）:
  [SAFE-1] 业务表零写: 除唯一例外的 sandbox 表 (chaos_lock_sandbox) 的
           CREATE TABLE IF NOT EXISTS + seed 行外, 本文件不出现任何
           INSERT/UPDATE/DELETE/REPLACE/TRUNCATE/DROP 业务表语句。
           lock_items 模式对 items 仅做 SELECT ... FOR UPDATE(持 X 锁)
           绝不写 items。lock_table 模式对 items(或白名单表) 仅做
           LOCK TABLES <t> WRITE / UNLOCK TABLES(会话级表锁, 不含写动词、
           不写任何数据, 连接关闭即自动释放), 绝不写业务表。
  [SAFE-2] 保证清理: 每连接 try/finally; finally 里 ROLLBACK + close;
           每连接记录 CONNECTION_ID(); 收尾兜底对仍活着的自己会话发 KILL。
  [SAFE-3] 连接预算: --conns 默认/上限受限; 启动查 Threads_connected,
           断言 conns < max_connections - 当前 - 余量(50), 否则拒跑。
  [SAFE-4] 超时: 每连接设 connection/read timeout 并 SET SESSION
           max_execution_time(仅对 SELECT 生效, 单位 ms); SLEEP/锁单次时长有上限。
  [SAFE-5] 数据完整性核对由 harness 侧 (db_preexperiment.py) 在窗前后做
           CHECKSUM TABLE; 本注入器另提供 checksum_tables() 供其调用。

用法(独立 CLI):
  # 对照: N 连接各循环 SELECT SLEEP(t), 预期不传导
  python db_contention_injector.py --mode sleep --conns 10 --duration 30 --sleep-sec 2
  # 目标: N 连接各循环 items ORDER BY RAND() 重只读(全扫+filesort 吃 CPU/IO)
  python db_contention_injector.py --mode heavy_read --conns 8 --duration 60 --rand-limit 1000
  # 机制对照: sandbox 表只读事务 FOR UPDATE 持锁(MVCC 下不阻塞普通读)
  python db_contention_injector.py --mode lock_sandbox --conns 4 --duration 30 --lock-hold-sec 10
  # 机制对照: items 只读事务 FOR UPDATE 持锁(行 X 锁, MVCC 下不阻塞普通 SELECT 读者, 绝不写 items)
  python db_contention_injector.py --mode lock_items --conns 4 --duration 30 --lock-hold-sec 10
  # 主测(TASK-U 阻塞型表锁共因): LOCK TABLES items WRITE 表级写锁,「锁 Ns / 放 Ms」循环,
  #   广谱阻塞 items 的一切访问(含普通 SELECT 读者)→ 表作用域共因, 零数据修改(连接关闭即释放)。
  #   WRITE 锁同一时刻仅一持有者, 故硬钳 conns=1。
  python db_contention_injector.py --mode lock_table --lock-table items --duration 30 --lock-hold-sec 6 --release-gap-sec 2
  # 沙箱版(先验通路/释放正确性, 不碰 items): 对 chaos_lock_sandbox 表打表锁
  python db_contention_injector.py --mode lock_sandbox_table --lock-table chaos_lock_sandbox --duration 12 --lock-hold-sec 3 --release-gap-sec 2

可被 import(供 harness 后台线程启动/停止):
  inj = DbContentionInjector(mode="heavy_read", conns=8, rand_limit=1000)
  inj.start()                # 非阻塞, 后台线程铺开 N 连接
  ...                        # measure 窗
  inj.stop()                 # 置停止 Event + join + 清理
  # 或一把梭:
  DbContentionInjector(mode="sleep", conns=4).run_for(20)

本脚本是 plan §2.4.0 / G2 闸门「只读传导预实验」的 DB 端注入侧。
绕 Clash: 本脚本只连 MySQL(127.0.0.1:3306), 不发 HTTP, 无需代理处理。
"""
import argparse
import os
import re
import sys
import threading
import time
import traceback

try:
    import mysql.connector
except Exception as e:  # pragma: no cover
    print(f"[FATAL] mysql.connector 不可用: {e}", file=sys.stderr)
    raise

# ------------------------------------------------------------------
# .env 读取 (DB_* 变量)。优先 python-dotenv, 失败则手解析根 .env, 不硬编码密码。
# ------------------------------------------------------------------
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
ENV_PATH = os.path.join(ROOT, ".env")


def _load_env():
    cfg = {}
    try:
        from dotenv import dotenv_values  # python-dotenv 若装了优先用
        cfg = {k: v for k, v in dotenv_values(ENV_PATH).items() if v is not None}
    except Exception:
        # 手解析 KEY=VALUE 行(忽略注释/空行)
        try:
            with open(ENV_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, _, v = line.partition("=")
                    cfg[k.strip()] = v.strip()
        except Exception as e:
            print(f"[WARN] 读 .env 失败({e}), 回退环境变量", file=sys.stderr)
    # 环境变量覆盖 .env
    for k in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"):
        if os.environ.get(k):
            cfg[k] = os.environ[k]
    return cfg


def db_params():
    """返回 mysql.connector.connect(**kwargs) 的连接参数(从 .env 的 DB_* 读, 不硬编码密码)。"""
    cfg = _load_env()
    host = cfg.get("DB_HOST", "127.0.0.1")
    # 项目约定 service->127.0.0.1; localhost 走 IPv6 惩罚, 故强制 127.0.0.1
    if host in ("localhost", "::1", ""):
        host = "127.0.0.1"
    return {
        "host": host,
        "port": int(cfg.get("DB_PORT", "3306")),
        "user": cfg.get("DB_USER", "root"),
        "password": cfg.get("DB_PASSWORD", ""),
        "database": cfg.get("DB_NAME", "shopify2"),
        # [SAFE-4] 连接/读超时, 防连接泄露/卡死
        "connection_timeout": 10,
    }


# ------------------------------------------------------------------
# 重只读查询模板 (recon 验证: 100% 纯 SELECT, 全扫+filesort/groupby, EXPLAIN 已核)
# SQL_NO_CACHE 强制每次真算, 不吃 query cache(MySQL8 已无 query cache, 仍保留意图清晰)
# ------------------------------------------------------------------
def _heavy_read_sql(mode_param, limit):
    if mode_param == "group_by":
        # recon 实测 ~9.5s/次, 杀伤最大: GROUP BY 全表 + COUNT/AVG + filesort
        return ("SELECT SQL_NO_CACHE category, COUNT(*) AS cnt, AVG(price) AS avg_price "
                "FROM items GROUP BY category ORDER BY cnt DESC LIMIT %s")
    if mode_param == "filesort":
        # 全扫 + 多列 DESC filesort, 取 limit 行
        return ("SELECT SQL_NO_CACHE id, item_id, title, price FROM items "
                "ORDER BY rating DESC, review_count DESC LIMIT %s")
    # 默认 rand: 全扫 + RAND() 强制 O(n log n) 排序(无法用索引)
    return ("SELECT SQL_NO_CACHE id, item_id, title, price FROM items "
            "ORDER BY RAND() LIMIT %s")


# ------------------------------------------------------------------
# [SAFE-1] 业务表零写守门: 防回归 —— 模块加载/运行时静态扫描本文件「会被执行的」
# SQL, 一旦出现对业务表的写动词即拒跑(sandbox CREATE/seed 是唯一白名单)。
# ------------------------------------------------------------------
_WRITE_VERBS = ("INSERT", "UPDATE", "DELETE", "REPLACE", "TRUNCATE", "DROP", "ALTER")
_SANDBOX_TABLE = "chaos_lock_sandbox"


def _assert_sql_safe(sql, allow_sandbox_write=False):
    """对单条 SQL 做写动词检测。allow_sandbox_write=True 仅放行 sandbox 表的
    CREATE TABLE IF NOT EXISTS / INSERT ... chaos_lock_sandbox(seed)。

    注意: `SELECT ... FOR UPDATE` 是只读锁(非写), 其中的 UPDATE 是锁子句关键字,
    须在写动词扫描前剥除 `FOR UPDATE` / `LOCK IN SHARE MODE`, 否则会被误判。"""
    up = " " + sql.upper().strip() + " "
    # 剥除只读锁子句(FOR UPDATE / FOR SHARE / LOCK IN SHARE MODE)再做写动词扫描
    scan = re.sub(r"\bFOR\s+UPDATE\b", " ", up)
    scan = re.sub(r"\bFOR\s+SHARE\b", " ", scan)
    scan = re.sub(r"\bLOCK\s+IN\s+SHARE\s+MODE\b", " ", scan)
    # CREATE 仅允许 sandbox 表
    if "CREATE TABLE" in scan:
        if allow_sandbox_write and _SANDBOX_TABLE.upper() in scan:
            return
        raise AssertionError(f"[SAFE-1] 拒绝非 sandbox 的 CREATE: {sql[:80]}")
    for verb in _WRITE_VERBS:
        if re.search(r"\b" + verb + r"\b", scan):
            if allow_sandbox_write and verb == "INSERT" and _SANDBOX_TABLE.upper() in scan:
                continue  # 唯一允许的写: seed sandbox 表
            raise AssertionError(f"[SAFE-1] 检出写动词 {verb}, 拒绝执行业务表写: {sql[:80]}")


# ------------------------------------------------------------------
# 注入器主体
# ------------------------------------------------------------------
class DbContentionInjector:
    VALID_MODES = ("sleep", "heavy_read", "lock_sandbox", "lock_items",
                   "lock_table", "lock_sandbox_table")
    # 表级写锁模式: WRITE 锁同一时刻仅一持有者, 多连接只会互相排队无意义, 故硬钳 conns=1
    _TABLE_LOCK_MODES = ("lock_table", "lock_sandbox_table")
    # lock_table 可锁的表白名单(防 CLI 注入任意表名): 主测 items + 沙箱表
    _LOCK_TABLE_WHITELIST = ("items", _SANDBOX_TABLE)

    def __init__(self, mode, conns=4, sleep_sec=2.0, rand_limit=1000,
                 heavy_kind="rand", lock_hold_sec=10.0, lock_items_ids=None,
                 max_conns=150, reserve=50, stmt_timeout_ms=30000,
                 lock_table="items", release_gap_sec=2.0, verbose=True):
        assert mode in self.VALID_MODES, f"未知 mode: {mode}"
        self.mode = mode
        self.conns = int(conns)
        # [SAFE-4] 单次时长上限钳制
        self.sleep_sec = max(0.2, min(float(sleep_sec), 30.0))
        self.lock_hold_sec = max(0.2, min(float(lock_hold_sec), 30.0))
        # [SAFE-4] 放锁段时长复用 0.2~30s 钳制(锁 Ns / 放 Ms 循环, 让活栈周期性恢复)
        self.release_gap_sec = max(0.2, min(float(release_gap_sec), 30.0))
        self.rand_limit = int(rand_limit)
        self.heavy_kind = heavy_kind  # rand | filesort | group_by
        # lock_items 默认锁一小撮固定 id(只读 FOR UPDATE), 绝不写
        self.lock_items_ids = lock_items_ids or [1, 2, 3, 4, 5]
        # lock_table 锁的表名: 必须落在白名单(非用户拼接), 否则拒构造
        self.lock_table = str(lock_table)
        if mode in self._TABLE_LOCK_MODES and self.lock_table not in self._LOCK_TABLE_WHITELIST:
            raise AssertionError(
                f"[SAFE-1] lock_table='{self.lock_table}' 不在白名单 "
                f"{self._LOCK_TABLE_WHITELIST}, 拒绝对任意表加表锁")
        # 表级写锁硬钳 conns=1(WRITE 锁仅一持有者, 多连接互相排队无意义)
        if mode in self._TABLE_LOCK_MODES and self.conns != 1:
            if verbose:
                print(f"[injector] {mode}: WRITE 锁单持有者, 硬钳 conns={self.conns}->1",
                      flush=True)
            self.conns = 1
        self.max_conns = int(max_conns)
        self.reserve = int(reserve)
        # [SAFE-4] max_execution_time 单位 ms; sleep/lock 模式需 > 单次时长
        self.stmt_timeout_ms = int(stmt_timeout_ms)
        self.verbose = verbose

        self._stop = threading.Event()
        self._threads = []
        self._conn_ids = []          # [SAFE-2] 记录每连接 CONNECTION_ID() 供兜底 KILL
        self._conn_ids_lock = threading.Lock()
        self._errors = []
        self._started = False
        # [SAFE-2 强化] 生命周期锁: 保护 _started 的检查+置位, 使 start()/stop()
        # 幂等可重入。防 harness 误用(如 run_for 自带 stop 与主线程再调 stop)导致
        # 两个 stop() 并发进入 _kill_residual_sessions/join 的收尾竞态。
        self._lifecycle_lock = threading.Lock()

    # -------- 工具 --------
    def _log(self, *a):
        if self.verbose:
            print("[injector]", *a, flush=True)

    def _connect(self):
        cn = mysql.connector.connect(**db_params())
        cur = cn.cursor()
        # [SAFE-4] 会话级语句超时(仅 SELECT 受 max_execution_time 约束, 单位 ms)
        try:
            cur.execute("SET SESSION max_execution_time = %s", (self.stmt_timeout_ms,))
        except Exception:
            pass  # 老版本不支持则忽略, 仍有 connection_timeout 兜底
        # [SAFE-4 强化] 服务端兜底超时: 防进程被 SIGKILL/外部 kill 硬杀后, 遗弃的
        # 空闲未提交事务在 Python 侧 finally/stop() 都跑不到的情况下, 仍持 items 行
        # X 锁直到默认 wait_timeout(常 28800s/8h)才被回收, 期间阻塞对热门商品行的业务写。
        #   innodb_lock_wait_timeout: 获取行锁(SELECT ... FOR UPDATE)最多等 5s, 而非默认 50s,
        #     并让本注入器自身抢不到锁时也不无限等。
        #     注意: innodb_lock_wait_timeout 只管 *InnoDB 行锁* 的获取等待, 对
        #     *表级锁(LOCK TABLES)/MDL 元数据锁* 的等待 **不适用** —— lock_table 模式下
        #     被表锁阻塞的读者不会被它限住, 别误以为它能兜住表锁。
        #   wait_timeout: 空闲会话被服务端在 lock_hold_sec+10(下限 60)秒内自动断开,
        #     断开即回滚未提交事务、释放所遗弃的行锁, 数十秒内回收而非 8h。
        #     对 lock_table 同样有效: 锁段内 self._stop.wait(lock_hold_sec) 期间, 该
        #     持表锁会话对服务端是 idle, wait_timeout 适用 —— 进程硬杀后该会话在
        #     max(60, hold+10)s 内被断连, LOCK TABLES 随连接关闭自动释放(遗弃锁兜底)。
        try:
            cur.execute("SET SESSION innodb_lock_wait_timeout = 5")
        except Exception:
            pass
        try:
            idle_timeout = max(60, int(self.lock_hold_sec) + 10)
            cur.execute("SET SESSION wait_timeout = %s", (idle_timeout,))
        except Exception:
            pass
        # READ COMMITTED: 避免 phantom, 锁生命周期透明(safety_notes 推荐)
        try:
            cur.execute("SET SESSION transaction_isolation = 'READ-COMMITTED'")
        except Exception:
            pass
        cur.execute("SELECT CONNECTION_ID()")
        cid = cur.fetchone()[0]
        cur.close()
        with self._conn_ids_lock:
            self._conn_ids.append(cid)
        return cn, cid

    # -------- [SAFE-3] 连接预算闸门 --------
    def preflight(self):
        """启动前核对连接预算; conns 超预算则抛错拒跑。返回 (current_threads, limit)。"""
        cn = mysql.connector.connect(**db_params())
        try:
            cur = cn.cursor()
            cur.execute("SHOW STATUS LIKE 'Threads_connected'")
            current = int(cur.fetchone()[1])
            cur.execute("SHOW VARIABLES LIKE 'max_connections'")
            maxc = int(cur.fetchone()[1])
            cur.close()
        finally:
            try:
                cn.close()
            except Exception:
                pass
        budget = maxc - current - self.reserve
        if self.conns > self.max_conns:
            raise AssertionError(
                f"[SAFE-3] conns={self.conns} 超硬上限 max_conns={self.max_conns}, 拒跑")
        if self.conns >= budget:
            raise AssertionError(
                f"[SAFE-3] conns={self.conns} >= 预算 {budget} "
                f"(max={maxc} - current={current} - reserve={self.reserve}), 拒跑")
        self._log(f"preflight OK: conns={self.conns} < budget={budget} "
                  f"(max_connections={maxc}, current={current}, reserve={self.reserve})")
        return current, maxc

    # -------- 各模式单连接 worker --------
    def _worker_sleep(self, cn):
        cur = cn.cursor()
        sql = "SELECT SLEEP(%s)"
        _assert_sql_safe(sql)
        while not self._stop.is_set():
            cur.execute(sql, (self.sleep_sec,))
            cur.fetchall()
        cur.close()

    def _worker_heavy(self, cn):
        cur = cn.cursor()
        sql = _heavy_read_sql(self.heavy_kind, self.rand_limit)
        _assert_sql_safe(sql)  # [SAFE-1] 纯 SELECT 校验
        while not self._stop.is_set():
            cur.execute(sql, (self.rand_limit,))
            cur.fetchall()  # 拉完结果集, 真实占用
        cur.close()

    def _worker_lock_sandbox(self, cn):
        cur = cn.cursor()
        # [SAFE-1] sandbox 表是唯一允许写的表(CREATE IF NOT EXISTS + seed)
        create_sql = (f"CREATE TABLE IF NOT EXISTS {_SANDBOX_TABLE} ("
                      "lock_id INT PRIMARY KEY, lock_name VARCHAR(100)) ENGINE=InnoDB")
        _assert_sql_safe(create_sql, allow_sandbox_write=True)
        cur.execute(create_sql)
        cn.commit()
        for lid in (1, 2):
            seed_sql = (f"INSERT IGNORE INTO {_SANDBOX_TABLE}(lock_id, lock_name) "
                        "VALUES (%s, %s)")
            _assert_sql_safe(seed_sql, allow_sandbox_write=True)
            cur.execute(seed_sql, (lid, f"chaos_seed_{lid}"))
        cn.commit()
        lock_sql = f"SELECT * FROM {_SANDBOX_TABLE} WHERE lock_id = 1 FOR UPDATE"
        _assert_sql_safe(lock_sql)  # SELECT...FOR UPDATE 无写动词
        while not self._stop.is_set():
            cn.start_transaction()
            cur.execute(lock_sql)
            cur.fetchall()  # 持 X 锁
            # 持锁 lock_hold_sec, 但分片检查停止信号
            self._stop.wait(self.lock_hold_sec)
            cn.rollback()  # [SAFE-2] 只读事务以 rollback 收尾, 释放锁
        cur.close()

    def _worker_lock_items(self, cn):
        cur = cn.cursor()
        # [SAFE-1] 仅对 items 做 SELECT ... FOR UPDATE(持锁), 绝不写 items
        placeholders = ",".join(["%s"] * len(self.lock_items_ids))
        lock_sql = f"SELECT id FROM items WHERE id IN ({placeholders}) FOR UPDATE"
        _assert_sql_safe(lock_sql)  # 含 FOR UPDATE 但无写动词, 通过
        while not self._stop.is_set():
            cn.start_transaction()
            cur.execute(lock_sql, tuple(self.lock_items_ids))
            cur.fetchall()
            self._stop.wait(self.lock_hold_sec)
            cn.rollback()  # 只读事务 rollback 释放锁, items 数据不变
        cur.close()

    def _ensure_sandbox(self, cn, cur):
        """[SAFE-1] 仅 lock_sandbox_table 用: CREATE IF NOT EXISTS + seed 沙箱表
        (chaos_lock_sandbox 是唯一允许写的表, 过 allow_sandbox_write 白名单)。"""
        create_sql = (f"CREATE TABLE IF NOT EXISTS {_SANDBOX_TABLE} ("
                      "lock_id INT PRIMARY KEY, lock_name VARCHAR(100)) ENGINE=InnoDB")
        _assert_sql_safe(create_sql, allow_sandbox_write=True)
        cur.execute(create_sql)
        cn.commit()
        for lid in (1, 2):
            seed_sql = (f"INSERT IGNORE INTO {_SANDBOX_TABLE}(lock_id, lock_name) "
                        "VALUES (%s, %s)")
            _assert_sql_safe(seed_sql, allow_sandbox_write=True)
            cur.execute(seed_sql, (lid, f"chaos_seed_{lid}"))
        cn.commit()

    def _worker_lock_table(self, cn):
        """[SAFE-1] 表级写锁:「锁 lock_hold_sec / 放 release_gap_sec」循环, 广谱阻塞
        被锁表的一切访问(含普通 SELECT 读者)→ 表作用域共因, 零数据修改。

        机制要点:
          * LOCK TABLES <t> WRITE 是 *会话级* 表锁, WRITE 锁同一时刻仅一持有者
            (故本模式硬钳 conns=1), 阻塞其他会话对该表的读与写直到 UNLOCK。
          * 不写任何数据(CHECKSUM 可证); 连接关闭即自动释放该会话所有表锁(遗弃锁兜底)。
          * UNLOCK TABLES 必须由 *持锁的同一连接 cn* 执行 —— 表锁会话级别, 别的会话
            UNLOCK 无效。本 worker 与 _run_one_conn.finally 均只用同一 cn 兜底 UNLOCK。
          * 隐式提交说明: mysql.connector 下执行 LOCK TABLES 时若连接处于隐式开启的
            事务中会触发隐式 COMMIT; 本会话纯只读(无任何写)故隐式提交无副作用 ——
            但若日后误在本 worker 加写操作, 该写会被 LOCK TABLES 的隐式提交意外落库,
            务必保持本会话零写。"""
        cur = cn.cursor()
        if self.mode == "lock_sandbox_table":
            self._ensure_sandbox(cn, cur)
        # 表名来自白名单(__init__ 已校验), 反引号包裹防保留字
        lock_sql = "LOCK TABLES `" + self.lock_table + "` WRITE"
        unlock_sql = "UNLOCK TABLES"
        _assert_sql_safe(lock_sql)    # LOCK TABLES 不含写动词, 通过
        _assert_sql_safe(unlock_sql)  # UNLOCK TABLES 不含写动词, 通过
        while not self._stop.is_set():
            cur.execute(lock_sql)      # 上表锁(可能触发隐式 COMMIT; 本会话只读无副作用)
            self._stop.wait(self.lock_hold_sec)   # 锁段: 持锁 lock_hold_sec(可被 stop 提前打断)
            cur.execute(unlock_sql)    # [SAFE-2] 放锁, 必须同一 cn 执行
            self._stop.wait(self.release_gap_sec)  # 放锁段: 活栈周期性恢复
        cur.close()

    def _run_one_conn(self):
        """单连接生命周期: 连接 -> 跑 mode worker -> [SAFE-2] finally ROLLBACK+close。"""
        cn = None
        cid = None
        try:
            cn, cid = self._connect()
            dispatch = {
                "sleep": self._worker_sleep,
                "heavy_read": self._worker_heavy,
                "lock_sandbox": self._worker_lock_sandbox,
                "lock_items": self._worker_lock_items,
                "lock_table": self._worker_lock_table,
                "lock_sandbox_table": self._worker_lock_table,
            }[self.mode]
            dispatch(cn)
        except Exception as e:
            self._errors.append(f"conn(cid={cid}) {type(e).__name__}: {e}")
            if self.verbose:
                traceback.print_exc()
        finally:
            # [SAFE-2] 无论如何释放任何残留锁/事务。顺序固定:
            #   表锁兜底 UNLOCK(同一 cn) -> ROLLBACK -> close。
            if cn is not None:
                # [SAFE-2] 表锁会话级: 兜底 UNLOCK 必须用 *同一持锁连接 cn* 执行,
                # 不得新开连接(别的会话 UNLOCK 无效)。仅对表锁模式做, 失败静默。
                if self.mode in self._TABLE_LOCK_MODES:
                    try:
                        cur = cn.cursor()
                        cur.execute("UNLOCK TABLES")
                        cur.close()
                    except Exception:
                        pass
                try:
                    cn.rollback()
                except Exception:
                    pass
                try:
                    cn.close()
                except Exception:
                    pass

    # -------- 生命周期 API --------
    def start(self):
        """非阻塞: preflight -> 铺开 N 个 worker 线程。幂等(锁内检查+置位)。"""
        with self._lifecycle_lock:
            if self._started:
                return
            self.preflight()  # [SAFE-3]
            self._stop.clear()
            self._errors = []
            self._conn_ids = []
            self._threads = []
            for i in range(self.conns):
                t = threading.Thread(target=self._run_one_conn, name=f"inj-{self.mode}-{i}",
                                     daemon=True)
                t.start()
                self._threads.append(t)
            self._started = True
            threads = list(self._threads)
        self._log(f"started: mode={self.mode} conns={len(threads)}")

    def stop(self, join_timeout=15):
        """置停止 Event -> join -> [SAFE-2] 兜底 KILL 仍活着的自己会话。

        幂等可重入: 用 _lifecycle_lock 包住 _started 检查+置位, 第二次/并发调用
        被早退挡住, 杜绝两个 stop() 并发进入 _kill_residual_sessions/join 的收尾竞态。

        对 lock_table 的释放语义: _stop.set() 后 worker 在锁/放循环顶检测到停止即
        自身用同一 cn 执行 UNLOCK(join_timeout 内 worker 自身 UNLOCK 优先, 这是最干净
        的释放); 若 worker 卡死 join 超时, _kill_residual_sessions 对记录的
        CONNECTION_ID 发 KILL CONNECTION —— KILL 断会话即释放其持有的表锁。若被 KILL
        的正是唯一持 WRITE 锁的会话, 释放后被锁表(items)立即恢复(正是期望)。"""
        with self._lifecycle_lock:
            if not self._started:
                return
            # 立即置 False, 使任何并发/后续 stop() 在锁释放后即被早退挡住,
            # 真正的收尾(join + 兜底 KILL)只由本次调用执行一次。
            self._started = False
            self._stop.set()
            threads = list(self._threads)
        for t in threads:
            t.join(timeout=join_timeout)
        self._kill_residual_sessions()
        self._log(f"stopped. errors={len(self._errors)}")
        for e in self._errors[:10]:
            self._log("  err:", e)

    def _kill_residual_sessions(self):
        """[SAFE-2] 兜底: 对本注入器开的、可能仍活着的会话发 KILL CONNECTION,
        防止线程卡死时连接/锁泄露。用一条独立连接执行。
        注: KILL CONNECTION 会断掉被杀会话并释放其持有的一切锁, 含 lock_table 的
        会话级表锁 —— 杀掉唯一持 WRITE 锁的会话后, 被锁表立即恢复(期望行为)。"""
        with self._conn_ids_lock:
            cids = list(self._conn_ids)
        if not cids:
            return
        try:
            cn = mysql.connector.connect(**db_params())
            cur = cn.cursor()
            # 取当前仍存活的 processlist id
            cur.execute("SELECT ID FROM information_schema.PROCESSLIST")
            alive = {row[0] for row in cur.fetchall()}
            killed = 0
            for cid in cids:
                if cid in alive:
                    try:
                        cur.execute("KILL CONNECTION %s" % int(cid))  # int 注入安全
                        killed += 1
                    except Exception:
                        pass
            cur.close()
            cn.close()
            if killed:
                self._log(f"[SAFE-2] killed {killed} residual session(s)")
        except Exception as e:
            self._log(f"[SAFE-2] residual kill skipped: {e}")

    def run_for(self, duration):
        """阻塞便捷接口: start -> 等 duration 秒(可被 stop Event 提前打断) -> stop。"""
        self.start()
        try:
            self._stop.wait(timeout=duration)
        finally:
            self.stop()
        return {"mode": self.mode, "conns": self.conns, "errors": list(self._errors)}


# ------------------------------------------------------------------
# [SAFE-5] CHECKSUM 工具(供 harness 窗前后核对数据未变)
# ------------------------------------------------------------------
def checksum_tables(tables=("items", "inventory")):
    """对给定表跑 CHECKSUM TABLE, 返回 {table: checksum}。纯只读。"""
    out = {}
    cn = mysql.connector.connect(**db_params())
    try:
        cur = cn.cursor()
        for t in tables:
            # 表名来自固定白名单, 非用户输入
            cur.execute(f"CHECKSUM TABLE {t}")
            row = cur.fetchone()
            out[t] = int(row[1]) if row and row[1] is not None else None
        cur.close()
    finally:
        cn.close()
    return out


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------
def _build_argparser():
    p = argparse.ArgumentParser(description="安全只读 DB 争用注入器(形态B 共因预实验)")
    p.add_argument("--mode", required=False, default=None,
                   choices=list(DbContentionInjector.VALID_MODES),
                   help="注入模式; --checksum 自检时可省略")
    p.add_argument("--conns", type=int, default=4, help="并发连接数(默认4, 硬上限 --max-conns)")
    p.add_argument("--duration", type=float, default=30.0, help="注入持续秒(CLI 独跑用)")
    p.add_argument("--sleep-sec", type=float, default=2.0, help="sleep 模式单次 SLEEP 秒(<=30)")
    p.add_argument("--rand-limit", type=int, default=1000, help="heavy_read LIMIT 行数")
    p.add_argument("--heavy-kind", default="rand", choices=("rand", "filesort", "group_by"),
                   help="heavy_read 子类型(group_by 最重 ~9.5s/次)")
    p.add_argument("--lock-hold-sec", type=float, default=10.0,
                   help="lock_* 模式单次持锁秒(<=30; lock_table 建议 5~8s)")
    p.add_argument("--lock-table", default="items",
                   help="lock_table/lock_sandbox_table 模式锁的表名(白名单: items, "
                        + _SANDBOX_TABLE + ")")
    p.add_argument("--release-gap-sec", type=float, default=2.0,
                   help="lock_table「锁 Ns/放 Ms」循环的放锁段秒(0.2~30, 默认2)")
    p.add_argument("--max-conns", type=int, default=150, help="连接数硬上限")
    p.add_argument("--reserve", type=int, default=50, help="为业务留出的连接余量")
    p.add_argument("--stmt-timeout-ms", type=int, default=30000,
                   help="会话 max_execution_time(ms)")
    p.add_argument("--checksum", action="store_true",
                   help="只跑一次 items/inventory CHECKSUM 并退出(自检用)")
    return p


def main(argv=None):
    args = _build_argparser().parse_args(argv)
    if args.checksum:
        print("CHECKSUM:", checksum_tables())
        return 0
    if args.mode is None:
        print("[FATAL] 非 --checksum 自检时必须指定 --mode", file=sys.stderr)
        return 2

    inj = DbContentionInjector(
        mode=args.mode, conns=args.conns, sleep_sec=args.sleep_sec,
        rand_limit=args.rand_limit, heavy_kind=args.heavy_kind,
        lock_hold_sec=args.lock_hold_sec, max_conns=args.max_conns,
        reserve=args.reserve, stmt_timeout_ms=args.stmt_timeout_ms,
        lock_table=args.lock_table, release_gap_sec=args.release_gap_sec, verbose=True,
    )
    # [SAFE-5] 窗前后 CHECKSUM 核对(独跑也做, 多一道保险)
    before = checksum_tables()
    print("[checksum before]", before)
    t0 = time.time()
    res = inj.run_for(args.duration)
    print(f"[done] {res} elapsed={time.time()-t0:.1f}s")
    after = checksum_tables()
    print("[checksum after ]", after)
    if before != after:
        print("[ALARM] CHECKSUM 变化! 业务表数据被改动(不应发生), 请排查!")
        return 2
    print("[OK] CHECKSUM 一致, 业务表数据未变。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
