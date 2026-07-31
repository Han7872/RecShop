# Who&When baseline on agentfault —— 设计规格(SPEC)

> 目的:在 agentfault 数据集(64 faulted case)上跑 **Who&When**(ICML2025 Spotlight,
> `ag2ai/Agents_Failure_Attribution`)的 3 个 LLM-judge 方法(All-at-Once / Step-by-Step /
> Binary Search)+ **A2P**(姊妹工作,同 JSON 格式,`--a2p` 增强 prompt),LLM = DeepSeek,
> 用 MRCBench(`scripts/chaos/ctk/m9_score.py`)打分。**方法代码零改动 vendored 运行**
> (只鸭子类型换 client),保证方法保真度。
>
> 位置:`scripts/chaos/agentfault/eval/whowhen/`。产物落 `(v1)whowhen/`。

## 0. 铁律(实现约束)

- **不改** `third_party/reference/{whowhen,a2p}/` 任何文件(vendored 只读;方法保真度的根)。
- **不改** `services/**`、`scripts/chaos/ctk/*`(m9_score 只 import 不动)。
- 新文件只落 `scripts/chaos/agentfault/eval/whowhen/` + `(v1)whowhen/`。
- **不 pip install**;conda python = `python3`
  (openai / tqdm / python-dotenv 已在 env,先 `python -c "import openai,tqdm,dotenv"` 验证)。
- Windows 控制台是 gbk:任何跑中文内容的进程都要 `PYTHONIOENCODING=utf-8`,
  所有 `open()` 显式 `encoding='utf-8'`。
- `(v1)raw|spans|ledgers/` **只读**。

## 1. 三个脚本

### 1.1 `make_whowhen_cases.py`(adapter,纯离线无 API)

输入:`(v1)raw/*.json`(72 个),过滤 `row.injected==1 and
row.ledger_status=='injected'` → **64 faulted case**(normal 8 个排除:Who&When 预设任务已失败)。

每 case 输出一个 Who&When 格式 JSON:

```json
{
  "question": "<静态任务陈述,所有 case 完全一致>",
  "ground_truth": "N/A (open-ended recommendation task; no single reference answer)",
  "history": [
    {"name": "Sequence_Recommender",      "content": "<conv 文本>"},
    {"name": "User_Behavior_Analyzer",    "content": "<conv 文本>"},
    {"name": "Product_Analyzer",          "content": "<conv 文本>"},
    {"name": "Recommendation_Synthesizer","content": "<conv 文本>\n\n[Tool call] Synthesize_Recommendation(arguments): <raw args 串>"}
  ],
  "mistake_agent": "<row.root_cause_set(单根)>",
  "mistake_step": <注入 agent 在执行序里的下标,0-based,int>,
  "_provenance": {"case_id": "...", "kind": "...", "format_subtype": "...", "trace_id": "..."}
}
```

关键决策(全部有因):

- **执行序** = `injector_smoke.AGENT_NAMES`(Sequence_Recommender → User_Behavior_Analyzer →
  Product_Analyzer → Recommendation_Synthesizer)。raw 的 `resp.conversation` 键是**无下划线
  camel 名**(`SequenceRecommender` 等),映射 = канonical 名去掉下划线。history 顺序固定为执行序
  (conversation dict 是字母序,必须重排)。
- **`name` 用 canonical 下划线名**(和 GT 串一致;whowhen `evaluate.py` 的
  `[\w_]+` 正则 + substring 匹配兼容;4 个名字互不为子串)。
- **Synthesizer 的 content 统一追加 tool-call args 块**(所有 64 case 一致追加,不只 format):
  format_violation 腐蚀的是 `Synthesize_Recommendation.tool_call` 的 **raw arguments 串**
  (ledger `target` 字段实证),conversation 文本不含故障 → 不附 args 就是把故障对 judge 藏起来。
  统一追加防"有无 tool-call 块"本身成泄漏信号。
  取法:spans 文件 = `(v1)spans/<row.scenario_id>.jsonl`(per-combo),
  用 `injector_smoke.read_spans(span_file, row.trace_id)` 过滤本 case,再
  `injector_smoke.synthesizer_toolcall_arg_strings(spans)` 取 raw args 串(可能多条,全附,
  用编号列出)。**若某 case 取不到 args**:统一占位串 `"<no tool-call captured>"` 并计数上报
  (build 报告里必须打印 with-args/without-args 计数;若 without 集中在某一 kind → 停下来报告,
  这就是泄漏)。
  import 方式:`sys.path` 加 `scripts/chaos/agentfault/injector/` 后 `import injector_smoke`
  (它 import 顶层轻量,确认无副作用;只用 `AGENT_NAMES/read_spans/synthesizer_toolcall_arg_strings`)。
- **`question`/`ground_truth` 全 64 case 逐字节一致**(静态陈述,防泄漏)。question 描述任务:
  多 agent 电商推荐流水线,4 个 agent 顺序协作,基于用户历史交互产出一条带解释的个性化推荐。
  ground_truth 用上面的 N/A 句(本任务开放式无唯一参考答案 = Who&When 论文的 w/o ground-truth
  answer 设定;他们代码默认把答案塞进 prompt,我们塞的是 N/A 句,如实记录该设定差异)。
- **文件名不透明化**:输出 `case_001.json` … `case_064.json`(按 case_id 字典序编号,稳定可复现),
  另写 `cases_index.json`(case_NNN → 原 case_id + kind + mistake_agent)。原 case_id 含 GT agent
  名,虽然方法 prompt 不含文件名,不透明化把这条泄漏疑虑焊死。
- `mistake_step` = 注入 agent 在执行序的下标(0-based,和 whowhen step_by_step 的 idx、
  all_at_once prompt 的 step 语义一致;4 agent 各说一次话 → step 定位 ≡ agent 定位)。

输出目录:`(v1)whowhen/cases/`(+ `cases_index.json` 在 `whowhen/` 根)。

**build 自检(脚本末尾必做并打印)**:
1. case 数 == 64;per-kind 计数 == {hallucinate:24, wrong_item_pick:8, format_violation:32}。
2. 所有 case 的 question 串一致、ground_truth 串一致(断言)。
3. history 恰 4 条、顺序 == AGENT_NAMES(断言)。
4. hallucinate case:`row.divergent_needle` 出现在 mistake_agent 那条 content 里(计数上报,
   预期 24/24;needle 空的上报)。
5. wrongpick case:`B00000FAULT` 出现在 Synthesizer content(预期 8/8)。
6. format_malformed case:Synthesizer 的 args 块不可 json.loads(预期 8/8 里 malformed 的那些)。
7. tool-call args 捕获率上报(见上)。

### 1.2 `run_whowhen.py`(方法 harness,调 DeepSeek API)

- **vendored import**:用 `importlib.util.spec_from_file_location` 直接按路径加载
  `third_party/reference/whowhen/Automated_FA/Lib/utils.py`(命名 `ww_utils`)和
  `third_party/reference/a2p/Automated_FA/Lib/utils.py`(命名 `a2p_utils`)——两库模块名同为
  `Lib.utils`,按路径加载避开冲突;**不 import 两库的 inference.py**(有 torch/transformers 依赖)。
- **client**:`openai.OpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_API_BASE)`,
  从仓根 `.env` 读(python-dotenv;和各服务同一套变量,含 `DEEPSEEK_MODEL`,默认 deepseek-chat)。
  whowhen 代码只调 `client.chat.completions.create(...)` → 鸭子类型直接替换 AzureOpenAI,
  **方法函数零改动**。不动代理相关 env(DeepSeek 是外网,走系统默认;NO_PROXY 铁律只适用本机服务注入)。
- **4 个 run**(CLI `--method` 选,`all` 跑全):
  1. `all_at_once`   → `ww_utils.all_at_once(client, cases_dir, is_handcrafted=False, model=MODEL, max_tokens=1024)`
  2. `step_by_step`  → `ww_utils.step_by_step(...同...)`
  3. `binary_search` → `ww_utils.binary_search(...同...)`
  4. `a2p`           → `a2p_utils.all_at_once(client, cases_dir, is_handcrafted=False, model=MODEL, max_tokens=4096, a2p=True)`
     (A2P 论文默认 max_tokens 大;deepseek-chat 输出上限 8k,取 4096;如实记)
- 每 run 把 stdout 重定向到 `(v1)whowhen/outputs/<method>_deepseek.txt`
  (utf-8;whowhen 自己的 inference.py 就是这么干的,复刻其 redirect_stdout 模式,tqdm 在 stderr 不污染)。
- CLI:`--method {all_at_once,step_by_step,binary_search,a2p,all}`、`--cases-dir`(默认正式区,
  可指 smoke 目录)、`--out-dir`、`--max-cases N`(冒烟用:临时目录软链/复制前 N 个 case 跑,不动正式区)。
- 幂等:输出文件存在则默认跳过(`--force` 重跑覆盖);跑完打印该方法输出文件路径 + API 调用计数
  (在 client 外包一层计数 wrapper,不改方法代码)。

### 1.3 `score_whowhen.py`(打分,纯离线)

- 解析 `outputs/*.txt`:**逐字节复用 whowhen `evaluate.py` 的正则口径**
  (`Prediction for ([^:]+\.json):(.*?)(?=Prediction for|\Z)` + `Agent Name:\s*([\w_]+)` +
  `Step Number:\s*(\d+)`)。step_by_step 判"无错"的 case 无预测块 → 记 missing。
- GT 从 `cases/case_NNN.json` 的 `mistake_agent/mistake_step` 读。
- **双口径报告**(都算,并排):
  1. **Who&When 原生口径**:agent accuracy + step accuracy,分母 = 总 case 数(64),
     substring 匹配 —— 和他们 evaluate.py 完全一致(方法保真)。
  2. **MRCBench**(`m9_score.mrcbench`,import 照 `eval_agentfault_tierA.py` 的
     sys.path + stdout-rewrap 坑处理):单预测方法 → ranked = `[pred] + 其余 AGENT_NAMES
     (canonical 序,去重)`;missing/解析失败 → ranked = `[]`(mrcbench 对空排名全 0,已核)。
     **只有 K=1 有判别力**(单预测 + 4 候选;@3/@5 是天花板伪影)——表里全列但加脚注。
  3. **预测名归一化对照**:judge 可能输出 `Product Analyzer`(空格)等变体,原生正则只抓到
     `Product` → 判错。加一列"归一化 Hit@1"(大小写不敏感、空格/下划线等价、全名或唯一前缀匹配
     到 4 canonical 名),与原生口径并排 —— 分歧如实报,不替换原生数。
- 输出:`(v1)whowhen/whowhen_results.json`(per method × per fault-family
  {hallucinate,wrong_item_pick,format_violation} × overall,含 per-case 明细)+ stdout 打
  markdown 表(后续并入 BASELINE_RESULTS.md)。
- family 从 `cases_index.json` 反查。

## 2. 已知诚实事项(写结果时必带,scorer 输出的表格注脚也要带)

- **同源偏置**:judge LLM = DeepSeek = hallucinate 注入副 LLM 同族 → 可能继承盲点/或反向熟悉
  改写风格。EVAL_NOTES §4b 已记;结果表加脚注。换模型族复测留待有第二家 key。
- **wrongpick 哨兵显眼**:`B00000FAULT` 字面含 "FAULT" → LLM-judge 在该族的成绩是**上界**
  (哨兵伪影,不代表隐蔽 wrong-pick 可定位性)。脚注必带。
- **w/o answer 设定**:ground_truth 填 N/A 句(开放式任务无唯一答案),比 Who&When 代码默认
  (把真答案塞 prompt)更难 —— 与其论文 w/o GT-answer 设定对齐,如实记。
- **单预测方法的 MRCBench**:@3/@5/@R 无判别力,K=1 = 原生 agent accuracy(数值应吻合,
  作双口径互验)。
- **binary_search 平局随机**:whowhen 代码对模糊回答随机选半边(`random.randint`),跑前
  `random.seed(0)` 固定(harness 里设,不改他们代码;如实记 seed)。

## 3. 冒烟与全量(主循环亲驱,不在本 workflow 内)

- 冒烟:adapter 全量跑(离线)+ `run_whowhen.py --method all_at_once --max-cases 2` 真 API。
- 全量:4 method × 64 case,预估 API 调用 64+≤256+~128+64 ≈ 500-600 次,顺序跑 ~1-2h,
  主循环 nohup 亲驱 + Monitor。
