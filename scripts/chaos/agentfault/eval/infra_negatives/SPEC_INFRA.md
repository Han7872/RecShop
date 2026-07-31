# BARO / RCD / Eadro on agentfault —— 诚实负例规格(SPEC_INFRA)

> 目的:上游点名的成熟 metric-RCA 方法(BARO / RCD / Eadro)在 agentfault(agent 语义故障)上
> 按 RCAEval 惯例出**诚实负例**——"试了、如何退化、为什么退化"如实报,强于跳过。
> 预期结论(黑板 2026-07-16 裁决):Eadro 结构性不能跑;BARO 只剩 stage-2 的分布检验半;
> RCD 能跑但退化。**本工作是把裁决变成可复现的数字与代码。**
>
> 位置:`scripts/chaos/agentfault/eval/infra_negatives/`。产物落 `(v1)infra_negatives/`。

## 0. 铁律

- 不改 `third_party/**`(RCAEval/Eadro/_cl_patched 只读)、`services/**`、`scripts/chaos/ctk/*`。
- **不 pip install**(matplotlib + causal-learn==0.1.2.3 已在 recweb2,先验证 import;版本断言 0.1.2.3)。
- conda python = `python3`;PYTHONIOENCODING=utf-8;所有 open utf-8。
- `(v1)dataset_agentfault.csv` 只读。从**仓库根**运行(相对路径惯例同 third_party/ pilots)。
- RCD 前置:`sys.path.insert(0,"third_party/_cl_patched")` 在任何 causallearn import 前;
  `assert "third_party" in causallearn.__file__`(照 `third_party/rcd_pilot.py` L14-20 模式)。
- 方法实现**单文件加载 vendored 原版**(`RCAEval/RCAEval/e2e/baro.py` / `rcd.py`,
  spec_from_file_location 绕 e2e/__init__ 的 sknetwork,照 pilots 模式),不复制方法体。
- 随机性:numpy/random seed=0;RCD 多 seed(0-4)报均值±区间(照 rcd_pilot 的种子稳定性关注点)。

## 1. 数据与实体

- 输入 = `(v1)dataset_agentfault.csv`(72 行:64 faulted + 8 normal)。
- 候选实体 = 4 agent(`injector_smoke.AGENT_NAMES` 序,或硬编码同序;别引 injector_smoke——它
  import 有 env 副作用,这里硬编码 + 注释来源即可)。
- **特征通道(eval 铁律:用 corrected)**:主通道 `span_<A>_duration_corrected_ms`。
  对照通道(污染实验,只作叙事)`span_<A>_duration_ms`(raw)。
  可选补充通道:`span_<A>_child_max_duration_ms`、`span_<A>_child_httpx_count`(若方法吃多列)。
- GT = `root_cause_set`(单根);faulted 判定 = `injected==1`。fault-family = `kind`。

## 2. 方法与诚实适配(每个方法的"能跑到哪一步"必须如实)

### 2a. BARO(RCAEval `e2e/baro.py` 原版函数)
- **结构性缺口(必须写进结果)**:BARO = BOCPD 变点检测(stage-1)+ RobustScorer 排名(stage-2),
  输入是**case 内多变量时序**。agentfault 每 case 每 agent 只有 1 个标量(agent 只跑一次)→
  **case 内时序不存在,stage-1 结构性不可用**。
- **适配(尽量喂原版函数)**:构造 case 级伪时序 DataFrame——行 = 8 个 normal case(before 段)
  + 1 个目标 faulted case(after 段),列 = 4 agent 的 duration_corrected(列名给 RCAEval 习惯的
  `<agent>_latency` 形),`inject_time`/anomaly 边界显式给在 8/9 行处(= "变点已知"设定,BOCPD 被
  旁路——RCAEval 支持显式 inject_time 的跑法;读 baro.py 签名按实际参数喂)。每 faulted case 跑一次
  → 得 per-case 4 agent 排名。
- **标注**:方法名报 "BARO-stage2 (adapted: case-level pseudo-series, change-point given)";
  绝不简写成 "BARO"。若 baro.py 的函数形状不容许该喂法(如强依赖行数>阈值),如实降级为
  "RobustScorer 公式手算 analog"(median/IQR z-score vs 8 normal),并在结果里写清是 analog 非原版。
- **污染对照**:同一流程再跑一遍 raw duration 通道,并排报 corrected vs raw(预期:raw 在
  hallucinate 族借注入伪影得分,corrected 掉回随机 —— 与 tierA 平凡启发式呼应)。

### 2b. RCD(RCAEval `e2e/rcd.py` 原版 + _cl_patched)
- **结构性缺口**:RCD 要 normal/anomalous 两段**多行样本**做 χ² CI 因果发现。case 内 1 行 →
  per-case 跑法结构性不可用(试一次,如实记录失败形态:异常/空排名/常量)。
- **适配**:族级聚合——normal_df = 8 normal 行,anomalous_df = 该 fault-family 全部 case 行
  (hallu 24 / wrongpick 8 / format 32),列 = 4 agent duration_corrected。每族一次 → 族级排名,
  按 RCAEval per-case 打分惯例把该排名赋给族内每个 case(如实标"族级单排名摊派")。
- seeds 0-4;报排名稳定性(rcd_pilot 的关注点)。causal-learn 0.1.2.3 断言。
- **标注**:"RCD (adapted: family-pooled case-level samples; per-case structurally N/A)"。

### 2c. Eadro —— 不跑,写清为什么(结果文档一节)
- 需:case 内时序 + 服务依赖图(从 trace 抽)+ 监督训练。agentfault:无 case 内时序、
  4-agent 固定顺序流水线无隐藏拓扑、72 case 不够其训练范式;故**结构性 N/A**,只写文档不硬跑。
  (另注 K8S 线曾有 Eadro 泄漏切分教训,见 `(project docs)/eadro/`。)

## 3. 打分与输出

- MRCBench(`m9_score.mrcbench`,import 照 tierA 的 stdout-rewrap 坑处理)per fault-family ×
  overall;**Hit@1 头条**,@3/@5 天花板伪影脚注(与 whowhen scorer 同一套措辞惯例)。
- 对照行:Random(0.25)+ always-Synthesizer 常量基线(与 whowhen scorer 口径一致,重算一遍进同表)。
- 输出:`(v1)infra_negatives/infra_negatives_results.json`(per method × family,
  含适配标注串 + per-case 明细)+ `RESULTS_INFRA_NEGATIVES.md`(表 + 三方法"能跑到哪一步"叙事 +
  Eadro N/A 节 + 脚注)。
- 脚注必带:①corrected 通道 = 扣除注入开销后(EVAL_NOTES §4a);②BARO/RCD 均为 adapted 非原版
  全流程,适配点逐条列;③agent 语义故障本设计上不扰动 infra 标量(除已扣除的注入开销)→
  负例是**结论**不是失败;④raw 对照的伪影解读。

## 4. 脚本

- `run_infra_negatives.py`:一个入口,`--method {baro,rcd,all}`、`--channel {corrected,raw,both}`、
  `--out-dir`(默认正式区)。离线纯本地(无 API、无服务依赖),幂等(重跑覆盖,结果确定性)。
- 自检:①normal 基线统计与 CSV 一致(8 行);②per-case 排名恰含 4 agent 无重复;③RCD 断言
  causallearn 路径 + 版本;④GT 覆盖 64/64。
