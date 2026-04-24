# Alpha Mining 脚本说明

本文档详细说明 `main.py` 的实现功能、执行流程、核心参数、输出结果，以及当前代码中的已知问题与改进建议。

## 1. 脚本目标

该脚本用于自动化完成以下任务：

1. 登录 WorldQuant Brain API。
2. 拉取指定数据集（当前是 `pv13`）下的全部数据字段，并缓存到本地目录。
3. 基于字段批量拼接 Alpha 表达式。
4. 将每个表达式提交到模拟接口执行回测/仿真。
5. 轮询仿真进度，完成后输出返回的 `alpha_id`。

整体上，这是一个“**批量生成并提交 Alpha 仿真任务**”的自动化脚本。

## 2. 依赖与运行环境

脚本依赖：

- `requests`
- `pandas`

标准库依赖：

- `json`
- `os`
- `os.path`
- `time`

## 3. 认证逻辑

脚本启动后会先尝试读取凭证：

1. 优先读取本地文件 `brain_credentials.txt`（通过 `expanduser` 处理路径）。
2. 如果文件不存在，则从环境变量读取：
	- `BRAIN_USERNAME`
	- `BRAIN_PASSWORD`

随后通过 `requests.Session()` 创建会话，使用 `HTTPBasicAuth` 设置认证信息，并调用：

- `POST https://api.worldquantbrain.com/authentication`

脚本会打印认证接口的状态码和返回 JSON，用于调试。

## 4. 数据字段拉取：`get_datafields`

当前这部分逻辑已经从主脚本中拆分出去，放在 [datafields_store.py](datafields_store.py) 中，主脚本通过该模块完成字段拉取与本地落盘。

默认缓存目录为 `datafields_cache/`，目录结构类似：

```text
datafields_cache/
	pv13/
		20260424_153000/
			page_0001.json
			page_0002.json
```

该模块也可以单独运行，例如：

```bash
python datafields_store.py --dataset-id pv13 --output-dir datafields_cache --data-type GROUP
```

可选参数包括 `--instrument-type`、`--region`、`--delay`、`--universe` 和 `--search`，适合只想单独拉取某个数据集字段并缓存到本地时使用。

函数定义：

```python
def get_datafields(
		  s,
		  instrument_type: str = 'EQUITY',
		  region: str = 'USA',
		  delay: int = 1,
		  universe: str = 'TOP3000',
		  dataset_id: str = '',
		  data_type: str = 'MATRIX',
		  search: str = ''
)
```

### 4.1 功能

分页请求 `/data-fields` 接口，直到拉取完整数据：

- 每页固定 `limit=50`
- 使用 `offset` 做翻页（0, 50, 100, ...）
- 返回结果列表中的 `results` 累积到本地
- 最终转成 `pandas.DataFrame` 返回

### 4.2 当前调用

```python
fundamental6 = get_datafields(s=sess, dataset_id='pv13', data_type='GROUP')
```

表示当前脚本会拉取数据集 `pv13`、类型 `GROUP` 的全部字段。

### 4.3 限速处理

每次翻页后 `sleep(5)`，用于降低请求频率，减少被限流风险。

## 5. Alpha 表达式批量生成逻辑

脚本从数据字段 DataFrame 里取 `id` 列，作为基础因子：

```python
datafields_list = fundamental6['id'].values
```

然后采用笛卡尔积组合参数：

- 横截面分组算子：`group_mean`, `group_neutralize`
- 时间序列算子：`ts_mean`, `ts_rank`
- 窗口：`63`, `126`
- 分组维度：`market`, `sector`, `industry`

拼接目标表达式形如：

```text
group_mean(ts_rank(<datafield>, 63), sector)
```

每个表达式会封装为一个仿真请求体 `simulation_data`，写入 `alpha_list`。

## 6. 仿真提交与轮询

对 `alpha_list` 中每一个请求体执行：

1. `POST /simulations` 提交仿真。
2. 从响应头 `Location` 获取进度查询地址。
3. 轮询该地址：
	- 若 `Retry-After > 0`，等待对应秒数后继续查询
	- 若 `Retry-After == 0`，表示仿真完成
4. 从最终 JSON 中读取 `alpha` 字段并打印（即 `alpha_id`）。

若响应中没有 `Location`，脚本会打印提示并等待 10 秒后继续下一个任务。

## 7. 当前脚本的实际行为与已知问题

以下问题会直接影响结果正确性与任务规模：

1. **表达式里窗口参数写错变量**
	- 当前写法：`{days}`
	- 预期应为：`{day}`
	- 影响：表达式会把整个列表 `[63, 126]` 传入，而不是单个窗口值。

2. **`group` 变量名被重复使用（列表名与循环变量同名）**
	- 写法：`group = ['market', 'sector', 'industry']` 与 `for group in group:`
	- 影响：循环后 `group` 变量会变成字符串，后续再次进入循环时可能按字符迭代，导致组合异常膨胀或语义错误。

3. **函数参数 `s` 未被使用**
	- `get_datafields(s=...)` 内部实际调用的是全局 `sess`。
	- 影响：函数可复用性降低，也不利于测试。

4. **异常处理过于宽泛**
	- 使用裸 `except:`，会吞掉真实错误原因。
	- 建议改为捕获具体异常并记录错误细节。

5. **认证文件路径处理可能不符合预期**
	- `expanduser('brain_credentials.txt')` 不会自动补 `~`，通常只是返回原字符串。
	- 如果期望用户主目录，建议使用 `expanduser('~/brain_credentials.txt')`。

6. **缺少失败重试与速率控制策略**
	- 仅依赖固定 `sleep`，未针对 `429/5xx` 做指数退避重试。

7. **可能创建超大任务队列**
	- 理论组合量为：
	  - 每个字段：$2 \times 2 \times 2 \times 3 = 24$ 个表达式
	  - 总任务数：$24 \times \text{字段数}$
	- 字段数大时会导致大量请求与较长执行时间。

## 8. 建议的改进方向

1. 修正变量名冲突与格式化错误：`day`/`days`、`group` 命名分离。
2. 在 `get_datafields` 中统一使用参数 `s` 发请求，去除对全局变量依赖。
3. 增加请求超时、状态码判断与重试策略（如指数退避）。
4. 记录日志（开始时间、字段数、成功数、失败数、失败原因）。
5. 分批提交仿真，控制并发和总任务量，避免触发 API 限制。
6. 将配置（region/universe/delay/decay 等）外置到配置文件或命令行参数。

## 9. 脚本执行流程（简版）

```text
读取凭证 -> 会话认证 -> 拉取字段(分页) -> 生成表达式列表 ->
逐个提交仿真 -> 轮询完成 -> 输出 alpha_id
```

## 10. 一句话总结

这个脚本是一个面向 WorldQuant Brain 的批量 Alpha 仿真流水线原型，已经具备“认证、抓字段、组表达式、提仿真、取结果”的主流程，但在变量使用、容错和可维护性方面还需要修正后再用于稳定生产。
