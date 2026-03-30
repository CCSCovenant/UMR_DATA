# UMRM 双服务器架构方案（讨论稿）

## 1. 你确认的目标边界

新目标按你的要求固定为：

- 客户端是一个独立的 Python Web Tool（部署在单独 CPU 服务器），服务多个标注用户；
- 客户端目录下有视频文件，供标注页面浏览与播放；
- 服务端是 GPU 计算服务器，服务端也有视频文件与推理能力；
- Editing 请求只上传 `video_id + 时间戳 + prompt`，不上传视频片段；
- 服务端根据 `video_id` 在服务端视频库里裁剪并做 Foley；
- 客户端只接收音频结果文件，不接收合成视频。

## 2. 推荐架构（按新边界）

整体是“**CPU 标注网关 + GPU 推理中心**”双服务：

1. **Annotator Gateway（CPU 服务器）**
   - 承载网页、用户会话、标注流程；
   - 负责视频列表、预览、标注保存；
   - 向 GPU 服务发起 Editing/Foley 任务；
   - 拉取并缓存返回音频，提供给页面试听。

2. **Inference Service（GPU 服务器）**
   - 维护 `video_id -> 服务端绝对路径` 映射；
   - 接收任务参数（`video_id/edit_start/edit_end/prompt`）；
   - 在 GPU 端本地视频上裁剪 + Foley 推理；
   - 仅输出音频产物与任务状态。

## 3. 请求与数据契约

## 3.1 Editing 请求（CPU -> GPU）

建议请求体：

```json
{
  "video_id": "xxx",
  "editing_start": 12.3,
  "editing_end": 18.6,
  "editing_prompt": "footsteps on wooden floor",
  "guidance_scale": 4.5,
  "num_inference_steps": 50
}
```

约束：

- `video_id` 必须能在 GPU 服务端映射到唯一视频路径；
- 时间戳由 GPU 服务端再次校验；
- 不允许 CPU 端传原始路径，避免跨机路径歧义与安全风险。

## 3.2 任务状态查询（CPU -> GPU）

- `GET /api/infer/editing-task?id=<task_id>`
- 返回阶段、进度、日志、错误信息、音频下载标识。

## 3.3 音频拉取（CPU <- GPU）

- `GET /api/infer/audio?id=<audio_id>` 或签名 URL；
- CPU 网关缓存后通过自身 `/media` 暴露给浏览器。

## 4. video_id 设计（关键）

统一规则：

- `video_id` 在 CPU 与 GPU 两侧都可稳定生成；
- 建议来源：相对路径 + 文件大小 + mtime 的哈希；
- GPU 服务以“自己的视频索引”为准，不信任客户端路径。

最小可行方案：

- CPU 启动时扫描视频目录，展示 `video_id` 给前端；
- GPU 启动时扫描视频目录，建立 `video_id -> abs_path`；
- 若 `video_id` 在 GPU 不存在，任务立即失败并返回明确错误。

## 5. 分阶段执行计划（仅更新计划，不执行）

## 阶段 0：契约先行

- 冻结 API 契约：任务创建、状态轮询、音频下载；
- 确认 `video_id` 生成算法与冲突策略；
- 明确错误码与鉴权方案（CPU->GPU token）。

产出：

- 一份稳定接口文档与字段定义，供前后端同步开发。

## 阶段 1：GPU 推理服务改造

- 从现有 `server.py + foley_worker.py` 提取“纯推理服务接口”；
- 改 `run_foley_task` 输入为 `video_id + 时间戳`；
- 在 GPU 侧完成：视频定位、裁剪、Foley、音频产出；
- 返回任务状态与音频文件标识。

## 阶段 2：CPU 标注网关改造

- 保留现有页面交互与标注流程；
- `startEditingFoley` 改为调用 GPU API（只传 `video_id + 时间戳 + prompt`）；
- 轮询 GPU 任务状态并渲染进度；
- 完成后拉取音频到本地缓存并在页面播放器展示。

## 阶段 3：多用户与运维能力

- CPU 网关侧增加用户标识、审计字段、并发限流；
- GPU 侧增加任务队列、并发控制、优先级策略；
- 增加失败重试、超时回收、日志检索；
- 标注数据集中归档（DB 或统一对象存储）。

## 6. 面向当前代码的改造清单

基于现有仓库，后续执行时建议按以下改：

1. `server.py` 拆分为两个角色：CPU 网关模式 / GPU 推理模式；
2. 现有 `/api/editing` 入参从 `video_path` 改为 `video_id`；
3. 新增 GPU 端视频索引模块（建立 `video_id -> path`）；
4. `clip_video_segment` 改为在 GPU 端按 `video_id` 查路径后执行；
5. 结果只保留 `audio_path/audio_url`，去掉“合并视频”链路；
6. 前端 `app.js` 的 Editing 结果渲染只展示音频播放器与任务日志；
7. 保留 `foley_worker.py` 作为 GPU 内部执行器，减少改造风险。

## 7. 风险与规避

- 风险：CPU 与 GPU 视频库不同步导致 `video_id` miss  
  规避：每日索引校验 + 提前预警 + 任务前置校验。

- 风险：同名文件映射错误  
  规避：`video_id` 不用文件名，使用哈希指纹。

- 风险：GPU 并发高峰排队过长  
  规避：队列可观测、优先级调度、超时与配额控制。

- 风险：跨服务调用被滥用  
  规避：CPU->GPU 双向鉴权、IP 白名单、签名 token。

## 8. 下一步（等待你审阅后再执行）

你审阅通过后，我将按以下顺序落地：

1. 先提交 API 契约与 `video_id` 规则实现；
2. 再改 GPU 端 `/api/editing` 新协议；
3. 再改 CPU 端前端与网关转发；
4. 最后补监控、鉴权和回归测试。

---

结论：  
在你设定的“CPU 客户端网关 + GPU 推理服务 + 仅传 video_id 与时间戳 + 只回传音频”模式下，带宽与系统边界最清晰，且便于多人稳定使用。
