# UMRM 标注网页

这是一个轻量本地网页程序，提供：

- 读取服务器目录中的视频
- 上传视频到当前目录
- 选择 `3-15s` 的标注片段
- 可选调用 VLM，对选中片段进行理解并更新 `current state`
- `Editing` 接入 HunyuanVideo-Foley，可对 editing 区间生成配音
- 必填 `reaction` 和 `motion prompt`
- 中文到英文翻译工具
- 保存标注到本地 `jsonl`

## 启动

```bash
cd /data/cws/Project/UMRM/UI
python server.py
```

默认打开：

```text
http://127.0.0.1:8765
```

现在视频预览默认优先走服务器端轻量缓存代理：

- 首次打开某个视频时，服务端会用 `ffmpeg` 生成较小的预览 mp4
- 生成完成后会缓存到 `/data/cws/Project/UMRM/UI/preview_cache`
- 后续同一视频再打开时，会直接复用缓存，跨机器查看会更顺滑

网页里新增了服务配置区，可直接设置：

- `VLM Base URL / Model / API Key`
- `HunyuanVideo-Foley` 的 conda 环境、项目路径、模型路径、GPU ID、推理参数

## 默认路径

- 视频目录默认读取：`/data/cws/Project/UMRM/data/UMRM/videos`
- 上传目录默认：`/data/cws/Project/UMRM/UI/uploads`
- 标注保存文件：`/data/cws/Project/UMRM/UI/data/annotations.jsonl`

## 可选环境变量

如果要给局域网内其他标注者使用，建议这样启动：

```bash
cd /data/cws/Project/UMRM/UI
export UMRM_UI_HOST=0.0.0.0
export UMRM_SHARED_TOKEN=replace-with-a-long-random-string
python server.py
```

其他机器访问时，直接带 token 打开：

```text
http://<你的服务器IP>:8765/?token=replace-with-a-long-random-string
```

预览缓存的码率/分辨率也可以调：

```bash
export UMRM_PREVIEW_HEIGHT=540
export UMRM_PREVIEW_VIDEO_BITRATE=900k
export UMRM_PREVIEW_AUDIO_BITRATE=96k
```

如果要启用 VLM，可设置 OpenAI-compatible 接口：

```bash
export VLM_API_BASE=http://127.0.0.1:8000/v1
export VLM_API_KEY=your_key
export VLM_MODEL=your_vlm_model
```

翻译优先使用下面这些配置；如果没配，会回退到公共翻译通道：

```bash
export TRANSLATE_API_BASE=http://127.0.0.1:8000/v1
export TRANSLATE_API_KEY=your_key
export TRANSLATE_MODEL=your_text_model
```

也支持复用：

```bash
export OPENAI_BASE_URL=http://127.0.0.1:8000/v1
export OPENAI_API_KEY=your_key
export OPENAI_MODEL=your_model
```

## 说明

- `Editing` 现在会把你选的 editing 区间裁成子视频，再在 `v2a` 环境里调用 `/data/cws/Project/HunyuanVideo-Foley/infer.py`。
- Foley 任务会在页面里显示当前阶段、denoising step 进度、ETA 提示和最近日志。
- 标注场景建议默认使用 `GPU 0 + XL`。`XXL` 主权重约 `9.6G`，首次冷启动明显更慢。
- VLM 不依赖本地 `ffmpeg/cv2`，改为前端从选中的时间段采样多帧后发给后端。
- 远程看片默认优先走轻量预览缓存；原始文件路径仍然用于标注记录、裁剪和 Foley。
- 如果开启 `UMRM_SHARED_TOKEN`，`/api/*`、`/media`、`/preview` 都需要带 token 才能访问，适合局域网共享。
- 保存结果采用 `jsonl` 追加写入，方便后续再处理。
