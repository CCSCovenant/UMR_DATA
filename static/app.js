const state = {
  config: null,
  videos: [],
  selectedVideo: null,
  clipDuration: 0,
  currentEditingTaskId: "",
  currentEditingResult: null,
  authToken: "",
  loading: {
    videos: false,
    vlm: false,
    translate: false,
    save: false,
    upload: false,
    settings: false,
    editing: false,
  },
};

const urlToken = new URLSearchParams(window.location.search).get("token")?.trim() || "";
const storedToken = window.localStorage.getItem("umrmSharedToken") || "";
state.authToken = urlToken || storedToken;
if (urlToken) {
  window.localStorage.setItem("umrmSharedToken", urlToken);
}

const elements = {
  videoRootInput: document.querySelector("#videoRootInput"),
  refreshVideosBtn: document.querySelector("#refreshVideosBtn"),
  uploadInput: document.querySelector("#uploadInput"),
  uploadBtn: document.querySelector("#uploadBtn"),
  rootMessage: document.querySelector("#rootMessage"),
  saveSettingsBtn: document.querySelector("#saveSettingsBtn"),
  vlmApiBaseInput: document.querySelector("#vlmApiBaseInput"),
  vlmModelInput: document.querySelector("#vlmModelInput"),
  vlmApiKeyInput: document.querySelector("#vlmApiKeyInput"),
  foleyCondaEnvInput: document.querySelector("#foleyCondaEnvInput"),
  foleyProjectDirInput: document.querySelector("#foleyProjectDirInput"),
  foleyModelPathInput: document.querySelector("#foleyModelPathInput"),
  foleyGpuIdInput: document.querySelector("#foleyGpuIdInput"),
  foleyModelSizeInput: document.querySelector("#foleyModelSizeInput"),
  foleyGuidanceInput: document.querySelector("#foleyGuidanceInput"),
  foleyStepsInput: document.querySelector("#foleyStepsInput"),
  foleyOffloadInput: document.querySelector("#foleyOffloadInput"),
  settingsMessage: document.querySelector("#settingsMessage"),
  vlmKeyHint: document.querySelector("#vlmKeyHint"),
  videoCount: document.querySelector("#videoCount"),
  videoList: document.querySelector("#videoList"),
  videoPlayer: document.querySelector("#videoPlayer"),
  selectedVideoMeta: document.querySelector("#selectedVideoMeta"),
  durationLabel: document.querySelector("#durationLabel"),
  clipStartInput: document.querySelector("#clipStartInput"),
  clipEndInput: document.querySelector("#clipEndInput"),
  clipValidationMessage: document.querySelector("#clipValidationMessage"),
  setStartBtn: document.querySelector("#setStartBtn"),
  setEndBtn: document.querySelector("#setEndBtn"),
  jumpStartBtn: document.querySelector("#jumpStartBtn"),
  jumpEndBtn: document.querySelector("#jumpEndBtn"),
  useVlmInput: document.querySelector("#useVlmInput"),
  runVlmBtn: document.querySelector("#runVlmBtn"),
  vlmPromptInput: document.querySelector("#vlmPromptInput"),
  currentStateInput: document.querySelector("#currentStateInput"),
  vlmSummaryOutput: document.querySelector("#vlmSummaryOutput"),
  useEditingInput: document.querySelector("#useEditingInput"),
  editingFields: document.querySelector("#editingFields"),
  editingPromptInput: document.querySelector("#editingPromptInput"),
  editingStartInput: document.querySelector("#editingStartInput"),
  editingEndInput: document.querySelector("#editingEndInput"),
  editingRunBtn: document.querySelector("#editingRunBtn"),
  editingMessage: document.querySelector("#editingMessage"),
  editingStageLabel: document.querySelector("#editingStageLabel"),
  editingProgressText: document.querySelector("#editingProgressText"),
  editingEtaLabel: document.querySelector("#editingEtaLabel"),
  editingElapsedLabel: document.querySelector("#editingElapsedLabel"),
  editingProgressBar: document.querySelector("#editingProgressBar"),
  editingLogOutput: document.querySelector("#editingLogOutput"),
  editingAudioPlayer: document.querySelector("#editingAudioPlayer"),
  editingAudioPath: document.querySelector("#editingAudioPath"),
  editingVideoPlayer: document.querySelector("#editingVideoPlayer"),
  editingVideoPath: document.querySelector("#editingVideoPath"),
  reactionInput: document.querySelector("#reactionInput"),
  motionPromptInput: document.querySelector("#motionPromptInput"),
  notesInput: document.querySelector("#notesInput"),
  saveBtn: document.querySelector("#saveBtn"),
  saveMessage: document.querySelector("#saveMessage"),
  translateSource: document.querySelector("#translateSource"),
  translateResult: document.querySelector("#translateResult"),
  translateBtn: document.querySelector("#translateBtn"),
  translateMessage: document.querySelector("#translateMessage"),
  vlmStatus: document.querySelector("#vlmStatus"),
  translateStatus: document.querySelector("#translateStatus"),
  foleyStatus: document.querySelector("#foleyStatus"),
  reloadAnnotationsBtn: document.querySelector("#reloadAnnotationsBtn"),
  recentList: document.querySelector("#recentList"),
  videoItemTemplate: document.querySelector("#videoItemTemplate"),
  recentItemTemplate: document.querySelector("#recentItemTemplate"),
};

function setMessage(node, text, level = "") {
  node.textContent = text || "";
  node.className = `message ${level}`.trim();
}

function withAuth(url) {
  const resolved = new URL(url, window.location.origin);
  if (state.authToken) {
    resolved.searchParams.set("token", state.authToken);
  }
  return resolved.toString();
}

function formatSeconds(value) {
  if (!Number.isFinite(value)) return "--";
  const minutes = Math.floor(value / 60);
  const seconds = value - minutes * 60;
  return `${minutes}:${seconds.toFixed(1).padStart(4, "0")}`;
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes)) return "--";
  const units = ["B", "KB", "MB", "GB"];
  let value = bytes;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  return `${value.toFixed(unitIndex === 0 ? 0 : 1)} ${units[unitIndex]}`;
}

function formatElapsed(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) return "--";
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
}

async function fetchJson(url, options = {}) {
  const headers = new Headers(options.headers || {});
  if (state.authToken) {
    headers.set("X-UMRM-Token", state.authToken);
  }
  const response = await fetch(withAuth(url), {
    ...options,
    headers,
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || payload.message || `Request failed: ${response.status}`);
  }
  return payload;
}

function fillSettings(settings) {
  if (!settings) return;
  const { vlm = {}, foley = {} } = settings;
  elements.vlmApiBaseInput.value = vlm.api_base || "";
  elements.vlmModelInput.value = vlm.model || "";
  elements.vlmApiKeyInput.value = "";
  elements.vlmKeyHint.textContent = vlm.api_key_set
    ? `VLM key: ${vlm.api_key_masked || "已设置"}`
    : "VLM key: 未设置";

  elements.foleyCondaEnvInput.value = foley.conda_env || "";
  elements.foleyProjectDirInput.value = foley.project_dir || "";
  elements.foleyModelPathInput.value = foley.model_path || "";
  elements.foleyGpuIdInput.value = foley.gpu_id ?? 0;
  elements.foleyModelSizeInput.value = foley.model_size || "xxl";
  elements.foleyGuidanceInput.value = foley.guidance_scale ?? 4.5;
  elements.foleyStepsInput.value = foley.num_inference_steps ?? 50;
  elements.foleyOffloadInput.checked = Boolean(foley.enable_offload);
}

async function loadConfig() {
  const config = await fetchJson("/api/config");
  state.config = config;
  elements.videoRootInput.value = config.default_video_root;
  elements.vlmStatus.textContent = config.vlm_configured ? "VLM: 已配置" : "VLM: 待补 model";
  elements.translateStatus.textContent = config.translation_configured
    ? "翻译: LLM 已配置"
    : "翻译: 公共备用通道";
  elements.foleyStatus.textContent = config.settings?.foley?.project_dir
    ? "Foley: 已接入 HunyuanVideo-Foley"
    : "Foley: 未配置";
  fillSettings(config.settings);
}

function renderVideoList() {
  elements.videoList.innerHTML = "";
  elements.videoCount.textContent = `${state.videos.length} 个`;

  if (!state.videos.length) {
    elements.videoList.innerHTML = '<p class="video-path">当前目录没有可用视频</p>';
    return;
  }

  state.videos.forEach((video) => {
    const node = elements.videoItemTemplate.content.firstElementChild.cloneNode(true);
    node.querySelector(".video-name").textContent = video.name;
    node.querySelector(".video-path").textContent = `${video.relative_path} · ${formatBytes(
      video.size_bytes
    )}`;
    if (state.selectedVideo?.absolute_path === video.absolute_path) {
      node.classList.add("active");
    }
    node.addEventListener("click", () => selectVideo(video));
    elements.videoList.appendChild(node);
  });
}

async function loadVideos(rootOverride = "") {
  state.loading.videos = true;
  elements.refreshVideosBtn.disabled = true;
  setMessage(elements.rootMessage, "正在读取视频目录...");
  try {
    const root = rootOverride || elements.videoRootInput.value.trim();
    const payload = await fetchJson(`/api/videos?root=${encodeURIComponent(root)}`);
    state.videos = payload.videos;
    elements.videoRootInput.value = payload.root;
    if (
      state.selectedVideo &&
      !state.videos.find((item) => item.absolute_path === state.selectedVideo.absolute_path)
    ) {
      state.selectedVideo = null;
      clearSelectedVideo();
    }
    renderVideoList();
    setMessage(elements.rootMessage, `已读取 ${payload.videos.length} 个视频`, "ok");
  } catch (error) {
    setMessage(elements.rootMessage, error.message, "error");
    state.videos = [];
    renderVideoList();
  } finally {
    state.loading.videos = false;
    elements.refreshVideosBtn.disabled = false;
  }
}

function clearSelectedVideo() {
  elements.videoPlayer.removeAttribute("src");
  elements.videoPlayer.load();
  elements.selectedVideoMeta.textContent = "请选择左侧视频";
  elements.durationLabel.textContent = "总时长: --";
  state.clipDuration = 0;
}

function selectVideo(video) {
  state.selectedVideo = video;
  renderVideoList();
  elements.videoPlayer.src = withAuth(video.preview_media_url || video.media_url);
  elements.videoPlayer.load();
  elements.selectedVideoMeta.textContent = `${video.relative_path} · ${formatBytes(video.size_bytes)}`;
  state.currentEditingTaskId = "";
  state.currentEditingResult = null;
  state.loading.editing = false;
  elements.editingRunBtn.disabled = false;
  resetEditingOutputs();
  setMessage(
    elements.rootMessage,
    video.preview_cached
      ? "已切到轻量预览缓存，远程查看会更顺滑"
      : "首次打开这个视频会先在服务器生成轻量预览缓存，之后会更顺滑",
    video.preview_cached ? "ok" : "warn"
  );
  setMessage(elements.saveMessage, "");
}

function getClipRange() {
  const start = Number(elements.clipStartInput.value);
  const end = Number(elements.clipEndInput.value);
  return { start, end, duration: end - start };
}

function getEditingRange() {
  const start = Number(elements.editingStartInput.value);
  const end = Number(elements.editingEndInput.value);
  return { start, end, duration: end - start };
}

function validateClipRange() {
  const { start, end, duration } = getClipRange();
  if (!state.selectedVideo) {
    setMessage(elements.clipValidationMessage, "请先选择一个视频", "warn");
    return false;
  }
  if (!Number.isFinite(start) || !Number.isFinite(end)) {
    setMessage(elements.clipValidationMessage, "开始和结束时间必须是数字", "error");
    return false;
  }
  if (start < 0 || end <= start) {
    setMessage(elements.clipValidationMessage, "结束时间必须大于开始时间", "error");
    return false;
  }
  if (state.clipDuration && end > state.clipDuration) {
    setMessage(elements.clipValidationMessage, "结束时间不能超过视频总时长", "error");
    return false;
  }
  if (duration < 3 || duration > 15) {
    setMessage(elements.clipValidationMessage, "标注片段时长必须在 3 到 15 秒之间", "warn");
    return false;
  }
  setMessage(
    elements.clipValidationMessage,
    `当前片段长度 ${duration.toFixed(1)} 秒，符合要求`,
    "ok"
  );
  return true;
}

function validateEditingRange() {
  if (!elements.useEditingInput.checked) {
    setMessage(elements.editingMessage, "", "");
    return true;
  }
  const clip = getClipRange();
  const editing = getEditingRange();
  if (!Number.isFinite(editing.start) || !Number.isFinite(editing.end) || editing.end <= editing.start) {
    setMessage(elements.editingMessage, "Editing 区间不合法", "error");
    return false;
  }
  if (editing.start < clip.start || editing.end > clip.end) {
    setMessage(elements.editingMessage, "Editing 区间必须落在当前标注片段内", "warn");
    return false;
  }
  setMessage(
    elements.editingMessage,
    `Editing 区间 ${editing.duration.toFixed(1)} 秒，将裁片后送入 Foley`,
    "ok"
  );
  return true;
}

function updateClipFromCurrentTime(target) {
  const currentTime = Number(elements.videoPlayer.currentTime || 0).toFixed(1);
  target.value = currentTime;
  validateClipRange();
  validateEditingRange();
}

function resetEditingOutputs() {
  elements.editingAudioPlayer.removeAttribute("src");
  elements.editingAudioPlayer.load();
  elements.editingAudioPath.textContent = "";
  elements.editingVideoPlayer.removeAttribute("src");
  elements.editingVideoPlayer.load();
  elements.editingVideoPath.textContent = "";
  elements.editingStageLabel.textContent = "阶段: 未开始";
  elements.editingProgressText.textContent = "进度: --";
  elements.editingEtaLabel.textContent = "ETA: --";
  elements.editingElapsedLabel.textContent = "耗时: --";
  elements.editingProgressBar.value = 0;
  elements.editingLogOutput.value = "";
  setMessage(elements.editingMessage, "");
}

function toggleEditingFields() {
  const enabled = elements.useEditingInput.checked;
  elements.editingFields.classList.toggle("hidden", !enabled);
  if (enabled) {
    if (!elements.editingStartInput.value || Number(elements.editingEndInput.value) <= Number(elements.editingStartInput.value)) {
      elements.editingStartInput.value = elements.clipStartInput.value || "0.0";
      elements.editingEndInput.value = elements.clipEndInput.value || "0.0";
    }
    validateEditingRange();
  } else {
    setMessage(elements.editingMessage, "");
  }
}

async function waitForEvent(target, eventName) {
  return new Promise((resolve, reject) => {
    const handleResolve = () => {
      cleanup();
      resolve();
    };
    const handleReject = () => {
      cleanup();
      reject(new Error(`Failed while waiting for ${eventName}`));
    };
    const cleanup = () => {
      target.removeEventListener(eventName, handleResolve);
      target.removeEventListener("error", handleReject);
    };
    target.addEventListener(eventName, handleResolve, { once: true });
    target.addEventListener("error", handleReject, { once: true });
  });
}

async function extractFramesFromClip(videoUrl, start, end, count = 6) {
  const sampler = document.createElement("video");
  sampler.src = withAuth(videoUrl);
  sampler.muted = true;
  sampler.preload = "auto";
  sampler.playsInline = true;
  sampler.crossOrigin = "anonymous";

  if (sampler.readyState < 1) {
    await waitForEvent(sampler, "loadedmetadata");
  }

  const canvas = document.createElement("canvas");
  canvas.width = sampler.videoWidth || 640;
  canvas.height = sampler.videoHeight || 360;
  const context = canvas.getContext("2d");
  const frames = [];
  const interval = count === 1 ? 0 : (end - start) / (count - 1);

  for (let index = 0; index < count; index += 1) {
    const timePoint = Math.min(end, start + interval * index);
    sampler.currentTime = timePoint;
    await waitForEvent(sampler, "seeked");
    context.drawImage(sampler, 0, 0, canvas.width, canvas.height);
    frames.push(canvas.toDataURL("image/jpeg", 0.8));
  }

  sampler.removeAttribute("src");
  sampler.load();
  return frames;
}

async function runVlmAssist() {
  if (!elements.useVlmInput.checked) {
    setMessage(elements.saveMessage, "如需调用 VLM，请先勾选“使用 VLM 理解当前片段”", "warn");
    return;
  }
  if (!validateClipRange()) return;
  if (!state.selectedVideo) {
    setMessage(elements.saveMessage, "请先选择视频", "error");
    return;
  }

  state.loading.vlm = true;
  elements.runVlmBtn.disabled = true;
  elements.vlmSummaryOutput.value = "";
  setMessage(elements.saveMessage, "正在采样片段帧并调用 VLM...", "");

  try {
    const { start, end } = getClipRange();
    const frames = await extractFramesFromClip(state.selectedVideo.media_url, start, end, 6);
    const payload = await fetchJson("/api/vlm-understand", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        video_name: state.selectedVideo.relative_path,
        clip_start: start,
        clip_end: end,
        current_state: elements.currentStateInput.value.trim(),
        vlm_prompt: elements.vlmPromptInput.value.trim(),
        frames,
      }),
    });
    elements.currentStateInput.value = payload.current_state || elements.currentStateInput.value;
    elements.vlmSummaryOutput.value = payload.clip_summary || payload.raw_response || "";
    setMessage(elements.saveMessage, "VLM 已返回建议，Current State 已更新", "ok");
  } catch (error) {
    setMessage(elements.saveMessage, error.message, "error");
  } finally {
    state.loading.vlm = false;
    elements.runVlmBtn.disabled = false;
  }
}

async function translateText(text) {
  return fetchJson("/api/translate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text }),
  });
}

async function runTranslate() {
  const source = elements.translateSource.value.trim();
  if (!source) {
    setMessage(elements.translateMessage, "请输入中文内容", "warn");
    return;
  }

  state.loading.translate = true;
  elements.translateBtn.disabled = true;
  setMessage(elements.translateMessage, "正在翻译...", "");
  try {
    const payload = await translateText(source);
    elements.translateResult.value = payload.translated_text || "";
    setMessage(elements.translateMessage, `翻译完成，来源: ${payload.provider}`, "ok");
  } catch (error) {
    setMessage(elements.translateMessage, error.message, "error");
  } finally {
    state.loading.translate = false;
    elements.translateBtn.disabled = false;
  }
}

async function translateIntoField(targetId) {
  const target = document.getElementById(targetId);
  if (!target) return;
  const source = target.value.trim();
  if (!source) {
    setMessage(elements.translateMessage, "目标字段目前为空，没有可翻译内容", "warn");
    return;
  }
  elements.translateSource.value = source;
  await runTranslate();
  if (elements.translateResult.value.trim()) {
    target.value = elements.translateResult.value.trim();
  }
}

async function saveSettings() {
  state.loading.settings = true;
  elements.saveSettingsBtn.disabled = true;
  setMessage(elements.settingsMessage, "正在保存服务配置...", "");
  try {
    const payload = await fetchJson("/api/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        vlm: {
          api_base: elements.vlmApiBaseInput.value.trim(),
          model: elements.vlmModelInput.value.trim(),
          api_key: elements.vlmApiKeyInput.value.trim(),
        },
        foley: {
          conda_env: elements.foleyCondaEnvInput.value.trim(),
          project_dir: elements.foleyProjectDirInput.value.trim(),
          model_path: elements.foleyModelPathInput.value.trim(),
          gpu_id: Number(elements.foleyGpuIdInput.value || 0),
          model_size: elements.foleyModelSizeInput.value,
          guidance_scale: Number(elements.foleyGuidanceInput.value),
          num_inference_steps: Number(elements.foleyStepsInput.value),
          enable_offload: elements.foleyOffloadInput.checked,
        },
      }),
    });
    fillSettings(payload.settings);
    elements.vlmStatus.textContent =
      payload.settings.vlm.api_base && payload.settings.vlm.model ? "VLM: 已配置" : "VLM: 待补 model";
    elements.foleyStatus.textContent = "Foley: 已接入 HunyuanVideo-Foley";
    setMessage(elements.settingsMessage, "配置已保存到服务端", "ok");
  } catch (error) {
    setMessage(elements.settingsMessage, error.message, "error");
  } finally {
    state.loading.settings = false;
    elements.saveSettingsBtn.disabled = false;
  }
}

function validateAnnotation() {
  if (!state.selectedVideo) {
    throw new Error("请先选择视频");
  }
  if (!validateClipRange()) {
    throw new Error("片段时长不符合要求");
  }
  if (!elements.reactionInput.value.trim()) {
    throw new Error("Reaction 为必填项");
  }
  if (!elements.motionPromptInput.value.trim()) {
    throw new Error("Motion Prompt 为必填项");
  }
  if (elements.useEditingInput.checked) {
    if (!elements.editingPromptInput.value.trim()) {
      throw new Error("启用 Editing 时，Editing Prompt 为必填");
    }
    if (!validateEditingRange()) {
      throw new Error("Editing 区间不合法");
    }
  }
}

function buildAnnotationPayload() {
  const clip = getClipRange();
  return {
    video_root: elements.videoRootInput.value.trim(),
    video_path: state.selectedVideo.absolute_path,
    video_relative_path: state.selectedVideo.relative_path,
    clip_start: clip.start,
    clip_end: clip.end,
    clip_duration: Number(clip.duration.toFixed(3)),
    use_vlm: elements.useVlmInput.checked,
    vlm_prompt: elements.vlmPromptInput.value.trim(),
    vlm_summary: elements.vlmSummaryOutput.value.trim(),
    current_state: elements.currentStateInput.value.trim(),
    use_editing: elements.useEditingInput.checked,
    editing_prompt: elements.editingPromptInput.value.trim(),
    editing_start: Number(elements.editingStartInput.value || 0),
    editing_end: Number(elements.editingEndInput.value || 0),
    editing_task_id: state.currentEditingTaskId || "",
    editing_audio_path: state.currentEditingResult?.audio_path || "",
    editing_video_path: state.currentEditingResult?.merged_video_path || "",
    reaction: elements.reactionInput.value.trim(),
    motion_prompt: elements.motionPromptInput.value.trim(),
    notes: elements.notesInput.value.trim(),
    ui_created_at: new Date().toISOString(),
  };
}

async function saveAnnotation() {
  state.loading.save = true;
  elements.saveBtn.disabled = true;
  setMessage(elements.saveMessage, "正在保存标注...", "");
  try {
    validateAnnotation();
    const payload = buildAnnotationPayload();
    const response = await fetchJson("/api/annotations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    setMessage(elements.saveMessage, `保存成功: ${response.saved_at}`, "ok");
    await loadRecentAnnotations();
  } catch (error) {
    setMessage(elements.saveMessage, error.message, "error");
  } finally {
    state.loading.save = false;
    elements.saveBtn.disabled = false;
  }
}

async function handleUpload() {
  const file = elements.uploadInput.files?.[0];
  if (!file) {
    setMessage(elements.rootMessage, "请先选择一个视频文件", "warn");
    return;
  }

  state.loading.upload = true;
  elements.uploadBtn.disabled = true;
  setMessage(elements.rootMessage, "正在上传视频...", "");
  try {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("root", elements.videoRootInput.value.trim());
    const payload = await fetchJson("/api/upload", {
      method: "POST",
      body: formData,
    });
    setMessage(elements.rootMessage, `上传完成: ${payload.saved_path}`, "ok");
    elements.uploadInput.value = "";
    await loadVideos(elements.videoRootInput.value.trim());
  } catch (error) {
    setMessage(elements.rootMessage, error.message, "error");
  } finally {
    state.loading.upload = false;
    elements.uploadBtn.disabled = false;
  }
}

function renderEditingResult(task) {
  state.currentEditingResult = task;
  if (task.audio_media_url) {
    elements.editingAudioPlayer.src = withAuth(task.audio_media_url);
    elements.editingAudioPlayer.load();
    elements.editingAudioPath.textContent = task.audio_path || "";
  }
  if (task.merged_video_preview_url || task.merged_video_media_url) {
    elements.editingVideoPlayer.src = withAuth(
      task.merged_video_preview_url || task.merged_video_media_url
    );
    elements.editingVideoPlayer.load();
    elements.editingVideoPath.textContent = task.merged_video_path || "";
  }
}

function renderEditingTask(task) {
  elements.editingStageLabel.textContent = `阶段: ${task.stage_label || task.stage || "--"}`;
  if (task.progress_total) {
    elements.editingProgressText.textContent = `进度: ${task.progress_current}/${task.progress_total} (${task.progress_percent || 0}%)`;
    elements.editingProgressBar.value = task.progress_percent || 0;
  } else {
    elements.editingProgressText.textContent = "进度: 当前阶段暂无 step 信息";
    elements.editingProgressBar.value = 0;
  }
  elements.editingEtaLabel.textContent = `ETA: ${task.eta_hint || "--"}`;
  elements.editingElapsedLabel.textContent = `耗时: ${formatElapsed(task.elapsed_seconds)}`;
  elements.editingLogOutput.value = task.log_tail || task.latest_log_line || "";

  if (task.stage === "loading_models" && (task.elapsed_seconds || 0) > 30) {
    setMessage(
      elements.editingMessage,
      "当前主要耗时在模型冷启动加载，不是在 denoising step。第一次跑或每次新进程启动都会明显更慢。",
      "warn"
    );
  }
}

async function pollEditingTask(taskId) {
  state.currentEditingTaskId = taskId;
  while (state.currentEditingTaskId === taskId) {
    const payload = await fetchJson(`/api/editing-task?id=${encodeURIComponent(taskId)}`);
    const task = payload.task;
    renderEditingTask(task);
    if (task.status === "completed") {
      renderEditingResult(task);
      setMessage(elements.editingMessage, "Foley 配音完成，可直接试听或查看带音视频", "ok");
      elements.editingRunBtn.disabled = false;
      state.loading.editing = false;
      return;
    }
    if (task.status === "failed") {
      setMessage(elements.editingMessage, task.error || "Foley 配音失败", "error");
      elements.editingRunBtn.disabled = false;
      state.loading.editing = false;
      return;
    }
    setMessage(elements.editingMessage, `Foley 任务状态: ${task.stage_label || task.status}`, "");
    await new Promise((resolve) => setTimeout(resolve, 3000));
  }
  state.loading.editing = false;
  elements.editingRunBtn.disabled = false;
}

async function startEditingFoley() {
  if (!elements.useEditingInput.checked) {
    setMessage(elements.editingMessage, "请先勾选“使用 Editing”", "warn");
    return;
  }
  if (!state.selectedVideo) {
    setMessage(elements.editingMessage, "请先选择视频", "error");
    return;
  }
  if (!validateClipRange() || !validateEditingRange()) {
    return;
  }
  if (!elements.editingPromptInput.value.trim()) {
    setMessage(elements.editingMessage, "Editing Prompt 不能为空", "warn");
    return;
  }

  state.loading.editing = true;
  elements.editingRunBtn.disabled = true;
  resetEditingOutputs();
  setMessage(elements.editingMessage, "正在提交 Foley 任务...", "");

  try {
    const clip = getClipRange();
    const editing = getEditingRange();
    const payload = await fetchJson("/api/editing", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        video_path: state.selectedVideo.absolute_path,
        clip_start: clip.start,
        clip_end: clip.end,
        editing_start: editing.start,
        editing_end: editing.end,
        editing_prompt: elements.editingPromptInput.value.trim(),
      }),
    });
    setMessage(elements.editingMessage, `任务已创建: ${payload.task.task_id}，开始处理...`, "");
    await pollEditingTask(payload.task.task_id);
  } catch (error) {
    setMessage(elements.editingMessage, error.message, "error");
    state.loading.editing = false;
    elements.editingRunBtn.disabled = false;
  }
}

async function loadRecentAnnotations() {
  try {
    const payload = await fetchJson("/api/annotations");
    renderRecentAnnotations(payload.annotations || []);
  } catch (error) {
    elements.recentList.innerHTML = `<p class="video-path">${error.message}</p>`;
  }
}

function renderRecentAnnotations(items) {
  elements.recentList.innerHTML = "";
  if (!items.length) {
    elements.recentList.innerHTML = '<p class="video-path">还没有保存过标注</p>';
    return;
  }

  items
    .slice()
    .reverse()
    .forEach((item) => {
      const node = elements.recentItemTemplate.content.firstElementChild.cloneNode(true);
      node.querySelector("h3").textContent = item.video_relative_path || item.video_path || "未命名视频";
      node.querySelector(
        ".recent-meta"
      ).textContent = `${formatSeconds(item.clip_start)} - ${formatSeconds(item.clip_end)} · ${
        item.saved_at || item.ui_created_at || ""
      }`;
      node.querySelector(".recent-text").textContent =
        item.current_state || item.reaction || item.motion_prompt || "(空)";
      elements.recentList.appendChild(node);
    });
}

function bindEvents() {
  elements.refreshVideosBtn.addEventListener("click", () => loadVideos());
  elements.uploadBtn.addEventListener("click", handleUpload);
  elements.saveSettingsBtn.addEventListener("click", saveSettings);
  elements.videoPlayer.addEventListener("loadedmetadata", () => {
    state.clipDuration = elements.videoPlayer.duration || 0;
    elements.durationLabel.textContent = `总时长: ${formatSeconds(state.clipDuration)}`;
    elements.clipStartInput.value = "0.0";
    elements.clipEndInput.value = Math.min(5, state.clipDuration || 5).toFixed(1);
    elements.editingStartInput.value = elements.clipStartInput.value;
    elements.editingEndInput.value = elements.clipEndInput.value;
    validateClipRange();
    validateEditingRange();
  });
  elements.clipStartInput.addEventListener("input", () => {
    validateClipRange();
    validateEditingRange();
  });
  elements.clipEndInput.addEventListener("input", () => {
    validateClipRange();
    validateEditingRange();
  });
  elements.editingStartInput.addEventListener("input", validateEditingRange);
  elements.editingEndInput.addEventListener("input", validateEditingRange);
  elements.setStartBtn.addEventListener("click", () => updateClipFromCurrentTime(elements.clipStartInput));
  elements.setEndBtn.addEventListener("click", () => updateClipFromCurrentTime(elements.clipEndInput));
  elements.jumpStartBtn.addEventListener("click", () => {
    elements.videoPlayer.currentTime = Number(elements.clipStartInput.value || 0);
  });
  elements.jumpEndBtn.addEventListener("click", () => {
    elements.videoPlayer.currentTime = Number(elements.clipEndInput.value || 0);
  });
  elements.useEditingInput.addEventListener("change", toggleEditingFields);
  elements.runVlmBtn.addEventListener("click", runVlmAssist);
  elements.translateBtn.addEventListener("click", runTranslate);
  elements.saveBtn.addEventListener("click", saveAnnotation);
  elements.editingRunBtn.addEventListener("click", startEditingFoley);
  elements.reloadAnnotationsBtn.addEventListener("click", loadRecentAnnotations);

  document.querySelectorAll(".fill-translate").forEach((button) => {
    button.addEventListener("click", () => {
      const targetId = button.dataset.target;
      const target = document.getElementById(targetId);
      if (!target || !elements.translateResult.value.trim()) {
        setMessage(elements.translateMessage, "当前没有可填入的翻译结果", "warn");
        return;
      }
      target.value = elements.translateResult.value.trim();
    });
  });

  document.querySelectorAll(".translate-target").forEach((button) => {
    button.addEventListener("click", async () => {
      const targetId = button.dataset.target;
      await translateIntoField(targetId);
    });
  });
}

async function init() {
  bindEvents();
  await loadConfig();
  await loadVideos();
  await loadRecentAnnotations();
  toggleEditingFields();
}

init().catch((error) => {
  setMessage(elements.rootMessage, error.message, "error");
});
