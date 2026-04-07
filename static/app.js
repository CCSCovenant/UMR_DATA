const state = {
  config: null,
  videos: [],
  selectedVideo: null,
  videoStatusByPath: {},
  clipDuration: 0,
  currentEditingTaskId: "",
  currentEditingResult: null,
  authToken: "",
  annotatorId: "",
  userName: "",
  hlsPlayer: null,
  videoDurationByPath: {},
  claimHeartbeatTimer: null,
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
const CLIP_MIN_DURATION = 2;
const CLIP_MAX_DURATION = 15;
const CLIP_DURATION_EPS = 1e-6;
const ANNOTATION_PATH_STORAGE_PREFIX = "umrmAnnotationPath";
const ANNOTATOR_ID_STORAGE_PREFIX = "umrmAnnotatorId";
const USER_NAME_STORAGE_KEY = "umrmUserName";
const VIDEO_STATUS_POLL_INTERVAL_MS = 15000;
const CLAIM_HEARTBEAT_INTERVAL_MS = 5 * 60 * 1000;

const elements = {
  userNameInput: document.querySelector("#userNameInput"),
  applyUserBtn: document.querySelector("#applyUserBtn"),
  currentUserPill: document.querySelector("#currentUserPill"),
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
  vlmTranslateToZhInput: document.querySelector("#vlmTranslateToZhInput"),
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
  mentalReasoningInput: document.querySelector("#mentalReasoningInput"),
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
  annotationPathInput: document.querySelector("#annotationPathInput"),
  rememberPathBtn: document.querySelector("#rememberPathBtn"),
  verifyBtn: document.querySelector("#verifyBtn"),
  saveBtn: document.querySelector("#saveBtn"),
  saveMessage: document.querySelector("#saveMessage"),
  translateSource: document.querySelector("#translateSource"),
  translateResult: document.querySelector("#translateResult"),
  translateBtn: document.querySelector("#translateBtn"),
  translateToZhBtn: document.querySelector("#translateToZhBtn"),
  translateMessage: document.querySelector("#translateMessage"),
  vlmStatus: document.querySelector("#vlmStatus"),
  translateStatus: document.querySelector("#translateStatus"),
  foleyStatus: document.querySelector("#foleyStatus"),
  reloadAnnotationsBtn: document.querySelector("#reloadAnnotationsBtn"),
  recentList: document.querySelector("#recentList"),
  annotatedVideoCount: document.querySelector("#annotatedVideoCount"),
  annotatedVideosList: document.querySelector("#annotatedVideosList"),
  videoItemTemplate: document.querySelector("#videoItemTemplate"),
  recentItemTemplate: document.querySelector("#recentItemTemplate"),
  annotatedVideoItemTemplate: document.querySelector("#annotatedVideoItemTemplate"),
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

function getAnnotatorStorageKey() {
  return `${ANNOTATOR_ID_STORAGE_PREFIX}:${state.authToken || "anonymous"}`;
}

function sanitizeUserName(value) {
  return String(value || "")
    .trim()
    .replace(/[^A-Za-z0-9._:-]+/g, "_")
    .slice(0, 80);
}

function setCurrentUser(nameValue) {
  const normalized = sanitizeUserName(nameValue);
  if (!normalized) {
    throw new Error("请先输入有效用户名（仅支持字母数字._:-）");
  }
  state.userName = normalized;
  state.annotatorId = normalized;
  elements.userNameInput.value = normalized;
  elements.currentUserPill.textContent = `用户: ${normalized}`;
  window.localStorage.setItem(USER_NAME_STORAGE_KEY, normalized);
  window.sessionStorage.setItem(getAnnotatorStorageKey(), normalized);
}

function ensureSignedIn() {
  if (state.annotatorId) return;
  throw new Error("请先输入用户名并点击进入");
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

function normalizePathKey(value) {
  const raw = String(value || "").trim();
  try {
    return decodeURIComponent(raw).replace(/\\/g, "/");
  } catch (_) {
    return raw.replace(/\\/g, "/");
  }
}

function formatElapsed(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) return "--";
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
}

async function fetchJson(url, options = {}) {
  ensureSignedIn();
  const headers = new Headers(options.headers || {});
  if (state.authToken) {
    headers.set("X-UMRM-Token", state.authToken);
  }
  if (state.annotatorId) {
    headers.set("X-UMRM-Annotator-ID", state.annotatorId);
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

function statusText(statusValue) {
  const normalized = normalizeStatusValue(statusValue);
  if (normalized === "claimed") return "claimed";
  if (normalized === "completed_unverified") return "completed(unverified)";
  if (normalized === "verified") return "verified";
  return "unclaimed";
}

function normalizeStatusValue(statusValue) {
  const raw = String(statusValue || "").trim().toLowerCase();
  if (!raw) return "unclaimed";
  if (raw === "completed(unverified)" || raw === "completed-unverified" || raw === "completed unverified") {
    return "completed_unverified";
  }
  if (raw === "claimed" || raw === "completed_unverified" || raw === "verified" || raw === "unclaimed") {
    return raw;
  }
  return "unclaimed";
}

function isClaimedByOther(statusEntry) {
  if (!statusEntry) return false;
  const owner = String(statusEntry.claimed_by || "").trim();
  if (!owner) return false;
  const hasLease =
    Boolean(String(statusEntry.claim_expires_at || "").trim()) ||
    normalizeStatusValue(statusEntry.status) === "claimed";
  return hasLease && owner !== state.annotatorId;
}

function isClaimedByCurrentUser(statusEntry) {
  if (!statusEntry) return false;
  const owner = String(statusEntry.claimed_by || "").trim();
  if (!owner || owner !== state.annotatorId) return false;
  return (
    Boolean(String(statusEntry.claim_expires_at || "").trim()) ||
    normalizeStatusValue(statusEntry.status) === "claimed"
  );
}

function getVideoStatus(videoPath) {
  return normalizeStatusValue(state.videoStatusByPath[videoPath]?.status || "unclaimed");
}

function updateVideoStatusEntry(videoPath, statusEntry) {
  if (!videoPath || !statusEntry) return;
  const normalizedEntry = { ...statusEntry, status: normalizeStatusValue(statusEntry.status) };
  state.videoStatusByPath[videoPath] = normalizedEntry;
  const found = state.videos.find((item) => item.absolute_path === videoPath);
  if (found) {
    found.video_status = normalizedEntry;
  }
  if (state.selectedVideo?.absolute_path === videoPath) {
    state.selectedVideo.video_status = normalizedEntry;
  }
}

function resetClaimHeartbeat() {
  if (state.claimHeartbeatTimer) {
    window.clearInterval(state.claimHeartbeatTimer);
    state.claimHeartbeatTimer = null;
  }
  if (!state.selectedVideo) return;
  const statusEntry = state.videoStatusByPath[state.selectedVideo.absolute_path] || {};
  if (!isClaimedByCurrentUser(statusEntry)) return;
  state.claimHeartbeatTimer = window.setInterval(async () => {
    try {
      const payload = await fetchJson("/api/video-heartbeat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ video_path: state.selectedVideo.absolute_path }),
      });
      updateVideoStatusEntry(state.selectedVideo.absolute_path, payload.status_entry);
      renderVideoList();
    } catch (_) {
    }
  }, CLAIM_HEARTBEAT_INTERVAL_MS);
}

function getAnnotationPathStorageKey() {
  return `${ANNOTATION_PATH_STORAGE_PREFIX}:${state.userName || "anonymous"}`;
}

function saveAnnotationPathPreference(pathValue) {
  window.localStorage.setItem(getAnnotationPathStorageKey(), pathValue);
}

function getCurrentAnnotationPath() {
  return elements.annotationPathInput.value.trim();
}

function getAnnotationPathOrThrow() {
  const annotationPath = getCurrentAnnotationPath();
  if (!annotationPath) {
    throw new Error("请先填写标注保存路径（.json 或 .jsonl）");
  }
  return annotationPath;
}

function fillSettings(settings) {
  if (!settings) return;
  const { vlm = {}, foley = {} } = settings;
  elements.vlmApiBaseInput.value = vlm.api_base || "";
  elements.vlmModelInput.value = vlm.model || "";
  elements.vlmApiKeyInput.value = "";
  elements.vlmTranslateToZhInput.checked = vlm.translate_to_zh !== false;
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
  const storedAnnotationPath = window.localStorage.getItem(getAnnotationPathStorageKey()) || "";
  elements.annotationPathInput.value = storedAnnotationPath || config.annotation_file || "";
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
    const currentStatusEntry = video.video_status || state.videoStatusByPath[video.absolute_path] || {};
    const currentStatus = currentStatusEntry.status || getVideoStatus(video.absolute_path);
    const lockedByOther = isClaimedByOther(currentStatusEntry);
    const ownerText = lockedByOther ? ` (${currentStatusEntry.claimed_by})` : "";
    node.querySelector(".video-path").textContent = `${video.relative_path} · ${formatBytes(
      video.size_bytes
    )} · 状态: ${statusText(currentStatus)}${ownerText}`;
    node.disabled = lockedByOther;
    if (lockedByOther) {
      node.title = `已被 ${currentStatusEntry.claimed_by} 领取`;
    }
    if (state.selectedVideo?.absolute_path === video.absolute_path) {
      node.classList.add("active");
    }
    node.addEventListener("click", async () => {
      if (lockedByOther) {
        setMessage(
          elements.rootMessage,
          `该视频已被 ${currentStatusEntry.claimed_by} 领取，暂时不可选`,
          "warn"
        );
        return;
      }
      try {
        await selectVideo(video);
      } catch (error) {
        setMessage(elements.rootMessage, error.message, "error");
        await refreshVideoStatuses().catch(() => {});
      }
    });
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
    state.videoStatusByPath = {};
    state.videos.forEach((video) => {
      if (video.video_status) {
        state.videoStatusByPath[video.absolute_path] = video.video_status;
      }
    });
    elements.videoRootInput.value = payload.root;
    if (
      state.selectedVideo &&
      !state.videos.find((item) => item.absolute_path === state.selectedVideo.absolute_path)
    ) {
      state.selectedVideo = null;
      clearSelectedVideo();
      resetClaimHeartbeat();
    } else if (state.selectedVideo) {
      const refreshed = state.videos.find(
        (item) => item.absolute_path === state.selectedVideo.absolute_path
      );
      if (refreshed) {
        state.selectedVideo = refreshed;
      }
    }
    renderVideoList();
    await loadAnnotatedVideos();
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
  if (state.hlsPlayer) {
    state.hlsPlayer.destroy();
    state.hlsPlayer = null;
  }
  elements.videoPlayer.removeAttribute("src");
  elements.videoPlayer.load();
  elements.selectedVideoMeta.textContent = "请选择左侧视频";
  elements.durationLabel.textContent = "总时长: --";
  state.clipDuration = 0;
  state.selectedVideo = null;
}

function applyVideoDuration(durationSeconds, resetRanges = false, allowShrink = false) {
  if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) {
    return false;
  }
  if (
    !allowShrink &&
    Number.isFinite(state.clipDuration) &&
    state.clipDuration > 0 &&
    durationSeconds + 0.2 < state.clipDuration
  ) {
    return false;
  }
  const nextDuration =
    !allowShrink && Number.isFinite(state.clipDuration) && state.clipDuration > 0
      ? Math.max(state.clipDuration, durationSeconds)
      : durationSeconds;
  state.clipDuration = nextDuration;
  elements.durationLabel.textContent = `总时长: ${formatSeconds(nextDuration)}`;
  if (resetRanges) {
    elements.clipStartInput.value = "0.0";
    elements.clipEndInput.value = Math.min(5, nextDuration || 5).toFixed(1);
    elements.editingStartInput.value = elements.clipStartInput.value;
    elements.editingEndInput.value = elements.clipEndInput.value;
  } else {
    const clipEnd = Number(elements.clipEndInput.value);
    if (Number.isFinite(clipEnd) && clipEnd > nextDuration) {
      elements.clipEndInput.value = nextDuration.toFixed(1);
      elements.editingEndInput.value = elements.clipEndInput.value;
    }
  }
  validateClipRange();
  validateEditingRange();
  return true;
}

async function ensureSelectedVideoDuration(video) {
  if (!video?.absolute_path) return;
  const cached = state.videoDurationByPath[video.absolute_path];
  if (applyVideoDuration(cached, true)) {
    return;
  }
  const payload = await fetchJson(`/api/video-metadata?path=${encodeURIComponent(video.absolute_path)}`);
  const duration = Number(payload.duration_seconds);
  if (!Number.isFinite(duration) || duration <= 0) return;
  state.videoDurationByPath[video.absolute_path] = duration;
  if (!state.selectedVideo || state.selectedVideo.absolute_path !== video.absolute_path) return;
  applyVideoDuration(duration, true);
}

function playVideoSource(video) {
  if (state.hlsPlayer) {
    state.hlsPlayer.destroy();
    state.hlsPlayer = null;
  }
  const fallbackUrl = withAuth(video.stream_media_url || video.preview_media_url || video.media_url);
  const fallbackMode = video.stream_media_url ? "stream-low" : video.preview_cached ? "preview" : "media";
  const hlsUrl = video.hls_media_url ? withAuth(video.hls_media_url) : "";
  if (hlsUrl) {
    if (window.Hls && window.Hls.isSupported()) {
      const hls = new window.Hls({
        enableWorker: true,
        lowLatencyMode: true,
        xhrSetup: (xhr) => {
          if (state.authToken) {
            xhr.setRequestHeader("X-UMRM-Token", state.authToken);
          }
        },
      });
      state.hlsPlayer = hls;
      hls.on(window.Hls.Events.ERROR, (_event, data) => {
        if (!data?.fatal) return;
        if (!state.selectedVideo || state.selectedVideo.absolute_path !== video.absolute_path) return;
        hls.destroy();
        if (state.hlsPlayer === hls) {
          state.hlsPlayer = null;
        }
        elements.videoPlayer.src = fallbackUrl;
        elements.videoPlayer.load();
        setMessage(elements.rootMessage, "HLS 播放失败，已自动切换到低码率流", "warn");
      });
      hls.loadSource(hlsUrl);
      hls.attachMedia(elements.videoPlayer);
      return "hls-js";
    }
    if (!state.authToken && elements.videoPlayer.canPlayType("application/vnd.apple.mpegurl")) {
      elements.videoPlayer.src = hlsUrl;
      elements.videoPlayer.load();
      return "hls-native";
    }
  }
  elements.videoPlayer.src = fallbackUrl;
  elements.videoPlayer.load();
  return fallbackMode;
}

async function releaseCurrentVideoClaim() {
  if (!state.selectedVideo?.absolute_path) return;
  const statusEntry = state.videoStatusByPath[state.selectedVideo.absolute_path] || {};
  if (!isClaimedByCurrentUser(statusEntry)) return;
  try {
    const payload = await fetchJson("/api/video-release", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ video_path: state.selectedVideo.absolute_path }),
    });
    updateVideoStatusEntry(state.selectedVideo.absolute_path, payload.status_entry);
    renderVideoList();
  } catch (_) {
  }
}

function releaseCurrentVideoClaimSync() {
  if (!state.selectedVideo?.absolute_path) return;
  const statusEntry = state.videoStatusByPath[state.selectedVideo.absolute_path] || {};
  if (!isClaimedByCurrentUser(statusEntry)) return;
  const payload = JSON.stringify({
    video_path: state.selectedVideo.absolute_path,
    annotator_id: state.annotatorId,
  });
  const body = new Blob([payload], { type: "application/json" });
  try {
    if (navigator.sendBeacon) {
      navigator.sendBeacon(withAuth("/api/video-release"), body);
    }
  } catch (_) {
  }
}

async function refreshVideoStatuses() {
  const root = elements.videoRootInput.value.trim();
  const payload = await fetchJson(`/api/video-statuses?root=${encodeURIComponent(root)}`);
  Object.entries(payload.statuses || {}).forEach(([videoPath, statusEntry]) => {
    updateVideoStatusEntry(videoPath, statusEntry);
  });
  if (state.selectedVideo?.absolute_path) {
    const selectedStatus = state.videoStatusByPath[state.selectedVideo.absolute_path];
    if (isClaimedByOther(selectedStatus)) {
      clearSelectedVideo();
      resetClaimHeartbeat();
      setMessage(elements.rootMessage, `当前片段已被 ${selectedStatus.claimed_by} 接管，已自动取消选中`, "warn");
    }
  }
  resetClaimHeartbeat();
  renderVideoList();
}

async function claimVideoOrThrow(video) {
  const payload = await fetchJson("/api/video-claim", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      video_path: video.absolute_path,
      video_relative_path: video.relative_path,
    }),
  });
  updateVideoStatusEntry(video.absolute_path, payload.status_entry);
  renderVideoList();
}

async function selectVideo(video) {
  if (
    state.selectedVideo?.absolute_path &&
    state.selectedVideo.absolute_path !== video.absolute_path
  ) {
    await releaseCurrentVideoClaim();
  }
  await claimVideoOrThrow(video);
  state.selectedVideo = video;
  renderVideoList();
  elements.durationLabel.textContent = "总时长: 读取中...";
  const playbackMode = playVideoSource(video);
  ensureSelectedVideoDuration(video).catch(() => {});
  elements.selectedVideoMeta.textContent = `${video.relative_path} · ${formatBytes(video.size_bytes)}`;
  state.currentEditingTaskId = "";
  state.currentEditingResult = null;
  state.loading.editing = false;
  elements.editingRunBtn.disabled = false;
  resetEditingOutputs();
  const playbackMsg = playbackMode.startsWith("hls")
    ? "已启用 HLS 分段播放，边下边播更稳定"
    : playbackMode === "stream-low"
      ? "已启用低码率流式播放，首帧加载更快"
      : video.preview_cached
        ? "已切到轻量预览缓存，远程查看会更顺滑"
        : "首次打开这个视频会先在服务器生成轻量预览缓存，之后会更顺滑";
  const playbackLevel =
    playbackMode.startsWith("hls") || playbackMode === "stream-low" || video.preview_cached ? "ok" : "warn";
  setMessage(elements.rootMessage, playbackMsg, playbackLevel);
  setMessage(elements.saveMessage, "");
  await loadCurrentVideoAnnotations();
  await loadAnnotatedVideos();
  resetClaimHeartbeat();
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
  if (duration < CLIP_MIN_DURATION - CLIP_DURATION_EPS) {
    setMessage(elements.clipValidationMessage, "标注片段长度不能小于 2 秒", "warn");
    return false;
  }
  if (duration > CLIP_MAX_DURATION + CLIP_DURATION_EPS) {
    setMessage(elements.clipValidationMessage, "标注片段长度不能大于 15 秒", "warn");
    return false;
  }
  setMessage(
    elements.clipValidationMessage,
    `当前片段长度 ${duration.toFixed(1)} 秒，合法范围 ${CLIP_MIN_DURATION}-${CLIP_MAX_DURATION} 秒`,
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

async function translateText(text, direction = "zh-en") {
  const endpoint = direction === "en-zh" ? "/api/translate-en-zh" : "/api/translate";
  return fetchJson(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text }),
  });
}

async function runTranslate(direction = "zh-en") {
  const source = elements.translateSource.value.trim();
  if (!source) {
    setMessage(
      elements.translateMessage,
      direction === "en-zh" ? "请输入英文内容" : "请输入中文内容",
      "warn"
    );
    return;
  }

  state.loading.translate = true;
  elements.translateBtn.disabled = true;
  elements.translateToZhBtn.disabled = true;
  setMessage(elements.translateMessage, "正在翻译...", "");
  try {
    const payload = await translateText(source, direction);
    elements.translateResult.value = payload.translated_text || "";
    setMessage(elements.translateMessage, `翻译完成，来源: ${payload.provider}`, "ok");
  } catch (error) {
    setMessage(elements.translateMessage, error.message, "error");
  } finally {
    state.loading.translate = false;
    elements.translateBtn.disabled = false;
    elements.translateToZhBtn.disabled = false;
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
  await runTranslate("zh-en");
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
          translate_to_zh: elements.vlmTranslateToZhInput.checked,
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
  const statusEntry = state.videoStatusByPath[state.selectedVideo.absolute_path] || {};
  if (!isClaimedByCurrentUser(statusEntry)) {
    throw new Error("请先领取该视频后再保存");
  }
  getAnnotationPathOrThrow();
  if (elements.useEditingInput.checked) {
    if (!elements.editingPromptInput.value.trim()) {
      throw new Error("启用 Editing 时，Editing Prompt 为必填");
    }
    if (!validateEditingRange()) {
      throw new Error("Editing 区间不合法");
    }
  }
}

async function ensureSaveClaimReady() {
  if (!state.selectedVideo) return;
  const statusEntry = state.videoStatusByPath[state.selectedVideo.absolute_path] || {};
  if (isClaimedByCurrentUser(statusEntry)) return;
  await claimVideoOrThrow(state.selectedVideo);
}

function buildAnnotationPayload() {
  const clip = getClipRange();
  return {
    annotation_path: getCurrentAnnotationPath(),
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
    mental_reasoning: elements.mentalReasoningInput.value.trim(),
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
    await ensureSaveClaimReady();
    const payload = buildAnnotationPayload();
    const response = await fetchJson("/api/annotations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (response.annotation_file) {
      elements.annotationPathInput.value = response.annotation_file;
      saveAnnotationPathPreference(response.annotation_file);
    } else {
      saveAnnotationPathPreference(getCurrentAnnotationPath());
    }
    setMessage(
      elements.saveMessage,
      `保存成功: ${response.saved_at} · ${response.annotation_file || getCurrentAnnotationPath()}`,
      "ok"
    );
    await refreshVideoStatuses();
    resetClaimHeartbeat();
    await loadCurrentVideoAnnotations(response.annotation_file || getCurrentAnnotationPath());
    await loadAnnotatedVideos(response.annotation_file || getCurrentAnnotationPath());
  } catch (error) {
    setMessage(elements.saveMessage, error.message, "error");
  } finally {
    state.loading.save = false;
    elements.saveBtn.disabled = false;
  }
}

async function markVideoVerified() {
  if (!state.selectedVideo) {
    setMessage(elements.saveMessage, "请先选择视频", "warn");
    return;
  }
  const currentStatus = getVideoStatus(state.selectedVideo.absolute_path);
  if (currentStatus !== "completed_unverified" && currentStatus !== "verified") {
    setMessage(elements.saveMessage, "只有 completed(unverified) 才能标记 verified", "warn");
    return;
  }
  try {
    const payload = await fetchJson("/api/video-status", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        video_path: state.selectedVideo.absolute_path,
        status: "verified",
      }),
    });
    updateVideoStatusEntry(state.selectedVideo.absolute_path, payload.status_entry);
    renderVideoList();
    setMessage(elements.saveMessage, "已标记为 verified", "ok");
  } catch (error) {
    setMessage(elements.saveMessage, error.message, "error");
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
  if (!state.selectedVideo.video_id) {
    setMessage(elements.editingMessage, "当前视频缺少 video_id，无法提交 Editing", "error");
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
        video_id: state.selectedVideo.video_id,
        video_path: state.selectedVideo.absolute_path,
        video_name: state.selectedVideo.relative_path,
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

async function loadCurrentVideoAnnotations(pathOverride = "") {
  try {
    const targetPath = pathOverride || getCurrentAnnotationPath();
    const params = new URLSearchParams();
    if (targetPath) {
      params.set("path", targetPath);
    }
    if (state.selectedVideo?.absolute_path) {
      params.set("video_path", state.selectedVideo.absolute_path);
    }
    const query = params.toString() ? `?${params.toString()}` : "";
    const payload = await fetchJson(`/api/annotations${query}`);
    if (payload.annotation_file) {
      elements.annotationPathInput.value = payload.annotation_file;
      saveAnnotationPathPreference(payload.annotation_file);
    }
    const filtered = (payload.annotations || []).filter((item) => isAnnotationForSelectedVideo(item));
    renderCurrentVideoAnnotations(filtered);
  } catch (error) {
    elements.recentList.innerHTML = `<p class="video-path">${error.message}</p>`;
  }
}

function isAnnotationForSelectedVideo(item) {
  if (!state.selectedVideo?.absolute_path) return false;
  const selectedAbs = normalizePathKey(state.selectedVideo.absolute_path);
  const selectedRel = normalizePathKey(state.selectedVideo.relative_path);
  const itemAbs = normalizePathKey(item.video_path || "");
  const itemRel = normalizePathKey(item.video_relative_path || "");
  return (itemAbs && itemAbs === selectedAbs) || (itemRel && selectedRel && itemRel === selectedRel);
}

function renderAnnotatedVideos(items) {
  elements.annotatedVideosList.innerHTML = "";
  elements.annotatedVideoCount.textContent = `${items.length} 个`;
  if (!items.length) {
    elements.annotatedVideosList.innerHTML = '<p class="video-path">还没有已标注视频</p>';
    return;
  }
  items.forEach((item) => {
    const node = elements.annotatedVideoItemTemplate.content.firstElementChild.cloneNode(true);
    const name = item.video_relative_path || item.video_path || "未命名视频";
    node.querySelector(".video-name").textContent = name;
    node.querySelector(".video-path").textContent = `片段数: ${item.segment_count} · 最近保存: ${
      item.latest_saved_at || "--"
    }`;
    const found = state.videos.find(
      (video) => normalizePathKey(video.absolute_path) === normalizePathKey(item.video_path)
    );
    if (state.selectedVideo?.absolute_path && isAnnotationForSelectedVideo(item)) {
      node.classList.add("active");
    }
    if (!found) {
      node.disabled = true;
      node.title = "当前目录下未找到该视频";
    } else {
      node.addEventListener("click", async () => {
        try {
          await selectVideo(found);
        } catch (error) {
          setMessage(elements.rootMessage, error.message, "error");
        }
      });
    }
    elements.annotatedVideosList.appendChild(node);
  });
}

async function loadAnnotatedVideos(pathOverride = "") {
  try {
    const targetPath = pathOverride || getCurrentAnnotationPath();
    const query = targetPath ? `?path=${encodeURIComponent(targetPath)}` : "";
    const payload = await fetchJson(`/api/annotations${query}`);
    if (payload.annotation_file) {
      elements.annotationPathInput.value = payload.annotation_file;
      saveAnnotationPathPreference(payload.annotation_file);
    }
    const grouped = new Map();
    (payload.annotations || []).forEach((item) => {
      const key = normalizePathKey(item.video_path || "");
      if (!key) return;
      const savedAt = item.saved_at || item.ui_created_at || "";
      const existing = grouped.get(key);
      if (!existing) {
        grouped.set(key, {
          video_path: item.video_path || "",
          video_relative_path: item.video_relative_path || "",
          segment_count: 1,
          latest_saved_at: savedAt,
        });
        return;
      }
      existing.segment_count += 1;
      if (savedAt > existing.latest_saved_at) {
        existing.latest_saved_at = savedAt;
      }
    });
    const items = Array.from(grouped.values()).sort((a, b) =>
      String(b.latest_saved_at || "").localeCompare(String(a.latest_saved_at || ""))
    );
    renderAnnotatedVideos(items);
  } catch (error) {
    elements.annotatedVideosList.innerHTML = `<p class="video-path">${error.message}</p>`;
    elements.annotatedVideoCount.textContent = "0 个";
  }
}

async function startWorkspaceForCurrentUser() {
  await loadConfig();
  await loadVideos();
  await loadCurrentVideoAnnotations();
  await loadAnnotatedVideos();
  toggleEditingFields();
}

function annotationPreviewText(item) {
  const lines = [];
  if (item.current_state) lines.push(`Current State: ${item.current_state}`);
  if (item.reaction) lines.push(`Reaction: ${item.reaction}`);
  if (item.motion_prompt) lines.push(`Motion Prompt: ${item.motion_prompt}`);
  if (item.mental_reasoning) lines.push(`Mental Reasoning: ${item.mental_reasoning}`);
  if (item.notes) lines.push(`Notes: ${item.notes}`);
  return lines.join("\n");
}

function renderCurrentVideoAnnotations(items) {
  elements.recentList.innerHTML = "";
  if (!state.selectedVideo?.absolute_path) {
    elements.recentList.innerHTML = '<p class="video-path">请选择视频后查看该视频的已保存片段</p>';
    return;
  }
  if (!items.length) {
    elements.recentList.innerHTML = '<p class="video-path">当前视频还没有已保存片段</p>';
    return;
  }

  items
    .slice()
    .sort((a, b) => Number(a.clip_start || 0) - Number(b.clip_start || 0))
    .forEach((item) => {
      const node = elements.recentItemTemplate.content.firstElementChild.cloneNode(true);
      node.querySelector("h3").textContent = `片段 ${formatSeconds(item.clip_start)} - ${formatSeconds(
        item.clip_end
      )}`;
      node.querySelector(
        ".recent-meta"
      ).textContent = `保存时间: ${item.saved_at || item.ui_created_at || "--"}`;
      node.querySelector(".recent-text").textContent = annotationPreviewText(item) || "(空)";
      const deleteBtn = node.querySelector(".recent-delete-btn");
      if (deleteBtn) {
        deleteBtn.disabled = !item.annotation_id;
        deleteBtn.addEventListener("click", async () => {
          if (!item.annotation_id) return;
          try {
            await fetchJson("/api/annotations-delete", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                annotation_id: item.annotation_id,
                annotation_path: item.annotation_path || getCurrentAnnotationPath(),
              }),
            });
            await loadCurrentVideoAnnotations(item.annotation_path || getCurrentAnnotationPath());
            await loadAnnotatedVideos(item.annotation_path || getCurrentAnnotationPath());
            setMessage(elements.saveMessage, "已删除该条标注", "ok");
          } catch (error) {
            setMessage(elements.saveMessage, error.message, "error");
          }
        });
      }
      elements.recentList.appendChild(node);
    });
}

function bindEvents() {
  elements.applyUserBtn.addEventListener("click", async () => {
    try {
      setCurrentUser(elements.userNameInput.value);
      setMessage(elements.rootMessage, `已登录用户: ${state.userName}`, "ok");
      await startWorkspaceForCurrentUser();
    } catch (error) {
      setMessage(elements.rootMessage, error.message, "error");
    }
  });
  elements.userNameInput.addEventListener("keydown", async (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    elements.applyUserBtn.click();
  });
  elements.refreshVideosBtn.addEventListener("click", () => loadVideos());
  elements.uploadBtn.addEventListener("click", handleUpload);
  elements.saveSettingsBtn.addEventListener("click", saveSettings);
  elements.videoPlayer.addEventListener("loadedmetadata", () => {
    const duration = elements.videoPlayer.duration;
    if (state.selectedVideo?.absolute_path) {
      applyVideoDuration(duration, false);
      ensureSelectedVideoDuration(state.selectedVideo).catch(() => {});
    }
  });
  elements.videoPlayer.addEventListener("durationchange", () => {
    const duration = elements.videoPlayer.duration;
    applyVideoDuration(duration, false);
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
  elements.translateBtn.addEventListener("click", () => runTranslate("zh-en"));
  elements.translateToZhBtn.addEventListener("click", () => runTranslate("en-zh"));
  elements.rememberPathBtn.addEventListener("click", async () => {
    const annotationPath = getCurrentAnnotationPath();
    if (!annotationPath) {
      setMessage(elements.saveMessage, "请先填写标注保存路径（.json 或 .jsonl）", "warn");
      return;
    }
    saveAnnotationPathPreference(annotationPath);
    setMessage(elements.saveMessage, `已记住标注路径: ${annotationPath}`, "ok");
    await loadCurrentVideoAnnotations(annotationPath);
    await loadAnnotatedVideos(annotationPath);
  });
  elements.saveBtn.addEventListener("click", saveAnnotation);
  elements.verifyBtn.addEventListener("click", markVideoVerified);
  elements.editingRunBtn.addEventListener("click", startEditingFoley);
  elements.reloadAnnotationsBtn.addEventListener("click", async () => {
    await loadCurrentVideoAnnotations();
    await loadAnnotatedVideos();
  });
  window.addEventListener("pagehide", releaseCurrentVideoClaimSync);
  window.addEventListener("beforeunload", releaseCurrentVideoClaimSync);

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
  const savedUserName = window.localStorage.getItem(USER_NAME_STORAGE_KEY) || "";
  if (savedUserName.trim()) {
    setCurrentUser(savedUserName);
    await startWorkspaceForCurrentUser();
  } else {
    elements.currentUserPill.textContent = "用户: 未登录";
    setMessage(elements.rootMessage, "请先输入用户名并点击进入", "warn");
    toggleEditingFields();
  }
  window.setInterval(() => {
    if (!state.annotatorId) return;
    if (document.visibilityState !== "visible") return;
    if (!state.selectedVideo?.absolute_path) return;
    refreshVideoStatuses().catch(() => {});
  }, VIDEO_STATUS_POLL_INTERVAL_MS);
}

init().catch((error) => {
  setMessage(elements.rootMessage, error.message, "error");
});
