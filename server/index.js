const http = require("http");
const express = require("express");
const cors = require("cors");
const { WebSocketServer } = require("ws");
const { nanoid } = require("nanoid");
const { URL } = require("url");

const PORT = Number(process.env.PORT || 10001);
const ROOM_INACTIVE_TTL_MS = 1000 * 60 * 60 * 4;
const OFFLINE_PARTICIPANT_TTL_MS = 1000 * 60 * 60;
const HEARTBEAT_INTERVAL_MS = 1000 * 10;
const HEARTBEAT_MAX_MISSES = 3;
const CONTROL_ACTIONS = new Set(["play", "pause", "seek", "next", "queue"]);

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const rooms = new Map();

function makeDefaultPermissions() {
  return {
    canPause: true,
    canSeek: false,
    canNext: false,
    canQueue: false,
  };
}

function makeHostPermissions() {
  return {
    canPause: true,
    canSeek: true,
    canNext: true,
    canQueue: true,
  };
}

function clonePlayback(playback) {
  return {
    trackId: playback.trackId,
    title: playback.title,
    artist: playback.artist,
    durationMs: playback.durationMs,
    positionMs: playback.positionMs,
    paused: playback.paused,
    serverTs: playback.serverTs,
    source: playback.source,
  };
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function normalizeString(value, fallback = "") {
  if (typeof value !== "string") {
    return fallback;
  }

  const trimmed = value.trim();
  return trimmed || fallback;
}

function normalizeOptionalUrl(value) {
  if (typeof value !== "string") {
    return "";
  }

  return value.trim();
}

function normalizeIdentifier(value) {
  return normalizeString(value, "");
}

function ensureFiniteNumber(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }

  return value;
}

function validatePlaybackPayload(payload) {
  if (!isPlainObject(payload)) {
    return { ok: false, error: "Playback payload must be an object" };
  }

  const durationMs = ensureFiniteNumber(payload.durationMs);
  const positionMs = ensureFiniteNumber(payload.positionMs);
  if (durationMs === null || positionMs === null) {
    return { ok: false, error: "durationMs and positionMs must be finite numbers" };
  }

  if (durationMs < 0 || positionMs < 0) {
    return { ok: false, error: "durationMs and positionMs must be >= 0" };
  }

  if (durationMs > 0 && positionMs > durationMs) {
    return { ok: false, error: "positionMs must be <= durationMs" };
  }

  if (typeof payload.paused !== "boolean") {
    return { ok: false, error: "paused must be a boolean" };
  }

  return {
    ok: true,
    value: {
      trackId: normalizeString(payload.trackId, ""),
      title: normalizeString(payload.title, ""),
      artist: normalizeString(payload.artist, ""),
      durationMs,
      positionMs,
      paused: payload.paused,
    },
  };
}

function validateControlPayload(action, payload) {
  if (!CONTROL_ACTIONS.has(action)) {
    return { ok: false, error: "Unknown control action" };
  }

  if (payload !== undefined && !isPlainObject(payload)) {
    return { ok: false, error: "Control payload must be an object" };
  }

  if (action === "seek") {
    const positionMs = ensureFiniteNumber(payload && payload.positionMs);
    if (positionMs === null || positionMs < 0) {
      return { ok: false, error: "seek payload.positionMs must be a finite number >= 0" };
    }

    return {
      ok: true,
      value: {
        action,
        payload: { positionMs },
      },
    };
  }

  if (action === "queue") {
    const trackId = normalizeString(payload && payload.trackId, "");
    if (!trackId) {
      return { ok: false, error: "queue payload.trackId is required" };
    }

    return {
      ok: true,
      value: {
        action,
        payload: { trackId },
      },
    };
  }

  return {
    ok: true,
    value: {
      action,
      payload: {},
    },
  };
}

function validatePermissionsPayload(payload) {
  if (!isPlainObject(payload)) {
    return { ok: false, error: "permissions must be an object" };
  }

  const permissionKeys = ["canPause", "canSeek", "canNext", "canQueue"];
  for (const key of permissionKeys) {
    if (typeof payload[key] !== "boolean") {
      return { ok: false, error: `${key} must be a boolean` };
    }
  }

  return {
    ok: true,
    value: {
      canPause: payload.canPause,
      canSeek: payload.canSeek,
      canNext: payload.canNext,
      canQueue: payload.canQueue,
    },
  };
}

function createPlaybackSnapshot(source = "none") {
  const now = Date.now();
  return {
    trackId: "",
    title: "",
    artist: "",
    durationMs: 0,
    positionMs: 0,
    paused: true,
    serverTs: now,
    source,
  };
}

function sendHttpError(res, status, error) {
  return res.status(status).json({ error });
}

function buildRoomState(room) {
  return {
    id: room.id,
    createdAt: room.createdAt,
    hostId: room.hostId,
    roomName: room.roomName,
    playback: clonePlayback(room.playback),
    participants: Array.from(room.participants.values()).map((participant) => ({
      clientId: participant.clientId,
      role: participant.role,
      nickname: participant.nickname,
      avatarUrl: participant.avatarUrl,
      isConnected: Boolean(participant.socket && participant.socket.readyState === 1),
      permissions: participant.permissions,
      joinedAt: participant.joinedAt,
      lastSeenAt: participant.lastSeenAt,
    })),
  };
}

function send(socket, payload) {
  if (!socket || socket.readyState !== 1) {
    return;
  }

  socket.send(JSON.stringify(payload));
}

function broadcastRoom(room, payload, skipClientId = null) {
  for (const participant of room.participants.values()) {
    if (skipClientId && participant.clientId === skipClientId) {
      continue;
    }
    send(participant.socket, payload);
  }
}

function touchParticipant(participant) {
  participant.lastSeenAt = Date.now();
}

function touchRoom(room) {
  room.lastActivityAt = Date.now();
}

function isSocketOpen(socket) {
  return Boolean(socket) && socket.readyState === 1;
}

function logDisconnect(roomId, clientId, reason) {
  console.log(`[ws] room=${roomId} client=${clientId} disconnected: ${reason}`);
}

function buildParticipantPresence(clientId, isConnected, reason = "") {
  return {
    type: "participant_presence",
    clientId,
    isConnected,
    at: Date.now(),
    reason,
  };
}

function pickNextHost(room) {
  for (const participant of room.participants.values()) {
    if (isSocketOpen(participant.socket)) {
      return participant;
    }
  }

  return null;
}

function reassignHostIfNeeded(room, previousHostId) {
  if (room.hostId !== previousHostId) {
    return;
  }

  const nextHost = pickNextHost(room);
  for (const participant of room.participants.values()) {
    participant.role = "listener";
    participant.permissions = makeDefaultPermissions();
  }

  if (!nextHost) {
    room.hostId = "";
    return;
  }

  room.hostId = nextHost.clientId;
  nextHost.role = "host";
  nextHost.permissions = makeHostPermissions();
  broadcastRoom(room, { type: "room_state", state: buildRoomState(room) });
}

function ensureRoomExists(roomId) {
  const room = rooms.get(roomId);
  if (!room) {
    return null;
  }
  return room;
}

function ensureParticipant(room, clientId) {
  const participant = room.participants.get(clientId);
  if (!participant) {
    return null;
  }
  return participant;
}

app.get("/health", (_req, res) => {
  res.json({ ok: true, rooms: rooms.size });
});

app.get("/capabilities", (_req, res) => {
  res.json({
    desktopExtensionOnly: true,
    mobileCompanionMode: "manual_join_by_link",
    playbackSyncModel: "state_and_commands_only",
    usesYnison: false,
  });
});

app.post("/rooms", (req, res) => {
  const body = isPlainObject(req.body) ? req.body : {};
  const hostClientId = normalizeIdentifier(body.hostClientId) || nanoid(10);
  const hostNickname = normalizeString(body.hostNickname, "Host");
  const hostAvatarUrl = normalizeOptionalUrl(body.hostAvatarUrl);
  const roomName = normalizeString(body.roomName, "Sync lobby");

  const roomId = nanoid(8);
  const createdAt = Date.now();

  const room = {
    id: roomId,
    createdAt,
    roomName,
    hostId: hostClientId,
    lastActivityAt: createdAt,
    playback: createPlaybackSnapshot(),
    participants: new Map(),
  };

  room.participants.set(hostClientId, {
    clientId: hostClientId,
    nickname: hostNickname,
    avatarUrl: hostAvatarUrl,
    role: "host",
    permissions: makeHostPermissions(),
    joinedAt: createdAt,
    lastSeenAt: createdAt,
    socket: null,
  });

  rooms.set(roomId, room);
  res.status(201).json({ roomId, clientId: hostClientId, state: buildRoomState(room) });
});

app.post("/rooms/:roomId/join", (req, res) => {
  const roomId = normalizeIdentifier(req.params.roomId);
  if (!roomId) {
    return sendHttpError(res, 400, "roomId is required");
  }

  const room = ensureRoomExists(roomId);
  if (!room) {
    return sendHttpError(res, 404, "Room not found");
  }

  const body = isPlainObject(req.body) ? req.body : {};
  const clientId = normalizeIdentifier(body.clientId) || nanoid(10);
  const nickname = normalizeString(body.nickname, "Guest");
  const avatarUrl = normalizeOptionalUrl(body.avatarUrl);

  const existing = room.participants.get(clientId);
  if (existing) {
    existing.nickname = nickname;
    existing.avatarUrl = avatarUrl;
    touchParticipant(existing);
    touchRoom(room);
    return res.json({ roomId: room.id, clientId, state: buildRoomState(room) });
  }

  room.participants.set(clientId, {
    clientId,
    nickname,
    avatarUrl,
    role: "listener",
    permissions: makeDefaultPermissions(),
    joinedAt: Date.now(),
    lastSeenAt: Date.now(),
    socket: null,
  });

  touchRoom(room);
  return res.status(201).json({ roomId: room.id, clientId, state: buildRoomState(room) });
});

app.get("/rooms/:roomId", (req, res) => {
  const roomId = normalizeIdentifier(req.params.roomId);
  if (!roomId) {
    return sendHttpError(res, 400, "roomId is required");
  }

  const room = ensureRoomExists(roomId);
  if (!room) {
    return sendHttpError(res, 404, "Room not found");
  }
  res.json(buildRoomState(room));
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", (socket, request) => {
  const reqUrl = new URL(request.url, `http://${request.headers.host}`);
  const roomId = reqUrl.searchParams.get("roomId");
  const clientId = reqUrl.searchParams.get("clientId");

  if (!roomId || !clientId) {
    send(socket, { type: "error", error: "roomId and clientId are required" });
    socket.close(1008, "Bad request");
    return;
  }

  const room = ensureRoomExists(roomId);
  if (!room) {
    send(socket, { type: "error", error: "Room not found" });
    socket.close(1008, "Room not found");
    return;
  }

  const participant = ensureParticipant(room, clientId);
  if (!participant) {
    send(socket, { type: "error", error: "Participant not registered in room" });
    socket.close(1008, "Not a participant");
    return;
  }

  if (participant.socket && participant.socket.readyState === 1) {
    participant.socket._disconnectReason = "replaced_by_newer_connection";
    participant.socket.close(1000, "Replaced by newer connection");
  }

  participant.socket = socket;
  socket._missedPongs = 0;
  socket._disconnectReason = "";
  touchParticipant(participant);
  touchRoom(room);

  if (!room.hostId) {
    reassignHostIfNeeded(room, "");
  }

  socket.on("pong", () => {
    socket._missedPongs = 0;
    touchParticipant(participant);
    touchRoom(room);
  });

  send(socket, { type: "room_state", state: buildRoomState(room) });
  broadcastRoom(room, buildParticipantPresence(clientId, true, "connected"), clientId);

  socket.on("message", (raw) => {
    let payload;
    try {
      payload = JSON.parse(raw.toString("utf8"));
    } catch (_err) {
      send(socket, { type: "error", error: "Invalid JSON message" });
      return;
    }

    touchParticipant(participant);
    touchRoom(room);

    if (payload.type === "state_update") {
      const playbackResult = validatePlaybackPayload(payload);
      if (!playbackResult.ok) {
        send(socket, { type: "error", error: playbackResult.error });
        return;
      }

      room.playback = {
        ...playbackResult.value,
        serverTs: Date.now(),
        source: clientId,
      };

      broadcastRoom(room, { type: "state_update", playback: room.playback }, clientId);
      return;
    }

    if (payload.type === "control") {
      const action = normalizeString(payload.action, "");
      const sender = ensureParticipant(room, clientId);
      if (!sender) {
        send(socket, { type: "error", error: "Participant not registered in room" });
        return;
      }

      const controlResult = validateControlPayload(action, payload.payload);
      if (!controlResult.ok) {
        send(socket, { type: "error", error: controlResult.error });
        return;
      }

      const isHost = sender && sender.role === "host";
      const allowed =
        isHost ||
        (action === "play" && sender.permissions.canPause) ||
        (action === "pause" && sender.permissions.canPause) ||
        (action === "seek" && sender.permissions.canSeek) ||
        (action === "next" && sender.permissions.canNext) ||
        (action === "queue" && sender.permissions.canQueue);

      if (!allowed) {
        send(socket, { type: "error", error: "Permission denied for this action" });
        return;
      }

      broadcastRoom(
        room,
        {
          type: "control",
          action: controlResult.value.action,
          payload: controlResult.value.payload,
          from: clientId,
        },
        clientId
      );
      return;
    }

    if (payload.type === "permissions_update") {
      if (participant.role !== "host") {
        send(socket, { type: "error", error: "Only host can update permissions" });
        return;
      }

      const targetId = String(payload.clientId || "");
      const target = ensureParticipant(room, targetId);
      if (!target) {
        send(socket, { type: "error", error: "Target participant not found" });
        return;
      }

      const permissionsResult = validatePermissionsPayload(payload.permissions);
      if (!permissionsResult.ok) {
        send(socket, { type: "error", error: permissionsResult.error });
        return;
      }

      target.permissions = permissionsResult.value;

      broadcastRoom(room, {
        type: "permissions_update",
        clientId: targetId,
        permissions: target.permissions,
      });
      return;
    }

    if (payload.type === "ping") {
      socket._missedPongs = 0;
      send(socket, { type: "pong", now: Date.now() });
      return;
    }

    send(socket, { type: "error", error: "Unknown message type" });
  });

  socket.on("close", () => {
    if (participant.socket !== socket) {
      return;
    }

    participant.socket = null;
    touchParticipant(participant);
    touchRoom(room);
    const disconnectReason = socket._disconnectReason || "socket_closed";
    logDisconnect(room.id, clientId, disconnectReason);
    reassignHostIfNeeded(room, clientId);
    broadcastRoom(room, buildParticipantPresence(clientId, false, disconnectReason));
  });
});

setInterval(() => {
  for (const room of rooms.values()) {
    for (const participant of room.participants.values()) {
      const socket = participant.socket;
      if (!isSocketOpen(socket)) {
        continue;
      }

      socket._missedPongs = Number(socket._missedPongs || 0) + 1;
      if (socket._missedPongs > HEARTBEAT_MAX_MISSES) {
        socket._disconnectReason = "heartbeat_timeout";
        socket.terminate();
        continue;
      }

      try {
        socket.ping();
      } catch (_err) {
        socket._disconnectReason = "heartbeat_ping_failed";
        socket.terminate();
      }
    }
  }
}, HEARTBEAT_INTERVAL_MS);

setInterval(() => {
  const now = Date.now();
  for (const [roomId, room] of rooms.entries()) {
    for (const [participantId, participant] of room.participants.entries()) {
      const isOffline = !isSocketOpen(participant.socket);
      const isExpired = now - participant.lastSeenAt > OFFLINE_PARTICIPANT_TTL_MS;
      if (isOffline && isExpired) {
        room.participants.delete(participantId);
        reassignHostIfNeeded(room, participantId);
      }
    }

    const hasConnected = Array.from(room.participants.values()).some(
      (participant) => isSocketOpen(participant.socket)
    );
    const isStale = now - room.lastActivityAt > ROOM_INACTIVE_TTL_MS;
    if (!hasConnected && isStale) {
      rooms.delete(roomId);
    }
  }
}, 1000 * 60 * 5);

server.listen(PORT, () => {
  // Log only once on startup to keep console output quiet.
  console.log(`Yandex sync backend started on http://localhost:${PORT}`);
});
