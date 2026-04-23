const http = require("http");
const express = require("express");
const cors = require("cors");
const { WebSocketServer } = require("ws");

const PORT = Number(process.env.PORT || 11001);

const PROTOCOL_VERSION = 1;
const MAX_HTTP_JSON_BYTES = "8mb";
const MAX_WS_MESSAGE_BYTES = 8 * 1024 * 1024;

const ROOM_STALE_TIMEOUT_MS = Number(process.env.ROOM_STALE_TIMEOUT_MS || 10000);
const OFFLINE_PARTICIPANT_TTL_MS = Number(process.env.OFFLINE_PARTICIPANT_TTL_MS || 60 * 60 * 1000);
const ROOM_CLEANUP_TTL_MS = Number(process.env.ROOM_CLEANUP_TTL_MS || 60 * 60 * 1000);
const COMMAND_REQUEST_TTL_MS = Number(process.env.COMMAND_REQUEST_TTL_MS || 60 * 1000);
const CLEANUP_INTERVAL_MS = Number(process.env.CLEANUP_INTERVAL_MS || 3000);

const SERVER_CLIENT_ID = "server";
const ALLOWED_ORIGINS = new Set(["https://music.yandex.ru"]);
const DELEGATED_COMMANDS = new Set(["play", "pause", "seek", "next", "previous"]);
const ALL_COMMANDS = new Set(["play", "pause", "seek", "next", "previous", "changeTrack", "closeRoom", "kick"]);
const COMMAND_CLOSE_ROOM = "closeRoom";
const COMMAND_KICK = "kick";
const LEAVE_CLOSE_ROOM = "closeRoom";

const app = express();
const server = http.createServer(app);

const rooms = new Map();
const sessions = new Map();

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isFiniteNumber(value) {
  return typeof value === "number" && Number.isFinite(value);
}

function isValidClientId(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizeText(value) {
  return typeof value === "string" ? value.trim() : "";
}

function normalizeTrackText(value) {
  return normalizeText(value)
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function isAllowedCorsOrigin(origin) {
  if (!origin) {
    return true;
  }
  return origin.startsWith("chrome-extension://") || ALLOWED_ORIGINS.has(origin);
}

function now() {
  return Date.now();
}

function toEnvelope(type, roomId, clientId, payload) {
  return {
    type,
    v: PROTOCOL_VERSION,
    roomId,
    clientId,
    ts: now(),
    payload,
  };
}

function send(socket, payload) {
  if (!socket || socket.readyState !== 1) {
    return;
  }

  socket.send(JSON.stringify(payload));
}

function sendError(socket, roomId, clientId, code, message) {
  send(socket, toEnvelope("ERROR", roomId, clientId, { code, message }));
}

function makePermissions(participant) {
  if (!participant) {
    return { isHost: false, canControl: false, canDelegate: false };
  }

  const isHost = participant.role === "host";
  return {
    isHost,
    canControl: isHost ? true : Boolean(participant.canControl),
    canDelegate: isHost ? Boolean(participant.canDelegate) : false,
  };
}

function participantsForSnapshot(room) {
  return Array.from(room.participants.values()).map((participant) => ({
    clientId: participant.clientId,
    nickname: participant.nickname,
    avatarUrl: participant.avatarUrl,
    role: participant.role,
    isConnected: Boolean(participant.isConnected),
  }));
}

function roomSnapshotPayload(room, viewerClientId) {
  return {
    id: room.id,
    hostClientId: room.hostClientId,
    participants: participantsForSnapshot(room),
    track: room.track,
    playbackState: room.playbackState,
    permissions: makePermissions(room.participants.get(viewerClientId)),
    stateVersion: room.stateVersion,
    updatedAt: room.updatedAt,
    stale: room.stale,
  };
}

function connectedPayload(room, viewerClientId) {
  return {
    clientId: viewerClientId,
    role: room.participants.get(viewerClientId)?.role || "listener",
    permissions: makePermissions(room.participants.get(viewerClientId)),
    roomState: {
      id: room.id,
      hostClientId: room.hostClientId,
      participants: participantsForSnapshot(room),
      track: room.track,
      playbackState: room.playbackState,
      stateVersion: room.stateVersion,
      updatedAt: room.updatedAt,
    },
  };
}

function sendRoomParticipants(room) {
  for (const participant of room.participants.values()) {
    if (!participant.socket || participant.socket.readyState !== 1) {
      continue;
    }

    send(
      participant.socket,
      toEnvelope("ROOM_PARTICIPANTS", room.id, SERVER_CLIENT_ID, {
        participants: participantsForSnapshot(room),
        permissions: makePermissions(participant),
      }),
    );
  }
}

function closeRoom(room, byClientId, reason) {
  const payload = {
    reason: normalizeText(reason) || LEAVE_CLOSE_ROOM,
    by: normalizeText(byClientId) || normalizeText(room.hostClientId) || SERVER_CLIENT_ID,
  };
  room.isClosing = true;

  for (const participant of room.participants.values()) {
    if (participant.socket && participant.socket.readyState === 1) {
      send(participant.socket, toEnvelope("ROOM_CLOSED", room.id, SERVER_CLIENT_ID, payload));
      participant.socket.close(1000, "room closed");
    }
  }

  rooms.delete(room.id);
}

function normalizeTrackPayload(value) {
  if (!isPlainObject(value)) {
    return { ok: false, error: "TRACK_INFO.payload.track is required" };
  }

  const title = normalizeTrackText(value.title);
  if (!title) {
    return { ok: false, error: "track.title is required" };
  }

  if (!Array.isArray(value.artists)) {
    return { ok: false, error: "track.artists must be an array" };
  }

  const artists = value.artists
    .map((artist) => normalizeTrackText(artist))
    .filter(Boolean);

  if (!artists.length) {
    return { ok: false, error: "track.artists must contain at least one value" };
  }

  const durationSec = value.durationSec;
  if (!isFiniteNumber(durationSec) || durationSec < 0) {
    return { ok: false, error: "track.durationSec must be a non-negative number" };
  }

  return {
    ok: true,
    value: {
      title,
      artists,
      durationSec,
    },
  };
}

function normalizePlaybackPayload(value) {
  if (!isPlainObject(value)) {
    return { ok: false, error: "PLAYBACK_STATE.state is required" };
  }

  if (typeof value.isPlaying !== "boolean") {
    return { ok: false, error: "state.isPlaying must be a boolean" };
  }

  if (!isFiniteNumber(value.positionSec) || value.positionSec < 0) {
    return { ok: false, error: "state.positionSec must be a non-negative number" };
  }

  if (!isFiniteNumber(value.durationSec) || value.durationSec < 0) {
    return { ok: false, error: "state.durationSec must be a non-negative number" };
  }

  if (value.positionSec > value.durationSec && value.durationSec > 0) {
    return { ok: false, error: "state.positionSec cannot exceed state.durationSec" };
  }

  if (!isFiniteNumber(value.positionAtServerMs) || value.positionAtServerMs < 0) {
    return { ok: false, error: "state.positionAtServerMs must be a non-negative number" };
  }

  return {
    ok: true,
    value: {
      isPlaying: value.isPlaying,
      positionSec: value.positionSec,
      durationSec: value.durationSec,
      positionAtServerMs: value.positionAtServerMs,
    },
  };
}

function parseEnvelope(rawMessage) {
  let message;
  try {
    message = JSON.parse(rawMessage.toString("utf8"));
  } catch (_error) {
    return { ok: false, errorCode: "INVALID_MESSAGE", errorMessage: "Invalid JSON" };
  }

  if (!isPlainObject(message)) {
    return { ok: false, errorCode: "INVALID_MESSAGE", errorMessage: "Message must be an object" };
  }

  const type = normalizeText(message.type);
  if (!type) {
    return { ok: false, errorCode: "INVALID_MESSAGE", errorMessage: "type is required" };
  }

  const roomId = normalizeText(message.roomId);
  if (!roomId) {
    return { ok: false, errorCode: "INVALID_MESSAGE", errorMessage: "roomId is required" };
  }

  const clientId = normalizeText(message.clientId);
  if (!isValidClientId(clientId)) {
    return { ok: false, errorCode: "INVALID_MESSAGE", errorMessage: "clientId is required" };
  }

  if (!isFiniteNumber(message.v) || message.v !== PROTOCOL_VERSION) {
    return {
      ok: false,
      errorCode: "INVALID_VERSION",
      errorMessage: "Unsupported protocol version",
      roomId,
      clientId,
      type,
    };
  }

  if (!isFiniteNumber(message.ts)) {
    return {
      ok: false,
      errorCode: "INVALID_MESSAGE",
      errorMessage: "ts is required",
      roomId,
      clientId,
      type,
    };
  }

  if (!isPlainObject(message.payload)) {
    return {
      ok: false,
      errorCode: "INVALID_MESSAGE",
      errorMessage: "payload is required",
      roomId,
      clientId,
      type,
    };
  }

  return {
    ok: true,
    value: {
      type,
      roomId,
      clientId,
      payload: message.payload,
    },
  };
}

function createRoom(roomId, hostClientId) {
  const timestamp = now();
  return {
    id: roomId,
    createdAt: timestamp,
    updatedAt: timestamp,
    stale: false,
    hostClientId,
    isClosing: false,
    stateVersion: 1,
    trackMetaVersion: 0,
    track: null,
    playbackState: null,
    participants: new Map(),
    lastHostHeartbeatAt: timestamp,
    commandRequestHistory: new Map(),
  };
}

function createParticipant(clientId, nickname, avatarUrl, role) {
  return {
    clientId,
    nickname,
    avatarUrl,
    role,
    canControl: role === "host",
    canDelegate: role === "host",
    isConnected: false,
    socket: null,
    lastSeenAt: now(),
  };
}

function getRoom(roomId) {
  return rooms.get(roomId) || null;
}

function cleanupCommandHistory(room, currentTs = now()) {
  for (const [clientId, requestMap] of room.commandRequestHistory.entries()) {
    for (const [requestId, ts] of requestMap.entries()) {
      if (currentTs - ts > COMMAND_REQUEST_TTL_MS) {
        requestMap.delete(requestId);
      }
    }

    if (!requestMap.size) {
      room.commandRequestHistory.delete(clientId);
    }
  }
}

function isCommandDuplicate(room, clientId, requestId) {
  const byClient = room.commandRequestHistory.get(clientId);
  if (!byClient) {
    return false;
  }

  return byClient.has(requestId);
}

function rememberCommand(room, clientId, requestId) {
  let byClient = room.commandRequestHistory.get(clientId);
  if (!byClient) {
    byClient = new Map();
    room.commandRequestHistory.set(clientId, byClient);
  }

  byClient.set(requestId, now());
}

function canExecuteCommand(participant, action) {
  if (!participant) {
    return false;
  }

  if (participant.role === "host") {
    return true;
  }

  if (!participant.canControl) {
    return false;
  }

  return DELEGATED_COMMANDS.has(action);
}

function setSessionRoomClient(socket, roomId, clientId) {
  sessions.set(socket, { roomId, clientId });
}

function removeSession(socket) {
  sessions.delete(socket);
}

function setConnected(room, participant, socket, clientId) {
  if (participant.socket && participant.socket !== socket && participant.socket.readyState === 1) {
    participant.socket.close(1000, "replaced by newer connection");
  }

  participant.socket = socket;
  participant.isConnected = true;
  participant.lastSeenAt = now();
  if (participant.role === "host") {
    room.hostClientId = participant.clientId;
    room.lastHostHeartbeatAt = now();
    room.stale = false;
    participant.canControl = true;
    participant.canDelegate = true;
  }

  setSessionRoomClient(socket, room.id, clientId);
}

function setDisconnected(room, participant) {
  participant.isConnected = false;
  participant.socket = null;
  participant.lastSeenAt = now();
}

function markRoomTouch(room, participant = null) {
  room.updatedAt = now();
  if (participant && participant.role === "host") {
    room.lastHostHeartbeatAt = now();
    room.stale = false;
  }
}

function handleHello(socket, roomId, clientId, payload) {
  const roleHint = payload && payload.roleHint === "host" ? "host" : "listener";
  const nickname = normalizeText(payload.nickname) || "Guest";
  const avatarUrl = normalizeText(payload.avatarUrl);

  let room = getRoom(roomId);

  if (!room) {
    if (roleHint !== "host") {
      sendError(socket, roomId, clientId, "ROOM_NOT_FOUND", "Room not found");
      return;
    }

    room = createRoom(roomId, clientId);
    rooms.set(roomId, room);
  }

  let participant = room.participants.get(clientId);
  if (!participant) {
    let role = roleHint;
    if (role === "host" && room.hostClientId && room.hostClientId !== clientId) {
      role = "listener";
    }

    if (!room.hostClientId && roleHint === "host") {
      role = "host";
      room.hostClientId = clientId;
    }

    participant = createParticipant(clientId, nickname, avatarUrl, role);
    room.participants.set(clientId, participant);
  } else {
    participant.nickname = nickname || participant.nickname;
    participant.avatarUrl = avatarUrl || participant.avatarUrl;
  }

  if (participant.role === "host") {
    participant.canControl = true;
    participant.canDelegate = true;
    room.hostClientId = participant.clientId;
  }

  setConnected(room, participant, socket, clientId);
  room.updatedAt = now();

  send(socket, toEnvelope("CONNECTED", room.id, clientId, connectedPayload(room, clientId)));
  send(socket, toEnvelope("ROOM_SNAPSHOT", room.id, clientId, roomSnapshotPayload(room, clientId)));
  sendRoomParticipants(room);
}

function handleTrackInfo(socket, room, participant, payload, roomId, clientId) {
  if (!participant || participant.role !== "host") {
    sendError(socket, roomId, clientId, "NO_PERMISSION", "Only host can publish TRACK_INFO");
    return;
  }

  if (!isPlainObject(payload)) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "TRACK_INFO.payload must be an object");
    return;
  }

  const trackResult = normalizeTrackPayload(payload.track);
  if (!trackResult.ok) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", trackResult.error);
    return;
  }

  const stateResult = normalizePlaybackPayload(payload.state);
  if (!stateResult.ok) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", stateResult.error);
    return;
  }

  if (!Number.isInteger(payload.stateVersion)) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "TRACK_INFO.stateVersion is required");
    return;
  }

  const incomingStateVersion = payload.stateVersion;
  if (incomingStateVersion <= room.stateVersion) {
    return;
  }

  room.track = trackResult.value;
  room.trackMetaVersion = Number.isInteger(payload.trackMetaVersion)
    ? Math.max(room.trackMetaVersion, payload.trackMetaVersion)
    : room.trackMetaVersion + 1;
  room.playbackState = {
    ...stateResult.value,
    stateVersion: incomingStateVersion,
  };
  room.stateVersion = incomingStateVersion;
  room.updatedAt = now();
  markRoomTouch(room, participant);

  const outboundPayload = {
    track: room.track,
    state: room.playbackState,
    trackMetaVersion: room.trackMetaVersion,
    stateVersion: incomingStateVersion,
  };

  for (const receiver of room.participants.values()) {
    if (!receiver.socket || receiver.socket.readyState !== 1) {
      continue;
    }

    send(receiver.socket, toEnvelope("TRACK_INFO", room.id, SERVER_CLIENT_ID, outboundPayload));
  }
}

function handlePlaybackState(socket, room, participant, payload, roomId, clientId) {
  if (!participant || participant.role !== "host") {
    sendError(socket, roomId, clientId, "NO_PERMISSION", "Only host can publish PLAYBACK_STATE");
    return;
  }

  if (!isPlainObject(payload) || !isPlainObject(payload.state)) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "PLAYBACK_STATE.payload.state is required");
    return;
  }

  if (!Number.isInteger(payload.stateVersion)) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "PLAYBACK_STATE.stateVersion is required");
    return;
  }

  const incomingStateVersion = payload.stateVersion;
  if (incomingStateVersion <= room.stateVersion) {
    return;
  }

  const stateResult = normalizePlaybackPayload(payload.state);
  if (!stateResult.ok) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", stateResult.error);
    return;
  }

  room.playbackState = {
    ...stateResult.value,
    stateVersion: incomingStateVersion,
  };
  room.stateVersion = incomingStateVersion;
  room.updatedAt = now();
  markRoomTouch(room, participant);

  const outboundPayload = {
    state: room.playbackState,
    stateVersion: incomingStateVersion,
  };

  for (const receiver of room.participants.values()) {
    if (receiver.role === "host") {
      continue;
    }

    if (!receiver.socket || receiver.socket.readyState !== 1) {
      continue;
    }

    send(receiver.socket, toEnvelope("PLAYBACK_STATE", room.id, SERVER_CLIENT_ID, outboundPayload));
  }
}

function handleCommandRequest(socket, room, participant, payload, roomId, clientId) {
  if (!isPlainObject(payload)) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "COMMAND_REQUEST.payload is required");
    return;
  }

  const requestId = normalizeText(payload.requestId);
  if (!requestId) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "requestId is required");
    return;
  }

  const action = normalizeText(payload.action);
  if (!action || !ALL_COMMANDS.has(action)) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "action is invalid");
    return;
  }

  if (isCommandDuplicate(room, clientId, requestId)) {
    return;
  }
  rememberCommand(room, clientId, requestId);

  if (participant.role === "host") {
    if (action === COMMAND_CLOSE_ROOM) {
      return closeRoom(room, clientId, normalizeText(payload.reason) || LEAVE_CLOSE_ROOM);
    }

    if (action === COMMAND_KICK) {
      const targetClientId = normalizeText(payload.targetClientId);
      if (!targetClientId) {
        sendError(socket, roomId, clientId, "INVALID_MESSAGE", "targetClientId is required for kick");
        return;
      }

      const target = room.participants.get(targetClientId);
      if (!target) {
        sendError(socket, roomId, clientId, "INVALID_MESSAGE", "Target participant not found");
        return;
      }

      if (targetClientId === room.hostClientId) {
        sendError(socket, roomId, clientId, "INVALID_MESSAGE", "Cannot kick host");
        return;
      }

      setDisconnected(room, target);
      room.participants.delete(targetClientId);
      if (target.socket && target.socket.readyState === 1) {
        target.socket.close(1000, "kicked by host");
      }

      sendRoomParticipants(room);
      return;
    }

    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "Host should use host-only sync commands");
    return;
  }

  if (!canExecuteCommand(participant, action)) {
    sendError(socket, roomId, clientId, "NO_PERMISSION", "No permission for command");
    return;
  }

  const host = room.participants.get(room.hostClientId);
  if (!host || !host.socket || host.socket.readyState !== 1) {
    sendError(socket, roomId, clientId, "NO_HOST", "Host is not connected");
    return;
  }

  const forwardPayload = {
    requestId,
    action,
  };

  if (action === "seek") {
    if (!isFiniteNumber(payload.positionSec) || payload.positionSec < 0) {
      sendError(socket, roomId, clientId, "INVALID_MESSAGE", "positionSec is required for seek");
      return;
    }

    forwardPayload.positionSec = payload.positionSec;
  }

  if (payload.targetClientId) {
    forwardPayload.targetClientId = normalizeText(payload.targetClientId);
  }

  if (action === "changeTrack" && isPlainObject(payload.track)) {
    const trackResult = normalizeTrackPayload(payload.track);
    if (!trackResult.ok) {
      sendError(socket, roomId, clientId, "INVALID_MESSAGE", trackResult.error);
      return;
    }

    forwardPayload.track = trackResult.value;
  }

  send(
    host.socket,
    toEnvelope("COMMAND_TO_HOST", room.id, room.hostClientId, forwardPayload),
  );
}

function handleControlTransfer(socket, room, participant, payload, roomId, clientId) {
  if (!participant || participant.role !== "host") {
    sendError(socket, roomId, clientId, "NO_PERMISSION", "Only host can transfer control");
    return;
  }

  if (!isPlainObject(payload)) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "CONTROL_TRANSFER.payload is required");
    return;
  }

  const targetClientId = normalizeText(payload.targetClientId);
  if (!targetClientId) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "targetClientId is required");
    return;
  }

  const target = room.participants.get(targetClientId);
  if (!target) {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "targetClientId not found");
    return;
  }

  if (target.role === "host") {
    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "Cannot transfer control to host");
    return;
  }

  target.canControl = Boolean(payload.canControl);

  const payloadOut = {
    clientId: targetClientId,
    permissions: makePermissions(target),
    role: target.role,
  };

  for (const receiver of room.participants.values()) {
    if (!receiver.socket || receiver.socket.readyState !== 1) {
      continue;
    }

    send(receiver.socket, toEnvelope("CONTROL_GRANTED", room.id, SERVER_CLIENT_ID, payloadOut));
  }

  sendRoomParticipants(room);
}

function handleLeave(socket, room, participant, payload, roomId, clientId) {
  if (!participant) {
    return;
  }

  const reason = normalizeText(payload.reason);
  if (reason === LEAVE_CLOSE_ROOM) {
    if (!room.isClosing) {
      return sendError(socket, roomId, clientId, "NO_PERMISSION", "Room is not in close flow");
    }

    setDisconnected(room, participant);
    room.participants.delete(clientId);
    if (socket && socket.readyState === 1) {
      socket.close(1000, "leave after close");
    }
    room.updatedAt = now();
    return;
  }

  setDisconnected(room, participant);
  room.participants.delete(clientId);

  if (participant.role === "host") {
    room.hostClientId = "";
    room.stale = true;
    room.lastHostHeartbeatAt = 0;
  }

  if (socket && socket.readyState === 1) {
    socket.close(1000, "leave");
  }

  room.updatedAt = now();
  sendRoomParticipants(room);
}

function requireSession(socket, roomId, clientId) {
  const session = sessions.get(socket);
  if (!session) {
    return false;
  }

  if (session.roomId !== roomId || session.clientId !== clientId) {
    return false;
  }

  return true;
}

app.use((req, res, next) => {
  if (req.headers["access-control-request-private-network"] === "true") {
    res.setHeader("Access-Control-Allow-Private-Network", "true");
  }
  next();
});

app.use(
  cors({
    origin: (origin, callback) => {
      if (isAllowedCorsOrigin(origin)) {
        callback(null, true);
      } else {
        callback(new Error("Origin not allowed"));
      }
    },
    methods: ["GET", "POST", "OPTIONS", "HEAD"],
    allowedHeaders: ["Content-Type", "Authorization"],
  }),
);

app.use(express.json({ limit: MAX_HTTP_JSON_BYTES }));

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, ts: now() });
});

app.get("/api/rooms/:roomId", (req, res) => {
  const roomId = normalizeText(req.params.roomId);
  if (!roomId) {
    return res.status(400).json({ error: "roomId is required" });
  }

  const room = getRoom(roomId);
  if (!room) {
    return res.status(404).json({ error: "Room not found" });
  }

  res.json({
    id: room.id,
    hostClientId: room.hostClientId,
    stateVersion: room.stateVersion,
    updatedAt: room.updatedAt,
    stale: room.stale,
    createdAt: room.createdAt,
    track: room.track,
    playbackState: room.playbackState,
    participants: participantsForSnapshot(room),
  });
});

app.get("/api/stats", (_req, res) => {
  let connectedClients = 0;
  let staleRooms = 0;

  for (const room of rooms.values()) {
    if (room.stale) {
      staleRooms += 1;
    }

    for (const participant of room.participants.values()) {
      if (participant.isConnected && participant.socket) {
        connectedClients += 1;
      }
    }
  }

  res.json({
    rooms: rooms.size,
    staleRooms,
    connectedClients,
    ts: now(),
  });
});

const wss = new WebSocketServer({
  server,
  path: "/api/ws",
  maxPayload: MAX_WS_MESSAGE_BYTES,
});

wss.on("connection", (socket, request) => {
  const origin = request.headers.origin || "";
  if (!isAllowedCorsOrigin(origin)) {
    socket.close(1008, "Origin not allowed");
    return;
  }

  socket.on("message", (rawMessage) => {
    const parsed = parseEnvelope(rawMessage);
    if (!parsed.ok) {
      sendError(socket, parsed.roomId || "", parsed.clientId || "", parsed.errorCode, parsed.errorMessage);
      return;
    }

    const { type, roomId, clientId, payload } = parsed.value;
    if (!isValidClientId(roomId) || !isValidClientId(clientId)) {
      return sendError(socket, roomId, clientId, "INVALID_MESSAGE", "Invalid identifiers");
    }

    if (type === "HELLO") {
      return handleHello(socket, roomId, clientId, payload);
    }

    if (!requireSession(socket, roomId, clientId)) {
      return sendError(socket, roomId, clientId, "INVALID_MESSAGE", "Send HELLO first for this socket");
    }

    const room = getRoom(roomId);
    if (!room) {
      return sendError(socket, roomId, clientId, "ROOM_NOT_FOUND", "Room not found");
    }

    const participant = room.participants.get(clientId);
    if (!participant) {
      return sendError(socket, roomId, clientId, "INVALID_MESSAGE", "Client is not in room");
    }

    if (participant.socket !== socket) {
      return sendError(socket, roomId, clientId, "INVALID_MESSAGE", "Socket mismatch");
    }

    room.updatedAt = now();

    if (type === "TRACK_INFO") {
      return handleTrackInfo(socket, room, participant, payload, roomId, clientId);
    }

    if (type === "PLAYBACK_STATE") {
      return handlePlaybackState(socket, room, participant, payload, roomId, clientId);
    }

    if (type === "COMMAND_REQUEST") {
      return handleCommandRequest(socket, room, participant, payload, roomId, clientId);
    }

    if (type === "CONTROL_TRANSFER") {
      return handleControlTransfer(socket, room, participant, payload, roomId, clientId);
    }

    if (type === "LEAVE") {
      return handleLeave(socket, room, participant, payload, roomId, clientId);
    }

    sendError(socket, roomId, clientId, "INVALID_MESSAGE", "Unknown event type");
  });

  socket.on("close", () => {
    const session = sessions.get(socket);
    if (!session) {
      return;
    }

    const room = rooms.get(session.roomId);
    if (!room) {
      removeSession(socket);
      return;
    }

    const participant = room.participants.get(session.clientId);
    if (!participant || participant.socket !== socket) {
      removeSession(socket);
      return;
    }

    setDisconnected(room, participant);
    if (participant.role === "host") {
      room.stale = true;
      room.lastHostHeartbeatAt = 0;
    }

    room.updatedAt = now();
    sendRoomParticipants(room);
    removeSession(socket);
  });
});

setInterval(() => {
  const timestamp = now();

  for (const room of rooms.values()) {
    const host = room.participants.get(room.hostClientId);
    if (!host || !host.isConnected || timestamp - room.lastHostHeartbeatAt > ROOM_STALE_TIMEOUT_MS) {
      room.stale = true;
    } else {
      room.stale = false;
    }

    cleanupCommandHistory(room, timestamp);

    for (const [clientId, participant] of room.participants.entries()) {
      if (!participant.isConnected && timestamp - participant.lastSeenAt > OFFLINE_PARTICIPANT_TTL_MS) {
        room.participants.delete(clientId);

        if (clientId === room.hostClientId) {
          room.hostClientId = "";
        }
      }
    }

    if (!room.participants.size && timestamp - room.updatedAt > ROOM_CLEANUP_TTL_MS) {
      rooms.delete(room.id);
    }
  }
}, CLEANUP_INTERVAL_MS);

server.listen(PORT, () => {
  console.log(`ynison sync API listening on port ${PORT}`);
});
