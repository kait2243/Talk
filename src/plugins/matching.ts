import { FastifyPluginAsync } from "fastify";
import type WebSocket from "ws";
import { randomUUID } from "crypto";
import jwt from "jsonwebtoken";
import { prisma } from "../lib/prisma";
import { provisionEphemeralTurnServer, destroyEphemeralTurnServer } from "../services/cloud.service";

const JWT_SECRET = process.env.JWT_SECRET ?? "";
const MIN_TOKEN_BALANCE_TO_MATCH = 5;

const WS_CLOSE_INVALID_TOKEN = 4001;
const WS_CLOSE_USER_NOT_FOUND = 4002;
const WS_CLOSE_INSUFFICIENT_BALANCE = 4003;
const WS_CLOSE_ALREADY_CONNECTED = 4004;

const RELAYED_ACTIONS = new Set(["OFFER", "ANSWER", "ICE_CANDIDATE"]);

interface MatchJoinJwtPayload {
  userId: string;
}

interface Session {
  userA: string;
  userB: string;
  dropletId: string;
}

// Ordered FIFO queue of userIds currently waiting for a peer.
const matchingPool: string[] = [];

// Live socket handle for every userId currently connected to /v1/matching/join.
const activeConnections = new Map<string, WebSocket>();

// Live WebRTC signaling sessions, keyed by sessionId.
const activeSessions = new Map<string, Session>();

// Reverse lookup so an inbound message or a disconnect can find the session
// a given userId currently belongs to without scanning activeSessions.
const userSessionMap = new Map<string, string>();

function removeFromPool(userId: string): void {
  const idx = matchingPool.indexOf(userId);
  if (idx !== -1) {
    matchingPool.splice(idx, 1);
  }
}

function getPeerId(session: Session, userId: string): string {
  return session.userA === userId ? session.userB : session.userA;
}

function teardownSession(userId: string): void {
  const sessionId = userSessionMap.get(userId);
  if (!sessionId) return;

  const session = activeSessions.get(sessionId);
  if (!session) {
    userSessionMap.delete(userId);
    return;
  }

  const peerId = getPeerId(session, userId);
  const peerSocket = activeConnections.get(peerId);
  if (peerSocket && peerSocket.readyState === peerSocket.OPEN) {
    peerSocket.send(JSON.stringify({ action: "PEER_DISCONNECTED" }));
  }

  activeSessions.delete(sessionId);
  userSessionMap.delete(session.userA);
  userSessionMap.delete(session.userB);

  // Fire-and-forget: destroy the droplet the instant either side drops so
  // spend doesn't linger, without blocking socket cleanup on DO API latency.
  destroyEphemeralTurnServer(session.dropletId).catch((err) => {
    console.error(
      `Failed to destroy TURN droplet ${session.dropletId} for session ${sessionId}:`,
      err
    );
  });
}

function verifyToken(token: string | undefined): MatchJoinJwtPayload | null {
  if (!token) return null;
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    if (
      typeof decoded === "object" &&
      decoded !== null &&
      typeof (decoded as Record<string, unknown>).userId === "string"
    ) {
      return { userId: (decoded as Record<string, unknown>).userId as string };
    }
    return null;
  } catch {
    return null;
  }
}

const matchingPlugin: FastifyPluginAsync = async (fastify) => {
  fastify.get(
    "/v1/matching/join",
    { websocket: true },
    async (socket, request) => {
      const query = request.query as Record<string, string | undefined>;
      const payload = verifyToken(query.token);

      if (!payload) {
        socket.close(WS_CLOSE_INVALID_TOKEN, "Invalid or missing token");
        return;
      }

      const { userId } = payload;

      if (activeConnections.has(userId)) {
        socket.close(WS_CLOSE_ALREADY_CONNECTED, "User already in queue");
        return;
      }

      const user = await prisma.user.findUnique({
        where: { id: userId },
        select: { tokenBalance: true },
      });

      if (!user) {
        socket.close(WS_CLOSE_USER_NOT_FOUND, "User not found");
        return;
      }

      if (user.tokenBalance < MIN_TOKEN_BALANCE_TO_MATCH) {
        socket.close(WS_CLOSE_INSUFFICIENT_BALANCE, "Insufficient token balance");
        return;
      }

      activeConnections.set(userId, socket);

      const cleanup = () => {
        removeFromPool(userId);
        teardownSession(userId);
        activeConnections.delete(userId);
      };

      socket.on("close", cleanup);
      socket.on("error", cleanup);

      socket.on("message", (raw) => {
        let parsed: unknown;
        try {
          parsed = JSON.parse(raw.toString());
        } catch {
          return;
        }

        if (
          typeof parsed !== "object" ||
          parsed === null ||
          typeof (parsed as Record<string, unknown>).action !== "string"
        ) {
          return;
        }

        const { action } = parsed as { action: string };
        if (!RELAYED_ACTIONS.has(action)) return;

        const sessionId = userSessionMap.get(userId);
        if (!sessionId) return;

        const session = activeSessions.get(sessionId);
        if (!session) return;

        const peerId = getPeerId(session, userId);
        const peerSocket = activeConnections.get(peerId);
        if (!peerSocket || peerSocket.readyState !== peerSocket.OPEN) return;

        peerSocket.send(JSON.stringify(parsed));
      });

      // Pair with the oldest still-connected waiting user, if any, skipping
      // any stale entries left behind by a connection that dropped without
      // its "close" handler having run yet.
      let peerSocket: WebSocket | undefined;
      let peerId: string | undefined;

      while (matchingPool.length > 0) {
        const candidateId = matchingPool.shift()!;
        const candidateSocket = activeConnections.get(candidateId);
        if (candidateSocket && candidateSocket.readyState === candidateSocket.OPEN) {
          peerId = candidateId;
          peerSocket = candidateSocket;
          break;
        }
      }

      if (peerId && peerSocket) {
        const sessionId = randomUUID();
        const matchedPeerId = peerId;
        const matchedPeerSocket = peerSocket;

        let provisioned;
        try {
          provisioned = await provisionEphemeralTurnServer(sessionId);
        } catch (err) {
          fastify.log.error(
            { err, sessionId },
            "Failed to provision TURN server for match; requeuing both users"
          );
          const errorPayload = JSON.stringify({
            action: "MATCH_ERROR",
            message: "Failed to provision session infrastructure, retrying…",
          });
          if (socket.readyState === socket.OPEN) {
            socket.send(errorPayload);
            matchingPool.push(userId);
          }
          if (matchedPeerSocket.readyState === matchedPeerSocket.OPEN) {
            matchedPeerSocket.send(errorPayload);
            matchingPool.push(matchedPeerId);
          }
          return;
        }

        // Either side may have dropped during the provisioning round-trip —
        // don't finalize a session for a socket that's no longer open, and
        // don't leak the droplet we just paid for.
        if (matchedPeerSocket.readyState !== matchedPeerSocket.OPEN || socket.readyState !== socket.OPEN) {
          await destroyEphemeralTurnServer(provisioned.dropletId).catch((err) =>
            fastify.log.error({ err, dropletId: provisioned.dropletId }, "Failed to destroy orphaned TURN droplet")
          );
          if (socket.readyState === socket.OPEN) matchingPool.push(userId);
          if (matchedPeerSocket.readyState === matchedPeerSocket.OPEN) matchingPool.push(matchedPeerId);
          return;
        }

        activeSessions.set(sessionId, {
          userA: matchedPeerId,
          userB: userId,
          dropletId: provisioned.dropletId,
        });
        userSessionMap.set(matchedPeerId, sessionId);
        userSessionMap.set(userId, sessionId);

        const { turnUrl, turnUsername, turnCredential } = provisioned;

        socket.send(
          JSON.stringify({
            action: "MATCH_FOUND",
            peerId: matchedPeerId,
            sessionId,
            turnUrl,
            turnUsername,
            turnCredential,
          })
        );
        matchedPeerSocket.send(
          JSON.stringify({
            action: "MATCH_FOUND",
            peerId: userId,
            sessionId,
            turnUrl,
            turnUsername,
            turnCredential,
          })
        );
      } else {
        matchingPool.push(userId);
      }
    }
  );
};

export default matchingPlugin;
