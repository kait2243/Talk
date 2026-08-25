import { FastifyPluginAsync } from "fastify";
import type WebSocket from "ws";
import jwt from "jsonwebtoken";
import { prisma } from "../lib/prisma";

const JWT_SECRET = process.env.JWT_SECRET ?? "";
const MIN_TOKEN_BALANCE_TO_MATCH = 5;

const WS_CLOSE_INVALID_TOKEN = 4001;
const WS_CLOSE_USER_NOT_FOUND = 4002;
const WS_CLOSE_INSUFFICIENT_BALANCE = 4003;
const WS_CLOSE_ALREADY_CONNECTED = 4004;

interface MatchJoinJwtPayload {
  userId: string;
}

// Ordered FIFO queue of userIds currently waiting for a peer.
const matchingPool: string[] = [];

// Live socket handle for every userId currently connected to /v1/matching/join.
// Kept alongside the array queue so a match can be delivered to both peers
// and so a disconnect can be spliced out of matchingPool in O(n) worst case
// (bounded by pool size, which is small relative to total connections).
const activeConnections = new Map<string, WebSocket>();

function removeFromPool(userId: string): void {
  const idx = matchingPool.indexOf(userId);
  if (idx !== -1) {
    matchingPool.splice(idx, 1);
  }
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

      socket.on("close", () => {
        removeFromPool(userId);
        activeConnections.delete(userId);
      });

      socket.on("error", () => {
        removeFromPool(userId);
        activeConnections.delete(userId);
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
        socket.send(JSON.stringify({ action: "MATCH_FOUND", peerId }));
        peerSocket.send(JSON.stringify({ action: "MATCH_FOUND", peerId: userId }));
      } else {
        matchingPool.push(userId);
      }
    }
  );
};

export default matchingPlugin;
