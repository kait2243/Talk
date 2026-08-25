import { randomBytes } from "crypto";

const DO_API_BASE = "https://api.digitalocean.com/v2";
const DROPLET_REGION = "nyc3";
const DROPLET_SIZE = "s-1vcpu-1gb";
const DROPLET_IMAGE = "docker-20-04";
const TURN_PORT = 3478;
const TURN_REALM = "talk.internal";

const REQUEST_TIMEOUT_MS = 15_000;
const PROVISION_POLL_INTERVAL_MS = 5_000;
const PROVISION_POLL_MAX_ATTEMPTS = 24; // ~2 minutes of polling before giving up

export interface ProvisionedTurnServer {
  turnUrl: string;
  dropletId: string;
  turnUsername: string;
  turnCredential: string;
}

interface DigitalOceanNetwork {
  ip_address: string;
  type: "public" | "private";
}

interface DigitalOceanDroplet {
  id: number;
  status: string;
  networks?: { v4?: DigitalOceanNetwork[] };
}

class DigitalOceanApiError extends Error {
  constructor(message: string, public readonly status: number) {
    super(message);
    this.name = "DigitalOceanApiError";
  }
}

function getApiToken(): string {
  const token = process.env.DIGITALOCEAN_API_TOKEN;
  if (!token) {
    throw new Error("DIGITALOCEAN_API_TOKEN is not set");
  }
  return token;
}

async function doFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    const response = await fetch(`${DO_API_BASE}${path}`, {
      ...init,
      signal: controller.signal,
      headers: {
        Authorization: `Bearer ${getApiToken()}`,
        "Content-Type": "application/json",
        ...init.headers,
      },
    });

    if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new DigitalOceanApiError(
        `DigitalOcean API ${init.method ?? "GET"} ${path} failed: ${response.status} ${response.statusText} ${body}`,
        response.status
      );
    }

    if (response.status === 204) {
      return undefined as T;
    }

    return (await response.json()) as T;
  } catch (err) {
    if (err instanceof Error && err.name === "AbortError") {
      throw new Error(`DigitalOcean API request to ${path} timed out after ${REQUEST_TIMEOUT_MS}ms`);
    }
    throw err;
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Hardened Coturn bootstrap: long-term credentials (so the relay isn't an
 * anonymous open proxy) and a denied-peer-ip list covering RFC1918 / loopback
 * / link-local / carrier-NAT ranges (so the relay can't be abused to reach
 * internal/cloud-metadata addresses — the TURN-relay equivalent of an SSRF
 * guard).
 */
function buildCoturnUserData(username: string, password: string): string {
  return `#!/bin/bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y coturn

cat > /etc/turnserver.conf <<'CONF'
listening-port=${TURN_PORT}
fingerprint
lt-cred-mech
user=${username}:${password}
realm=${TURN_REALM}
no-tcp-relay
no-cli
no-multicast-peers
no-loopback-peers
stale-nonce=600
total-quota=100
denied-peer-ip=0.0.0.0-0.255.255.255
denied-peer-ip=10.0.0.0-10.255.255.255
denied-peer-ip=100.64.0.0-100.127.255.255
denied-peer-ip=127.0.0.0-127.255.255.255
denied-peer-ip=169.254.0.0-169.254.255.255
denied-peer-ip=172.16.0.0-172.31.255.255
denied-peer-ip=192.0.0.0-192.0.0.255
denied-peer-ip=192.168.0.0-192.168.255.255
denied-peer-ip=198.18.0.0-198.19.255.255
CONF

sed -i 's/^#\\?TURNSERVER_ENABLED=.*/TURNSERVER_ENABLED=1/' /etc/default/coturn || echo 'TURNSERVER_ENABLED=1' >> /etc/default/coturn

systemctl enable coturn
systemctl restart coturn
`;
}

async function createDroplet(sessionId: string, userData: string): Promise<string> {
  const { droplet } = await doFetch<{ droplet: DigitalOceanDroplet }>("/droplets", {
    method: "POST",
    body: JSON.stringify({
      name: `turn-${sessionId}`,
      region: DROPLET_REGION,
      size: DROPLET_SIZE,
      image: DROPLET_IMAGE,
      user_data: userData,
      ipv6: false,
      tags: ["ephemeral-turn"],
    }),
  });

  return String(droplet.id);
}

async function waitForPublicIp(dropletId: string): Promise<string> {
  for (let attempt = 0; attempt < PROVISION_POLL_MAX_ATTEMPTS; attempt++) {
    const { droplet } = await doFetch<{ droplet: DigitalOceanDroplet }>(`/droplets/${dropletId}`);

    const publicNetwork = droplet.networks?.v4?.find((net) => net.type === "public");
    if (droplet.status === "active" && publicNetwork) {
      return publicNetwork.ip_address;
    }

    await new Promise((resolve) => setTimeout(resolve, PROVISION_POLL_INTERVAL_MS));
  }

  throw new Error(`Droplet ${dropletId} did not become active with a public IP within the polling window`);
}

/**
 * Provisions a fresh, single-purpose DigitalOcean droplet running a
 * hardened Coturn TURN relay for one signaling session, and returns the
 * connection details once the droplet is reachable.
 */
export async function provisionEphemeralTurnServer(
  sessionId: string
): Promise<ProvisionedTurnServer> {
  const turnUsername = `session-${sessionId}`;
  const turnCredential = randomBytes(18).toString("base64url");
  const userData = buildCoturnUserData(turnUsername, turnCredential);

  let dropletId: string;
  try {
    dropletId = await createDroplet(sessionId, userData);
  } catch (err) {
    throw new Error(
      `Failed to create TURN droplet for session ${sessionId}: ${(err as Error).message}`
    );
  }

  try {
    const publicIp = await waitForPublicIp(dropletId);
    return {
      turnUrl: `turn:${publicIp}:${TURN_PORT}`,
      dropletId,
      turnUsername,
      turnCredential,
    };
  } catch (err) {
    // The droplet exists but never became reachable — don't leak the spend.
    await destroyEphemeralTurnServer(dropletId).catch(() => undefined);
    throw new Error(
      `Failed to provision TURN server for session ${sessionId}: ${(err as Error).message}`
    );
  }
}

/**
 * Destroys the ephemeral TURN droplet backing a finished session. Idempotent —
 * a 404 (already destroyed) is treated as success rather than an error.
 */
export async function destroyEphemeralTurnServer(dropletId: string): Promise<void> {
  try {
    await doFetch<void>(`/droplets/${dropletId}`, { method: "DELETE" });
  } catch (err) {
    if (err instanceof DigitalOceanApiError && err.status === 404) {
      return;
    }
    throw err;
  }
}
