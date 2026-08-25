import Fastify from "fastify";
import websocket from "@fastify/websocket";
import matchingPlugin from "./plugins/matching";
import { prisma } from "./lib/prisma";

const PORT = Number(process.env.PORT ?? 3000);
const HOST = process.env.HOST ?? "0.0.0.0";

async function main() {
  const fastify = Fastify({ logger: true });

  await fastify.register(websocket, {
    options: { maxPayload: 1 * 1024 * 1024 },
  });

  await fastify.register(matchingPlugin);

  fastify.get("/health", async () => ({ status: "ok" }));

  const shutdown = async (signal: string) => {
    fastify.log.info(`Received ${signal}, shutting down`);
    await fastify.close();
    await prisma.$disconnect();
    process.exit(0);
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  try {
    await fastify.listen({ port: PORT, host: HOST });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

main();
