import { FastifyPluginAsync } from "fastify";
import { redeemQrCode } from "../services/token.service";

const QR_REDEEM_AMOUNT = 20;

interface RedeemBody {
  codeHash?: string;
  userId?: string;
}

const walletRoutes: FastifyPluginAsync = async (fastify) => {
  fastify.post<{ Body: RedeemBody }>("/v1/wallet/redeem", async (request, reply) => {
    const { codeHash, userId } = request.body ?? {};

    if (typeof codeHash !== "string" || codeHash.length === 0) {
      return reply.status(400).send({ message: "codeHash is required." });
    }
    if (typeof userId !== "string" || userId.length === 0) {
      return reply.status(400).send({ message: "userId is required." });
    }

    const result = await redeemQrCode(userId, codeHash, QR_REDEEM_AMOUNT);

    if (!result.success) {
      if (result.reason === "ALREADY_USED") {
        return reply
          .status(400)
          .send({ message: "This QR code has already been redeemed." });
      }
      return reply.status(404).send({ message: "User not found." });
    }

    return reply.status(200).send({
      tokenBalance: result.tokenBalance,
      amount: QR_REDEEM_AMOUNT,
    });
  });
};

export default walletRoutes;
