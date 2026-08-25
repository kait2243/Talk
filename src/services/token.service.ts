import { Prisma, TokenTransactionType } from "@prisma/client";
import { prisma } from "../lib/prisma";

/**
 * Atomically debits `cost` tokens from a user's balance for a video session.
 *
 * Uses a pessimistic row lock (`SELECT ... FOR UPDATE`) inside an interactive
 * transaction so concurrent requests for the same userId (e.g. rapid double-click)
 * serialize on the row instead of racing on a read-modify-write of tokenBalance.
 */
export async function deductTokensForSession(
  userId: string,
  cost: number
): Promise<boolean> {
  if (!Number.isInteger(cost) || cost <= 0) {
    throw new Error("cost must be a positive integer");
  }

  try {
    return await prisma.$transaction(
      async (tx) => {
        const locked = await tx.$queryRaw<{ id: string; tokenBalance: number }[]>`
          SELECT "id", "tokenBalance"
          FROM "User"
          WHERE "id" = ${userId}
          FOR UPDATE
        `;

        const user = locked[0];
        if (!user) {
          throw new Error(`User ${userId} not found`);
        }

        if (user.tokenBalance < cost) {
          return false;
        }

        await tx.tokenTransaction.create({
          data: {
            userId,
            amount: -cost,
            type: TokenTransactionType.VIDEO_CALL_DEBIT,
          },
        });

        await tx.user.update({
          where: { id: userId },
          data: { tokenBalance: { decrement: cost } },
        });

        return true;
      },
      {
        isolationLevel: Prisma.TransactionIsolationLevel.ReadCommitted,
        maxWait: 5000,
        timeout: 10000,
      }
    );
  } catch (err) {
    if (err instanceof Error && err.message.startsWith("User ")) {
      throw err;
    }
    // Serialization / deadlock retry could be layered here if isolation level is raised.
    throw err;
  }
}

/**
 * Redeems a scanned QR code exactly once, crediting the user's balance.
 * Relies on the unique constraint on UsedQRCode.qrHash to guarantee idempotency
 * even under concurrent duplicate scans (the losing transaction fails on the
 * unique violation and is treated as an already-used code).
 */
export type RedeemQrCodeResult =
  | { success: true; tokenBalance: number }
  | { success: false; reason: "ALREADY_USED" | "USER_NOT_FOUND" };

export async function redeemQrCode(
  userId: string,
  qrHash: string,
  creditAmount: number
): Promise<RedeemQrCodeResult> {
  try {
    const tokenBalance = await prisma.$transaction(async (tx) => {
      await tx.usedQRCode.create({
        data: { qrHash, userId },
      });

      await tx.tokenTransaction.create({
        data: {
          userId,
          amount: creditAmount,
          type: TokenTransactionType.QR_REDEEM,
        },
      });

      const updated = await tx.user.update({
        where: { id: userId },
        data: { tokenBalance: { increment: creditAmount } },
      });

      return updated.tokenBalance;
    });

    return { success: true, tokenBalance };
  } catch (err) {
    if (err instanceof Prisma.PrismaClientKnownRequestError) {
      if (err.code === "P2002") {
        return { success: false, reason: "ALREADY_USED" };
      }
      // P2003: foreign key violation (userId doesn't exist).
      // P2025: related record required for the update was not found.
      if (err.code === "P2003" || err.code === "P2025") {
        return { success: false, reason: "USER_NOT_FOUND" };
      }
    }
    throw err;
  }
}
