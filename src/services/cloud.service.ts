/**
 * Mock cloud proxy layer. Stands in for the real provisioning call (e.g. an
 * ephemeral coturn/EC2 instance spun up per session) so the signaling flow
 * can be built and tested against a stable interface ahead of that
 * infrastructure work.
 */
export async function provisionEphemeralTurnServer(
  sessionId: string
): Promise<{ turnUrl: string }> {
  await new Promise((resolve) => setTimeout(resolve, 1000));

  return {
    turnUrl: `turn:mock-instance-${sessionId}.yourdomain.com:3478`,
  };
}
