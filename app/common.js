const crypto = require("crypto");

const decryptUserId = (payloadJson) => {
  const secretKey = process.env.ENCRYPTION_KEY || "28778ab27c641f297b7ba4705a24e281";
  if (!secretKey) {
    throw new Error("ENCRYPTION_KEY is not set.");
  }
  // Parse if string
  const payload = typeof payloadJson === "string" ? JSON.parse(payloadJson) : payloadJson;
  if (!payload || Object.keys(payload).length === 0 || !payload.iv || !payload.content) {
    return 0;
  }
  const iv = Buffer.from(payload.iv, "base64");
  const content = Buffer.from(payload.content, "base64");
  const keyBuffer = Buffer.from(secretKey, "utf8").slice(0, 32); // AES-256 key must be 32 bytes
  const decipher = crypto.createDecipheriv("aes-256-cbc", keyBuffer, iv);
  let decrypted = decipher.update(content);
  decrypted = Buffer.concat([decrypted, decipher.final()]);

  // PKCS7 padding: last byte = number of padding bytes
  const pad = decrypted[decrypted.length - 1];
  let userId;
  if (pad > 0 && pad <= 16) {
    userId = decrypted.slice(0, -pad).toString("utf8");
  } else {
    userId = decrypted.toString("utf8");
  }
  return userId;
};

module.exports = decryptUserId;
