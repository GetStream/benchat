import { StreamChat } from "stream-chat";
import yargs from "yargs";

const argv = await yargs(process.argv.slice(2))
  .usage("Usage: $0 <command> [options]")
  .string(["apiKey", "apiSecret", "channelType", "channelID"])
  .demandOption(["apiKey", "apiSecret", "channelType", "channelID"])
  .requiresArg(["apiKey", "apiSecret", "channelType", "channelID"])
  .describe("apiKey", "Stream API key")
  .describe("apiSecret", "Stream API secret")
  .describe("channelType", "Channel type (e.g., messaging)")
  .describe("channelID", "Channel ID")
  // numeric and optional/string options
  .number([
    "connectionDelay",
    "userConnectionsMax",
    "userLifetime",
    "coolDown",
    "messagesPerMinute",
  ])
  .string(["userIDPrefix"])
  .default("connectionDelay", 100)
  .describe("connectionDelay", "ms to wait before launching a user connection")
  .default("userConnectionsMax", 99)
  .describe("userConnectionsMax", "how many users to connect the channel")
  .default("userLifetime", 6000)
  .describe(
    "userLifetime",
    "how long (ms) to keep the user connected before leaving",
  )
  .default("coolDown", 5000)
  .describe("coolDown", "how long (ms) to wait before one full run")
  .default("userIDPrefix", "tommaso-")
  .default("messagesPerMinute", 20)
  .describe("messagesPerMinute", "how many messages to send per minute")
  .default("includeAttachments", false)
  .describe("includeAttachments", "Whether to include attachments in messages")
  .default("sendReactions", false)
  .describe("sendReactions", "Whether to send reactions")
  .help("h")
  .parseAsync();

const {
  apiKey,
  apiSecret,
  channelType,
  channelID,
  connectionDelay,
  userConnectionsMax,
  userLifetime,
  coolDown,
  userIDPrefix,
  messagesPerMinute,
  includeAttachments,
  sendReactions,
} = argv;
const serverSideClient = new StreamChat(apiKey, apiSecret);

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
async function clientLoop(i: number) {
  const user = {
    id: `${userIDPrefix}${i}`,
  };
  const client = new StreamChat(apiKey, { allowServerSideConnect: true });
  const token = serverSideClient.createToken(user.id);
  await client.connectUser(user, token);

  const channel = client.channel(channelType, channelID);

  // FIXME: this benchmark makes it easier to hit a context switch race bug with our SDK
  channel.initialized = true;

  await channel.watch();
  const markReadPromise = null;
  let shouldMarkRead = true;
  const messageNewHandler = channel.on("message.new", () => {
    if (shouldMarkRead) {
      shouldMarkRead = false;
      setTimeout(() => {
        shouldMarkRead = true;
      }, 666);
      return channel.markRead();
    }
    return null;
  });
  await sleep(userLifetime);
  channel.off(messageNewHandler); // OL: should we remove this or fix it?

  // wait in case we have an in-flight mark read request
  await markReadPromise;
  await channel.stopWatching();
  await client.disconnectUser();
}

async function clientSetup(i: number) {
  const user = {
    id: `${userIDPrefix}${i}`,
  };
  const token = serverSideClient.createToken(user.id);
  const client = new StreamChat(apiKey, { allowServerSideConnect: true });
  await client.connectUser(user, token);
  await client.disconnectUser();
  return user.id;
}

function chunk<T>(arr: T[], size: number) {
  /* eslint-disable max-len */
  return Array.from({ length: Math.ceil(arr.length / size) }, (v, i) =>
    arr.slice(i * size, i * size + size),
  );
}

(async () => {
  console.log("Adding users to channel as members");
  const channel = serverSideClient.channel(channelType, channelID);

  /* eslint-disable no-restricted-syntax */
  for (const ids of chunk(Array.from(Array(userConnectionsMax).keys()), 100)) {
    const userPromises = [];
    /* eslint-disable no-restricted-syntax */
    for (const id of ids) {
      await sleep(connectionDelay / 10);
      userPromises.push(clientSetup(id));
    }
    const userIDs = await Promise.all(userPromises);
    await channel.addMembers(userIDs);
  }

  let msgNum = 1;
  setInterval(
    async () => {
      const userID = `${userIDPrefix}${userConnectionsMax - 1}`;
      await channel.sendEvent({ type: "typing.start", user_id: userID });
      await sleep(120);
      const p1 = channel.sendEvent({ type: "typing.stop", user_id: userID });
      const randomPadding = " random";
      const text = `msg: ${msgNum}${randomPadding.repeat(Math.round(Math.random() * 100))}`;
      const p2 = channel.sendMessage({
        text,
        user_id: userID,
        attachments: includeAttachments
          ? [{ type: "image", image_url: "https://picsum.photos/200" }]
          : [],
      });
      const [, message] = await Promise.all([p1, p2]);

      if (sendReactions) {
        Promise.all([
          channel.sendReaction(message.message.id, {
            type: "love",
            user_id: userID,
          }),
          channel.sendReaction(message.message.id, {
            type: "like",
            user_id: userID,
          }),
        ]);
      }

      msgNum += 1;
    },
    1000 / (messagesPerMinute / 60),
  );

  console.log("Running load loop");
  while (true) {
    const promises = [];
    for (let i = 0; i < userConnectionsMax; i += 1) {
      await sleep(connectionDelay);
      promises.push(clientLoop(i));
    }
    await Promise.all(promises);
    console.log(
      `completed one run, wait ${coolDown}ms before doing another run bit now`,
    );
    await sleep(coolDown);
  }
})();
