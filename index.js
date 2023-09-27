const { StreamChat } = require('stream-chat');

const { argv } = require('yargs/yargs')(process.argv.slice(2))
  .usage('Usage: $0 <command> [options]')
  .default('connectionDelay', 100)
  .describe('connectionDelay', 'ms to wait before launching a user connection')
  .default('userConnectionsMax', 99)
  .describe('userConnectionsMax', 'how many users to connect the channel')
  .default('userLifetime', 3500)
  .describe('userConnectionsMax', 'how long (ms) to keep the user connected before leaving')
  .default('coolDown', 5000)
  .describe('userConnectionsMax', 'how long (ms) to wait before one full run')
  .default('userIDPrefix', 'tommaso-')
  .default('messagesPerMinute', 20)
  .describe('userConnectionsMax', 'how many messages to send per minute')
  .demandOption(['apiKey', 'apiSecret', 'channelType', 'channelID'])
  .help('h');

const {
  apiKey, apiSecret, channelType, channelID, connectionDelay, userConnectionsMax,
  userLifetime, coolDown, userIDPrefix, messagesPerMinute,
} = argv;
const serverSideClient = new StreamChat(apiKey, apiSecret);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
async function clientLoop(i) {
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
  const messageNewHandler = channel.on('message.new', () => {
    if (shouldMarkRead) {
      shouldMarkRead = false;
      setTimeout(() => { shouldMarkRead = true; }, 666);
      return channel.markRead();
    }
    return null;
  });
  await sleep(userLifetime);
  channel.off(messageNewHandler);

  // wait in case we have an in-flight mark read request
  await markReadPromise;
  await channel.stopWatching();
  await client.disconnectUser();
}

async function clientSetup(i) {
  const user = {
    id: `${userIDPrefix}${i}`,
  };
  const token = serverSideClient.createToken(user.id);
  const client = new StreamChat(apiKey, { allowServerSideConnect: true });
  await client.connectUser(user, token);
  await client.disconnectUser();
  return user.id;
}

(async () => {
  console.log('Adding users to channel as members');
  const channel = serverSideClient.channel(channelType, channelID);
  const userPromises = [];
  for (let i = 0; i < userConnectionsMax; i += 1) {
    await sleep(connectionDelay / 10);
    userPromises.push(clientSetup(i));
  }
  const userIDs = await Promise.all(userPromises);
  await channel.addMembers(userIDs);

  let msgNum = 1;
  setInterval(async () => {
    const userID = `${userIDPrefix}${userConnectionsMax - 1}`;
    await channel.sendEvent({ type: 'typing.start', user_id: userID });
    await sleep(120);
    const p1 = channel.sendEvent({ type: 'typing.stop', user_id: userID });
    const p2 = channel.sendMessage({ text: `msg: ${msgNum}`, user_id: userID });
    await Promise.all([p1, p2]);
    msgNum += 1;
  }, (messagesPerMinute / 60) * 1000);

  console.log('Running load loop');
  while (true) {
    const promises = [];
    for (let i = 0; i < userConnectionsMax; i += 1) {
      await sleep(connectionDelay);
      promises.push(clientLoop(i));
    }
    await Promise.all(promises);
    console.log(`completed one run, wait ${coolDown}ms before doing another run bit now`);
    await sleep(coolDown);
  }
})();
