import assert from 'assert';
import { RedisStream } from '../dist';
import { Redis } from 'ioredis';
import { after, before, describe, it } from 'node:test';
import { randomUUID } from 'crypto';

const colors = {
  reset: "\x1b[0m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  magenta: "\x1b[35m",
  cyan: "\x1b[36m",
  white: "\x1b[37m"
};

let streamName = randomUUID()
let groupName = randomUUID()
let redis = new Redis()
let streamSrv = new RedisStream(redis)
let initMsgCount = 100

describe('redisRank', () => {
  after(async () => {
    await redis.del(streamName)
    console.log(`${colors.green} finished running tests ${colors.reset}`)
  })

  before(async () => {
    // 提交创建队列和消费者组，并且塞入100条数据进队列
    let arr: Promise<string | null>[] = []
    for(let i = 0; i < initMsgCount; i++){
      arr.push(streamSrv.add(streamName, `key${i}`, `value${i}`))
    }
    await Promise.all(arr)
    await streamSrv.createGroup(streamName, groupName)
  })

  it("消费者组消费消息", async () => {
    // 获取pending中的数据
    let pendingData = await streamSrv.pending(streamName, groupName)
    assert(pendingData.length === 0, new Error("pending 数据数量不正确"))

    let consumerName = randomUUID()
    let data1 = await streamSrv.groupRead(streamName, groupName, consumerName)
    let data2 = await streamSrv.groupRead(streamName, groupName, consumerName)

    // 再次获取pending中的数据
    pendingData = await streamSrv.pending(streamName, groupName, 2)
    assert(pendingData.length === 2, new Error("pending 数据数量不正确"))

    assert(data1?.id && data2?.id, new Error("未获取到数据的id"))
    // 确认第二条消息
    await streamSrv.ack(streamName, groupName, data2?.id)
    pendingData = await streamSrv.pending(streamName, groupName, 2)
    assert(pendingData.length === 1 && pendingData[0].id === data1.id, new Error("ack方法确认消息未生效"))


    let groupInfo = await streamSrv.groupInfo(streamName)
    assert(groupInfo[0].find(f => f.key == 'name')?.value === groupName, new Error("组名不正确"))
    assert(groupInfo[0].find(f => f.key == 'consumers')?.value === 1, new Error("consumers 不正确"))
    assert(groupInfo[0].find(f => f.key == 'pending')?.value === 1, new Error("pending 不正确"))

  })
})
