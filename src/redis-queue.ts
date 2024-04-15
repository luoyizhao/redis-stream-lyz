import { Redis, Cluster } from "ioredis";


export class RedisStream {
  public redis: Redis | Cluster
  constructor(redis: Redis | Cluster) {
    this.redis = redis
  }
  /**
   * @description: 设置已确认的id
   * @param {string} streamName stream名
   * @param {string} groupName 组名
   * @param {string} id id
   * @return {*}
   */
  async setId(streamName: string, groupName: string, id: string) {
    return await this.redis.xgroup("SETID", streamName, groupName, id);
  }

  /**
   * @description: 确认消息
   * @param {string} streamName stream名
   * @param {string} groupName 组名
   * @param {string} id 消息id
   * @return {*}
   */
  async ack(streamName: string, groupName: string, id: string) {
    return await this.redis.xack(streamName, groupName, id);
  }

  /**
   * @description: 获取pending中的数据
   * @param {string} streamName stream名
   * @param {string} groupName 组名
   * @param {number} count 获取条数
   * @param {string} start 开始id
   * @param {string} stop 结束id
   * @return {*}
   */
  async pending(streamName: string, groupName: string, count: number = 1, start: string = "-", stop: string = "+") {
    let result = await this.redis.xpending(streamName, groupName, start, stop, count)
    if(!result || !result[0]) return []
    return (result as string[][]).map((arr: string[]) => {
      return { id: arr[0], consumer: arr[1], num1: arr[2], num2: arr[3] }
    })
  }
  
  /**
   * @description: 读取stream中的元素
   * @param {string} streamName stream名
   * @param {string} id 起始id
   * @param {number} count 读取条数(默认为1)
   * @return {*}
   */
  async read(streamName: string, id: string, count: number = 1) {
    let result = await this.redis.xread("COUNT", count, "STREAMS", streamName, id)
    if(!result || !result[0]) return []
    return result[0][1].map(data => {
      let arr: {key: string, value: string}[] = []
      data[1].forEach((ele, index) => {
        if(index % 2 == 1){
          arr.push({key: data[1][index - 1], value: ele})
        }
      })
      return { id: data[0], data: arr }
    })
  }

  /**
   * @description: 添加元素到stream
   * @param {string} streamName stream名
   * @param {string} key 
   * @param {string} value
   * @param {string} id 元素id(默认自动生成自增id)
   * @return {*}
   */
  async add(streamName: string, key: string, value: string, id: string = "*") {
    return await this.redis.xadd(streamName, id, key, value)
  }
 
  /**
   * @description: 创建消费组
   * @param {string} streamName stream名
   * @param {string} groupName 组名
   * @param {string} id(默认从头开始消费消息，“$”为忽略当前队列内所有消息，从尾部开始消费)
   * @return {*}
   */
  async createGroup(streamName: string, groupName: string, id: string = "0") {
    return await this.redis.xgroup("CREATE",  streamName, groupName, id)
  }

  /**
   * @description: 消费组读取单条数据
   * @param {string} streamName stream名
   * @param {string} groupName 组名
   * @param {string} consumerName 消费者名
   * @return {*}
   */
  async groupRead(streamName: string, groupName: string, consumerName: string) {
    let result = await this.redis.xreadgroup("GROUP", groupName, consumerName, "COUNT", 1, "STREAMS", streamName, ">") as [string, [string, string[]][]][]
    if(!result || !result[0]) return null
    let data = result[0][1][0]
    let arr: {key: string, value: string}[] = []
    data[1].forEach((ele, index) => {
      if(index % 2 == 1){
        arr.push({key: data[1][index - 1], value: ele})
      }
    })
    return { id: data[0], data: arr }
  }

  
  /**
   * @description: 消费组读取多条数据
   * @param {string} streamName stream名
   * @param {string} groupName 组名
   * @param {string} consumerName 消费者名
   * @param {number} count 读取条数(默认为1)
   * @return {*}
   */
  async mulGroupRead(streamName: string, groupName: string, consumerName: string, count: number = 1) {
    let result = await this.redis.xreadgroup("GROUP", groupName, consumerName, "COUNT", count, "STREAMS", streamName, ">") as [string, [string, string[]][]][]
    if(!result || !result[0]) return []
    return result[0][1].map(data => {
      let arr: {key: string, value: string}[] = []
      data[1].forEach((ele, index) => {
        if(index % 2 == 1){
          arr.push({key: data[1][index - 1], value: ele})
        }
      })
      return { id: data[0], data: arr }
    })
  }
  
  /**
   * @description: 获取队列的消费组信息
   * @param {string} streamName 队列key
   * @return {*}
   */
  async groupInfo(streamName: string){
    let result = await this.redis.xinfo("GROUPS", streamName) as string[][]
    if(!result || !result[0]) return []
    return result.map(item => {
      let arr: {key: string, value: string | number}[] = []
      item.forEach((ele, index) => {
        if(index % 2 == 1){
          arr.push({key: item[index - 1], value: ele})
        }
      })
      return arr
    })
  }
}