import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { describe, it } from 'mocha'

import { createRangeBase, RangeBase, RangeBaseData } from '../src/RangeBase'
import { BaseType } from '../src/types'

describe('RangeBase baselet', function () {
  const disklet = makeMemoryDisklet()
  let rangebaseDb: RangeBase
  const dbName = 'testdb'
  const bucketSize = 2000000
  const rangeKey = 'createdAt'
  const idKey = 'id'
  const partitionName = 'transactions'
  const testData1: RangeBaseData = {
    [rangeKey]: 1594734520,
    [idKey]: 'abcd-efgh-ijkl-mnop',
    input: 'btc',
    output: 'eth'
  }
  const testData2: RangeBaseData = {
    [rangeKey]: 1594734520,
    [idKey]: 'bytc-efgh-ijkl-mnop',
    input: 'ltc',
    output: 'eth'
  }
  const testData3: RangeBaseData = {
    [rangeKey]: 1594484520,
    [idKey]: 'abcd-hitk-ijkl-mnop',
    input: 'eth',
    output: 'bat'
  }
  const testData4: RangeBaseData = {
    [rangeKey]: 1579073917,
    [idKey]: 'zbcd-efgh-ijkl-dfop',
    input: 'bat',
    output: 'btc'
  }
  const testData5: RangeBaseData = {
    [rangeKey]: 1594734520,
    [idKey]: 'zbcd-abcd-ijkl-ddop',
    input: 'bat',
    output: 'ltc'
  }
  const testData6: RangeBaseData = {
    [rangeKey]: 1594736520,
    [idKey]: 'xyxy-abcd-ijkl-ddop',
    input: 'bat',
    output: 'nexo'
  }
  it('create rangebase', async function () {
    const expectedTest = JSON.stringify({
      type: BaseType.RangeBase,
      bucketSize,
      rangeKey,
      idKey
    })
    rangebaseDb = await createRangeBase(
      disklet,
      dbName,
      bucketSize,
      rangeKey,
      idKey
    )
    expect(await disklet.getText(`${dbName}/config.json`)).equals(expectedTest)
  })
  it('insert data', async function () {
    await rangebaseDb.insert(partitionName, testData1)
    await rangebaseDb.insert(partitionName, testData2)
    await rangebaseDb.insert(partitionName, testData3)
    await rangebaseDb.insert(partitionName, testData4)
    await rangebaseDb.insert(partitionName, testData5)
    await rangebaseDb.insert(partitionName, testData6)

    const bucket = Math.floor(testData1[rangeKey] / bucketSize)
    const storedBucket = JSON.parse(
      await disklet.getText(`${dbName}/${partitionName}/${bucket}.json`)
    )
    const storedData = storedBucket.find(
      (item: RangeBaseData) => item[idKey] === testData1[idKey]
    )
    expect(JSON.stringify(storedData)).equals(JSON.stringify(testData1))
  })
  it('query data', async function () {
    const data1 = await rangebaseDb.query(partitionName, testData4[rangeKey])
    expect(data1.length).equals(1)
    expect(JSON.stringify(data1[0])).equals(JSON.stringify(testData4))
    const data2 = await rangebaseDb.query(
      partitionName,
      testData3[rangeKey],
      testData6[rangeKey]
    )
    expect(data2.length).equals(5)
    expect(JSON.stringify(data2[0])).equals(JSON.stringify(testData3))
  })
})
