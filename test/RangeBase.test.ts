import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { describe, it } from 'mocha'

import { getBucketPath, getConfig } from '../src/helpers'
import { createRangeBase, RangeBase, RangeBaseData } from '../src/RangeBase'
import { BaseType } from '../src/types'

describe('RangeBase baselet', function () {
  const disklet = makeMemoryDisklet()
  let rangebaseDb: RangeBase
  const dbName = 'testRangedb'
  const bucketSize = 2000000
  const idDatabaseName = `${dbName}_ids`
  const rangeKey = 'createdAt'
  const idKey = 'id'
  const idPrefixLength = 4
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
  const testData7: RangeBaseData = {
    [rangeKey]: 1234,
    [idKey]: 'zzzz-abcd-ijkl-ddop',
    input: 'bat',
    output: 'nexo'
  }
  it('create rangebase', async function () {
    const expectedTest = {
      type: BaseType.RangeBase,
      bucketSize,
      idDatabaseName,
      rangeKey,
      idKey,
      idPrefixLength,
      limits: {}
    }
    rangebaseDb = await createRangeBase(
      disklet,
      dbName,
      bucketSize,
      rangeKey,
      idKey,
      idPrefixLength
    )
    const config = await getConfig(disklet, dbName)
    expect(config).to.eql(expectedTest)
  })
  it('empty array', async () => {
    const data = await rangebaseDb.query(partitionName, 0)
    expect(data.length).to.eq(0)
  })
  it('insert data', async function () {
    await rangebaseDb.insert(partitionName, testData1)
    await rangebaseDb.insert(partitionName, testData2)
    await rangebaseDb.insert(partitionName, testData3)
    await rangebaseDb.insert(partitionName, testData4)
    await rangebaseDb.insert(partitionName, testData5)
    await rangebaseDb.insert(partitionName, testData6)
    await rangebaseDb.insert(partitionName, testData7)

    const bucket = Math.floor(testData1[rangeKey] / bucketSize)
    const storedBucket = await disklet.getText(
      getBucketPath(dbName, partitionName, bucket)
    )
    const storedData = JSON.parse(storedBucket).find(
      (item: RangeBaseData) => item[idKey] === testData1[idKey]
    )
    expect(storedData).eql(testData1)
  })
  it('duplicate data', async function () {
    let error
    try {
      await rangebaseDb.insert(partitionName, testData1)
    } catch (err) {
      error = err
    }

    expect(error.message).eql('Cannot insert data because id already exists')
  })
  it('query data', async function () {
    const data1 = await rangebaseDb.query(partitionName, testData4[rangeKey])
    expect(data1.length).eql(1)
    expect(data1[0]).eql(testData4)
    const data2 = await rangebaseDb.query(
      partitionName,
      testData3[rangeKey],
      testData6[rangeKey]
    )
    expect(data2.length).eql(5)
    expect(data2[0]).eql(testData3)
  })
  it('query data by id', async function () {
    const data5 = await rangebaseDb.queryById(partitionName, testData5[idKey])
    expect(data5).to.eql(testData5)
  })
  it('delete data', async function () {
    const dataToDelete = testData4
    await rangebaseDb.delete(partitionName, dataToDelete[idKey])
    const queriedData = await rangebaseDb.queryById(
      partitionName,
      dataToDelete[idKey]
    )
    expect(queriedData).equal(undefined)
  })
  it('move data', async function () {
    const moveToRange = 123456789
    const dataToMove = testData7
    const newData = {
      ...dataToMove,
      [rangeKey]: moveToRange
    }
    await rangebaseDb.move(partitionName, newData)

    const queriedOldRangeData = await rangebaseDb.query(
      partitionName,
      dataToMove[rangeKey]
    )
    const oldDataFromQuery = queriedOldRangeData.find(
      data => data[idKey] === dataToMove[idKey]
    )
    expect(oldDataFromQuery).equal(undefined)

    const queriedNewRangeData = await rangebaseDb.query(
      partitionName,
      moveToRange
    )
    const newDataFromQuery = queriedNewRangeData.find(
      data => data[idKey] === dataToMove[idKey]
    )
    expect(newDataFromQuery).eql(newData)
  })
})

describe('RangeBase min/max limits', function () {
  const disklet = makeMemoryDisklet()
  let rangebaseDb: RangeBase
  const dbName = 'testRangedb'
  const bucketSize = 3
  const rangeKey = 'createdAt'
  const idKey = 'id'
  const idPrefixLength = 4
  const partitionName = 'transactions'
  const testData1: RangeBaseData = {
    [rangeKey]: 0,
    [idKey]: 'abcd-efgh-ijkl-mnop',
    input: 'btc',
    output: 'eth'
  }
  const testData2: RangeBaseData = {
    [rangeKey]: 2,
    [idKey]: 'bytc-efgh-ijkl-mnop',
    input: 'ltc',
    output: 'eth'
  }
  const testData3: RangeBaseData = {
    [rangeKey]: 4,
    [idKey]: 'abcd-hitk-ijkl-mnop',
    input: 'eth',
    output: 'bat'
  }
  const testData4: RangeBaseData = {
    [rangeKey]: 5,
    [idKey]: 'zbcd-efgh-ijkl-dfop',
    input: 'bat',
    output: 'btc'
  }

  before('setup', async function () {
    rangebaseDb = await createRangeBase(
      disklet,
      dbName,
      bucketSize,
      rangeKey,
      idKey,
      idPrefixLength
    )
  })

  function testMinMax(min: any, max: any): void {
    expect(rangebaseDb.min(partitionName)).equal(min)
    expect(rangebaseDb.max(partitionName)).equal(max)
  }

  it('should have undefined min and max values', function () {
    testMinMax(undefined, undefined)
  })

  describe('inserting data', function () {
    it('should be able to calculate the max range value', async function () {
      await rangebaseDb.insert(partitionName, testData2)
      testMinMax(testData2[rangeKey], testData2[rangeKey])

      await rangebaseDb.insert(partitionName, testData1)
      testMinMax(testData1[rangeKey], testData2[rangeKey])

      await rangebaseDb.insert(partitionName, testData4)
      testMinMax(testData1[rangeKey], testData4[rangeKey])

      await rangebaseDb.insert(partitionName, testData3)
      testMinMax(testData1[rangeKey], testData4[rangeKey])
    })
  })

  describe('deleting data', function () {
    it('should be able to calculate the max range value', async function () {
      await rangebaseDb.delete(partitionName, testData1[idKey])
      testMinMax(testData2[rangeKey], testData4[rangeKey])

      await rangebaseDb.delete(partitionName, testData4[idKey])
      testMinMax(testData2[rangeKey], testData3[rangeKey])

      await rangebaseDb.delete(partitionName, testData3[idKey])
      testMinMax(testData2[rangeKey], testData2[rangeKey])

      await rangebaseDb.delete(partitionName, testData2[idKey])
      testMinMax(undefined, undefined)
    })
  })
})
