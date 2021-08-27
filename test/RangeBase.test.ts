import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { describe, it } from 'mocha'

import { getBucketPath, getConfig } from '../src/helpers'
import { createRangeBase, RangeBase, RangeData } from '../src/RangeBase'
import { BaseType } from '../src/types'

const rangeKey = 'createdAt'
const idKey = 'id'

type TestData = RangeData<
  Partial<{
    input: string
    output: string
  }>,
  typeof rangeKey,
  typeof idKey
>
type TestRangeBase = RangeBase<TestData, typeof rangeKey, typeof idKey>

describe('RangeBase baselet', function () {
  const disklet = makeMemoryDisklet()
  let rangebaseDb: TestRangeBase
  const dbName = 'testRangedb'
  const bucketSize = 2000000
  const idPrefixLength = 4
  const partitionName = 'transactions'
  const testData: TestData[] = [
    {
      [rangeKey]: 1594734520,
      [idKey]: 'abcd-efgh-ijkl-mnop',
      input: 'btc',
      output: 'eth'
    },
    {
      [rangeKey]: 1594734520,
      [idKey]: 'bytc-efgh-ijkl-mnop',
      input: 'ltc',
      output: 'eth'
    },
    {
      [rangeKey]: 1594484520,
      [idKey]: 'abcd-hitk-ijkl-mnop',
      input: 'eth',
      output: 'bat'
    },
    {
      [rangeKey]: 1579073917,
      [idKey]: 'zbcd-efgh-ijkl-dfop',
      input: 'bat',
      output: 'btc'
    },
    {
      [rangeKey]: 1594734520,
      [idKey]: 'zbcd-abcd-ijkl-ddop',
      input: 'bat',
      output: 'ltc'
    },
    {
      [rangeKey]: 1594736520,
      [idKey]: 'xyxy-abcd-ijkl-ddop',
      input: 'bat',
      output: 'nexo'
    },
    {
      [rangeKey]: 1234,
      [idKey]: 'zzzz-abcd-ijkl-ddop',
      input: 'bat',
      output: 'nexo'
    }
  ]
  it('create rangebase', async function () {
    const expectedTest = {
      type: BaseType.RangeBase,
      bucketSize,
      rangeKey,
      idKey,
      idPrefixLength,
      limits: {},
      sizes: {}
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
    await rangebaseDb.insert(partitionName, testData[0])
    await rangebaseDb.insert(partitionName, testData[1])
    await rangebaseDb.insert(partitionName, testData[2])
    await rangebaseDb.insert(partitionName, testData[3])
    await rangebaseDb.insert(partitionName, testData[4])
    await rangebaseDb.insert(partitionName, testData[5])
    await rangebaseDb.insert(partitionName, testData[6])

    const bucket = Math.floor(testData[0][rangeKey] / bucketSize)
    const storedBucket = await disklet.getText(
      getBucketPath(dbName, partitionName, bucket)
    )
    const storedData = JSON.parse(storedBucket).find(
      (item: TestData) => item[idKey] === testData[0][idKey]
    )
    expect(storedData).eql(testData[0])
    expect(rangebaseDb.size(partitionName)).equal(7)
  })
  it('duplicate data', async function () {
    let error
    try {
      await rangebaseDb.insert(partitionName, testData[0])
    } catch (err) {
      error = err
    }

    expect(error.message).eql('Cannot insert data because id already exists')
  })
  it('query data', async function () {
    const data1 = await rangebaseDb.query(partitionName, testData[3][rangeKey])
    expect(data1.length).eql(1)
    expect(data1[0]).eql(testData[3])
    const data2 = await rangebaseDb.query(
      partitionName,
      testData[2][rangeKey],
      testData[5][rangeKey]
    )
    expect(data2.length).eql(5)
    expect(data2[0]).eql(testData[2])
  })
  it('query data by id', async function () {
    const data5 = await rangebaseDb.queryById(
      partitionName,
      testData[4][rangeKey],
      testData[4][idKey]
    )
    expect(data5).to.eql(testData[4])
  })
  it('delete data', async function () {
    const dataToDelete = testData[3]
    await rangebaseDb.delete(
      partitionName,
      dataToDelete[rangeKey],
      dataToDelete[idKey]
    )
    const queriedData = await rangebaseDb.queryById(
      partitionName,
      dataToDelete[rangeKey],
      dataToDelete[idKey]
    )
    expect(queriedData).equal(undefined)
    expect(rangebaseDb.size(partitionName)).equal(6)
  })
  it('move data', async function () {
    const moveToRange = 123456789
    const dataToMove = testData[6]
    const newData = {
      ...dataToMove,
      [rangeKey]: moveToRange
    }
    await rangebaseDb.update(partitionName, dataToMove[rangeKey], newData)

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
  it('dumpData', async () => {
    const dump = await rangebaseDb.dumpData(partitionName)
    expect(dump).keys(['config', 'data'])
    expect(dump.data.length).is.lessThan(testData.length)
  })
})

describe('RangeBase min/max limits', function () {
  const disklet = makeMemoryDisklet()
  let rangebaseDb: TestRangeBase
  const dbName = 'testRangedb'
  const bucketSize = 3
  const rangeKey = 'createdAt'
  const idKey = 'id'
  const idPrefixLength = 4
  const partitionName = 'transactions'
  const testData: TestData[] = [
    {
      [rangeKey]: 0,
      [idKey]: 'abcd-efgh-ijkl-mnop',
      input: 'btc',
      output: 'eth'
    },
    {
      [rangeKey]: 2,
      [idKey]: 'bytc-efgh-ijkl-mnop',
      input: 'ltc',
      output: 'eth'
    },
    {
      [rangeKey]: 4,
      [idKey]: 'abcd-hitk-ijkl-mnop',
      input: 'eth',
      output: 'bat'
    },
    {
      [rangeKey]: 5,
      [idKey]: 'zbcd-efgh-ijkl-dfop',
      input: 'bat',
      output: 'btc'
    }
  ]

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

  it('should have 0 min and max values', function () {
    testMinMax(0, 0)
  })

  describe('inserting data', function () {
    it('should be able to calculate the max range value', async function () {
      await rangebaseDb.insert(partitionName, testData[1])
      testMinMax(testData[1][rangeKey], testData[1][rangeKey])

      await rangebaseDb.insert(partitionName, testData[0])
      testMinMax(testData[0][rangeKey], testData[1][rangeKey])

      await rangebaseDb.insert(partitionName, testData[3])
      testMinMax(testData[0][rangeKey], testData[3][rangeKey])

      await rangebaseDb.insert(partitionName, testData[2])
      testMinMax(testData[0][rangeKey], testData[3][rangeKey])
    })
  })

  describe('deleting data', function () {
    it('should be able to calculate the max range value', async function () {
      await rangebaseDb.delete(
        partitionName,
        testData[0][rangeKey],
        testData[0][idKey]
      )
      testMinMax(testData[1][rangeKey], testData[3][rangeKey])

      await rangebaseDb.delete(
        partitionName,
        testData[3][rangeKey],
        testData[3][idKey]
      )
      testMinMax(testData[1][rangeKey], testData[2][rangeKey])

      await rangebaseDb.delete(
        partitionName,
        testData[2][rangeKey],
        testData[2][idKey]
      )
      testMinMax(testData[1][rangeKey], testData[1][rangeKey])

      await rangebaseDb.delete(
        partitionName,
        testData[1][rangeKey],
        testData[1][idKey]
      )
      testMinMax(0, 0)
    })
  })
})

describe('RangeBase baselet findById', function () {
  const disklet = makeMemoryDisklet()
  let rangebaseDb: TestRangeBase
  const dbName = 'testRangedb'
  const bucketSize = 2000000
  const rangeKey = 'createdAt'
  const idKey = 'id'
  const idPrefixLength = 4
  const partitionName = 'transactions'

  const testData: TestData[] = [
    {
      [rangeKey]: 1111111111111,
      [idKey]: '111'
    },
    {
      [rangeKey]: 1111111111111,
      [idKey]: '222'
    },
    {
      [rangeKey]: 1111111111111,
      [idKey]: '333'
    },
    {
      [rangeKey]: 1111111111111,
      [idKey]: '444'
    },
    {
      [rangeKey]: 1111111111111,
      [idKey]: '555'
    }
  ]

  before(async () => {
    rangebaseDb = await createRangeBase(
      disklet,
      dbName,
      bucketSize,
      rangeKey,
      idKey,
      idPrefixLength
    )

    await rangebaseDb.insert(partitionName, testData[0])
    await rangebaseDb.insert(partitionName, testData[1])
    await rangebaseDb.insert(partitionName, testData[2])
    await rangebaseDb.insert(partitionName, testData[3])
    await rangebaseDb.insert(partitionName, testData[4])
  })

  it('should be able to binary search for an element with the same range but different ids', async function () {
    const ele1 = await rangebaseDb.queryById(
      partitionName,
      testData[0][rangeKey],
      testData[0][idKey]
    )
    expect(ele1).to.eql(testData[0])
    const ele2 = await rangebaseDb.queryById(
      partitionName,
      testData[1][rangeKey],
      testData[1][idKey]
    )
    expect(ele2).to.eql(testData[1])
    const ele3 = await rangebaseDb.queryById(
      partitionName,
      testData[2][rangeKey],
      testData[2][idKey]
    )
    expect(ele3).to.eql(testData[2])
    const ele4 = await rangebaseDb.queryById(
      partitionName,
      testData[3][rangeKey],
      testData[3][idKey]
    )
    expect(ele4).to.eql(testData[3])
    const ele5 = await rangebaseDb.queryById(
      partitionName,
      testData[4][rangeKey],
      testData[4][idKey]
    )
    expect(ele5).to.eql(testData[4])
  })
})

describe('RangeBase baselet queryByCount', function () {
  const disklet = makeMemoryDisklet()
  let rangebaseDb: TestRangeBase
  const dbName = 'testRangedb'
  const bucketSize = 2
  const rangeKey = 'createdAt'
  const idKey = 'id'
  const idPrefixLength = 4
  const partitionName = 'transactions'

  const testData: TestData[] = [
    {
      [rangeKey]: 1,
      [idKey]: '111'
    },
    {
      [rangeKey]: 2,
      [idKey]: '222'
    },
    {
      [rangeKey]: 3,
      [idKey]: '333'
    },
    {
      [rangeKey]: 4,
      [idKey]: '444'
    },
    {
      [rangeKey]: 5,
      [idKey]: '555'
    },
    {
      [rangeKey]: 6,
      [idKey]: '666'
    },
    {
      [rangeKey]: 7,
      [idKey]: '777'
    },
    {
      [rangeKey]: 8,
      [idKey]: '888'
    }
  ]

  before(async () => {
    rangebaseDb = await createRangeBase(
      disklet,
      dbName,
      bucketSize,
      rangeKey,
      idKey,
      idPrefixLength
    )

    await rangebaseDb.insert(partitionName, testData[0])
    await rangebaseDb.insert(partitionName, testData[1])
    await rangebaseDb.insert(partitionName, testData[2])
    await rangebaseDb.insert(partitionName, testData[3])
    await rangebaseDb.insert(partitionName, testData[4])
    await rangebaseDb.insert(partitionName, testData[5])
    await rangebaseDb.insert(partitionName, testData[6])
    await rangebaseDb.insert(partitionName, testData[7])
  })

  it('should be able to query items by count and offset', async function () {
    const items1 = await rangebaseDb.queryByCount(partitionName, 3, 0)
    expect(items1.length).to.equal(3)
    expect(items1).to.eql([testData[7], testData[6], testData[5]])

    const items2 = await rangebaseDb.queryByCount(partitionName, 3, 1)
    expect(items2.length).to.equal(3)
    expect(items2).to.eql([testData[6], testData[5], testData[4]])

    const items3 = await rangebaseDb.queryByCount(partitionName, 5, 3)
    expect(items3.length).to.equal(5)
    expect(items3).to.eql([
      testData[4],
      testData[3],
      testData[2],
      testData[1],
      testData[0]
    ])

    const items4 = await rangebaseDb.queryByCount(partitionName, 10, 2)
    expect(items4.length).to.equal(6)
    expect(items4).to.eql([
      testData[5],
      testData[4],
      testData[3],
      testData[2],
      testData[1],
      testData[0]
    ])
  })
})
