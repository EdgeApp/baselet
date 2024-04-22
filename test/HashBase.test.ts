import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { makeMemlet } from 'memlet'
import { describe, it } from 'mocha'

import { createHashBase, HashBase } from '../src/HashBase'
import { getBucketPath, getConfig } from '../src/helpers'
import { BaseType, HashBaseConfig } from '../src/types'

interface TestData {
  hash: string
  name: string
}
interface TestDataFixture {
  [partitionName: string]: TestData[]
}

describe('HashBase baselet', function () {
  const memlet = makeMemlet(makeMemoryDisklet())
  let hashbaseDb: HashBase<TestData>
  const dbName = 'testHashdb'
  const prefixSize = 2
  const testDataFixture: TestDataFixture = {
    students: [
      { hash: 'abcd-efgh-ijkl-mnop', name: 'bob' },
      { hash: 'bytc-efgh-ijkl-mnop', name: 'larry' },
      { hash: 'abcd-hitk-ijkl-mnop', name: 'ethan' },
      { hash: 'zbcd-efgh-ijkl-dfop', name: 'bateman' },
      { hash: 'zbcd-abcd-ijkl-ddop', name: 'batman' }
    ],
    teachers: [
      { hash: 'abcd-efgh-ijkl-mnop', name: 'mr. bob' },
      { hash: 'bytc-efgh-ijkl-mnop', name: 'mr. larry' },
      { hash: 'abcd-hitk-ijkl-mnop', name: 'mr. ethan' },
      { hash: 'zbcd-efgh-ijkl-dfop', name: 'mr. bateman' },
      { hash: 'zbcd-abcd-ijkl-ddop', name: 'batman' }
    ]
  }

  it('create hashbase', async function () {
    const expectedTest: HashBaseConfig = {
      type: BaseType.HashBase,
      prefixSize
    }

    hashbaseDb = await createHashBase(memlet, { name: dbName, prefixSize })
    expect(await getConfig(memlet, dbName)).eql(expectedTest)
  })
  it('insert data', async function () {
    for (const partitionName of Object.keys(testDataFixture)) {
      const testData = testDataFixture[partitionName]

      for (let i = 0; i < testData.length; i++) {
        const data = testData[i]
        await hashbaseDb.insert(partitionName, data.hash, data)
      }
      const prefix = testData[0].hash.substring(0, prefixSize)
      const storedData = await memlet.getJson(
        getBucketPath(dbName, partitionName, prefix)
      )
      expect(storedData[testData[0].hash]).eql(testData[0])
    }
  })
  it('query data', async function () {
    for (const partitionName of Object.keys(testDataFixture)) {
      const testData = testDataFixture[partitionName]

      const queriedData1 = await hashbaseDb.query(partitionName, [
        testData[0].hash
      ])
      const queriedData2 = await hashbaseDb.query(partitionName, [
        testData[1].hash,
        testData[2].hash
      ])
      const queriedData3 = await hashbaseDb.query(partitionName, [
        testData[4].hash,
        testData[3].hash
      ])
      expect(queriedData1).eql([testData[0]])
      expect(queriedData2).eql([testData[1], testData[2]])
      expect(queriedData3).eql([testData[4], testData[3]])
    }
  })
  it('delete data', async function () {
    for (const partitionName of Object.keys(testDataFixture)) {
      const testData = testDataFixture[partitionName]

      const dataToDelete = testData[testData.length - 1]

      const [queriedData1] = await hashbaseDb.query(partitionName, [
        dataToDelete.hash
      ])
      expect(queriedData1).to.eql(dataToDelete)

      await hashbaseDb.delete(partitionName, [dataToDelete.hash])
      const [queriedData2] = await hashbaseDb.query(partitionName, [
        dataToDelete.hash
      ])
      expect(queriedData2).eql(undefined)
    }
  })
  it('dumpData', async () => {
    for (const partitionName of Object.keys(testDataFixture)) {
      const testData = testDataFixture[partitionName]

      const dump = await hashbaseDb.dumpData(partitionName)

      expect(dump).keys(['config', 'data'])
      expect(dump.data).keys([partitionName])

      const dumpDataSet: { [hash: string]: TestData } = {}
      for (const data of testData) {
        dumpDataSet[data.hash] = data
      }

      for (const [key, value] of Object.entries(dump.data[partitionName])) {
        expect(value).to.deep.equal(dumpDataSet[key])
      }
    }
  })
  it('dumpData all', async () => {
    const dump = await hashbaseDb.dumpData()

    expect(dump).keys(['config', 'data'])
    expect(dump.data).keys(Object.keys(testDataFixture))

    for (const partitionName of Object.keys(testDataFixture)) {
      const testData = testDataFixture[partitionName]

      const dumpDataSet: { [hash: string]: TestData } = {}
      for (const data of testData) {
        dumpDataSet[data.hash] = data
      }

      for (const [key, value] of Object.entries(dump.data[partitionName])) {
        expect(value).to.deep.equal(dumpDataSet[key])
      }
    }
  })
})
