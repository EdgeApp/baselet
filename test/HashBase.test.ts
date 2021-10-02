import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { describe, it } from 'mocha'

import { createHashBase, HashBase } from '../src/HashBase'
import { getBucketPath, getConfig } from '../src/helpers'
import { BaseType, HashBaseConfig } from '../src/types'

interface TestData {
  hash: string
  input: string
  output: string
}

describe('HashBase baselet', function () {
  const disklet = makeMemoryDisklet()
  let hashbaseDb: HashBase<TestData>
  const dbName = 'testHashdb'
  const prefixSize = 2
  const partitionName = 'students'
  const dataSet: TestData[] = [
    { hash: 'abcd-efgh-ijkl-mnop', input: 'btc', output: 'eth' },
    { hash: 'bytc-efgh-ijkl-mnop', input: 'ltc', output: 'eth' },
    { hash: 'abcd-hitk-ijkl-mnop', input: 'eth', output: 'bat' },
    { hash: 'zbcd-efgh-ijkl-dfop', input: 'bat', output: 'btc' },
    { hash: 'zbcd-abcd-ijkl-ddop', input: 'bat', output: 'ltc' }
  ]
  it('create hashbase', async function () {
    const expectedTest: HashBaseConfig = {
      type: BaseType.HashBase,
      prefixSize
    }
    hashbaseDb = await createHashBase(disklet, { name: dbName, prefixSize })
    expect(await getConfig(disklet, dbName)).eql(expectedTest)
  })
  it('insert data', async function () {
    for (let i = 0; i < dataSet.length; i++) {
      const data = dataSet[i]
      await hashbaseDb.insert(partitionName, data.hash, data)
    }
    const prefix = dataSet[0].hash.substring(0, prefixSize)
    const storedData = await disklet.getText(
      getBucketPath(dbName, partitionName, prefix)
    )
    // @ts-ignore
    expect(JSON.parse(storedData)[dataSet[0].hash]).eql(dataSet[0])
  })
  it('query data', async function () {
    const queriedData1 = await hashbaseDb.query(partitionName, [
      dataSet[0].hash
    ])
    const queriedData2 = await hashbaseDb.query(partitionName, [
      dataSet[1].hash,
      dataSet[2].hash
    ])
    const queriedData3 = await hashbaseDb.query(partitionName, [
      dataSet[4].hash,
      dataSet[3].hash
    ])
    expect(queriedData1).eql([dataSet[0]])
    expect(queriedData2).eql([dataSet[1], dataSet[2]])
    expect(queriedData3).eql([dataSet[4], dataSet[3]])
  })
  it('delete data', async function () {
    const dataToDelete = dataSet[dataSet.length - 1]

    const [queriedData1] = await hashbaseDb.query(partitionName, [
      dataToDelete.hash
    ])
    expect(queriedData1).to.eql(dataToDelete)

    await hashbaseDb.delete(partitionName, [dataToDelete.hash])
    const [queriedData2] = await hashbaseDb.query(partitionName, [
      dataToDelete.hash
    ])
    expect(queriedData2).eql(undefined)
  })
  it('dumpData', async () => {
    const dump = await hashbaseDb.dumpData('')

    expect(dump).keys(['config', 'data'])
    expect(dump.data).keys([partitionName])

    const dumpDataSet: { [hash: string]: TestData } = {}
    for (const data of dataSet) {
      dumpDataSet[data.hash] = data
    }

    for (const [key, value] of Object.entries(dump.data[partitionName])) {
      expect(value).to.deep.equal(dumpDataSet[key])
    }
  })
})
