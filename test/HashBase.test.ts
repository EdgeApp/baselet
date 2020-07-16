import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { describe, it } from 'mocha'

import { createHashBase, HashBase } from '../src/HashBase'
import { BaseType } from '../src/types'

describe('HashBase baselet', function () {
  const disklet = makeMemoryDisklet()
  let hashbaseDb: HashBase
  const dbName = 'testHashdb'
  const prefixSize = 2
  const partitionName = 'students'
  const dataSet = [
    { hash: 'abcd-efgh-ijkl-mnop', input: 'btc', output: 'eth' },
    { hash: 'bytc-efgh-ijkl-mnop', input: 'ltc', output: 'eth' },
    { hash: 'abcd-hitk-ijkl-mnop', input: 'eth', output: 'bat' },
    { hash: 'zbcd-efgh-ijkl-dfop', input: 'bat', output: 'btc' },
    { hash: 'zbcd-abcd-ijkl-ddop', input: 'bat', output: 'ltc' },
    { hash: 'xyxy-abcd-ijkl-ddop', input: 'bat', output: 'nexo' }
  ]
  it('create hashbase', async function () {
    const expectedTest = JSON.stringify({
      type: BaseType.HashBase,
      prefixSize
    })
    hashbaseDb = await createHashBase(disklet, dbName, prefixSize)
    expect(await disklet.getText(`${dbName}/config.json`)).equals(expectedTest)
  })
  it('insert data', async function () {
    for (let i = 0; i < dataSet.length; i++) {
      const data = dataSet[i]
      await hashbaseDb.insert(partitionName, data.hash, data)
    }
    const prefix = dataSet[0].hash.substring(0, prefixSize)
    const storedData = await disklet.getText(
      `${dbName}/${partitionName}/${prefix}.json`
    )
    expect(JSON.stringify(JSON.parse(storedData)[dataSet[0].hash])).equals(
      JSON.stringify(dataSet[0])
    )
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
    expect(JSON.stringify(queriedData1)).equals(JSON.stringify([dataSet[0]]))
    expect(JSON.stringify(queriedData2)).equals(
      JSON.stringify([dataSet[1], dataSet[2]])
    )
    expect(JSON.stringify(queriedData3)).equals(
      JSON.stringify([dataSet[4], dataSet[3]])
    )
  })
})
