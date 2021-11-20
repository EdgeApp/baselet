import { expect } from 'chai'
import { makeMemoryDisklet } from 'disklet'
import { makeMemlet } from 'memlet'
import { describe, it } from 'mocha'

import { CountBase, createCountBase } from '../src/CountBase'
import { getBucketPath, getConfig } from '../src/helpers'
import { BaseType, CountBaseConfig } from '../src/types'

interface TestData {
  name: string
  age: string
  index: number
}

describe('CountBase baselet', function () {
  const memlet = makeMemlet(makeMemoryDisklet())
  let countbaseDb: CountBase<TestData>
  const options = {
    name: 'testCountdb',
    bucketSize: 10
  }
  const partitionName = 'users'
  const dataSet: TestData[] = [
    { name: 'jerry', age: '2', index: 0 },
    { name: 'max', age: '12', index: 1 },
    { name: 'ana', age: '26', index: 2 },
    { name: 'lucy', age: '17', index: 3 },
    { name: 'bobby', age: '9', index: 4 }
  ]

  it('create countbase', async function () {
    const expectedTest: CountBaseConfig = {
      type: BaseType.CountBase,
      bucketSize: options.bucketSize,
      partitions: {
        '': {
          length: 0
        }
      }
    }
    countbaseDb = await createCountBase(memlet, options)
    expect(await getConfig(memlet, options.name)).eql(expectedTest)
  })
  it('empty data', async function () {
    const [data] = await countbaseDb.query(partitionName, 0)
    expect(data).equal(undefined)
  })
  it('insert data', async function () {
    for (let i = 0; i < dataSet.length; i++) {
      const data = dataSet[i]
      await countbaseDb.insert(partitionName, data.index, data)
    }

    console.log()
    const storedConfig = await getConfig<any>(memlet, options.name)
    const bucketNumber = Math.floor(dataSet[0].index / options.bucketSize)
    const buckePath = getBucketPath(options.name, partitionName, bucketNumber)
    const storedData = await memlet.getJson(buckePath)
    expect(storedConfig.partitions[partitionName].length).eql(dataSet.length)
    expect(storedData[dataSet[0].index]).eql(dataSet[0])
  })
  it('query data', async function () {
    const queriedData1 = await countbaseDb.query(
      partitionName,
      0,
      dataSet.length - 1
    )
    const queriedData2 = await countbaseDb.query(partitionName, 3)
    const queriedData3 = await countbaseDb.query(partitionName, 3, 4)
    expect(queriedData1).eql(dataSet)
    expect(queriedData2).eql(dataSet.slice(3, 4))
    expect(queriedData3).eql(dataSet.slice(3, 5))
  })
  it('get length of partition', async function () {
    const partitionLength = await countbaseDb.length(partitionName)
    expect(partitionLength).eql(dataSet.length)
  })
  it('dumpData', async () => {
    const dump = await countbaseDb.dumpData(partitionName)
    expect(dump).keys(['config', 'data'])
    expect(dump.data).length(dataSet.length)
  })
})
