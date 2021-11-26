import { Disklet } from 'disklet'
import { Memlet } from 'memlet'

import {
  checkAndFormatPartition,
  checkDatabaseName,
  doesDatabaseExist,
  getBucketPath,
  getConfig,
  getOrMakeMemlet,
  isPositiveInteger,
  setConfig
} from './helpers'
import { BaseType, CountBaseConfig, CountBaseOptions, DataDump } from './types'

export interface CountBase<K> {
  databaseName: string
  insert(partition: string, index: number, data: K): Promise<void>
  query(partition: string, rangeStart: number, rangeEnd?: number): Promise<K[]>
  length(partition: string): number
  dumpData(partition: string): Promise<DataDump<CountBaseConfig, K[]>>
}

export async function openCountBase<K>(
  storage: Disklet | Memlet,
  databaseName: string
): Promise<CountBase<K>> {
  const memlet = getOrMakeMemlet(storage)

  const configData = await getConfig<CountBaseConfig>(memlet, databaseName)
  if (configData.type !== BaseType.CountBase) {
    throw new Error(`Tried to open CountBase, but type is ${configData.type}`)
  }

  const out: CountBase<K> = {
    databaseName,

    async insert(partition: string, index: number, data: K): Promise<void> {
      const formattedPartition = checkAndFormatPartition(partition)
      let metadataChanged = false
      let partitionMetadata = configData.partitions[formattedPartition]
      if (partitionMetadata === undefined) {
        partitionMetadata = { length: 0 }
        metadataChanged = true
      }
      const nextIndex = partitionMetadata.length

      if (Number.isNaN(index) || index < 0) {
        return Promise.reject(
          new Error('index must be a Number greater than 0')
        )
      }
      if (index > nextIndex) {
        return Promise.reject(
          new Error('index is larger than next index in partition')
        )
      }

      if (index === nextIndex) {
        ++partitionMetadata.length
        metadataChanged = true
      }

      const bucketNumber = Math.floor(index / configData.bucketSize)
      const bucketPath = getBucketPath(databaseName, partition, bucketNumber)
      const existingData = await memlet.getJson(bucketPath).catch(() => [])
      const bucketIndex = index % configData.bucketSize

      // Update bucket data
      existingData[bucketIndex] = data
      await memlet.setJson(bucketPath, existingData)

      // Update partition metadata
      if (metadataChanged) {
        configData.partitions[formattedPartition] = partitionMetadata
      }
      await setConfig(memlet, databaseName, configData)
    },

    async query(
      partition: string,
      rangeStart: number = 0,
      rangeEnd: number = rangeStart
    ): Promise<K[]> {
      // sanity check the range
      const bucketFetchers = []
      for (
        let bucketNumber = Math.floor(rangeStart / configData.bucketSize);
        bucketNumber <= Math.floor(rangeEnd / configData.bucketSize);
        bucketNumber++
      ) {
        const bucketPath = getBucketPath(databaseName, partition, bucketNumber)
        bucketFetchers.push(memlet.getJson(bucketPath).catch(() => []))
      }
      const bucketList = await Promise.all(bucketFetchers)
      const queryResults: K[] = []
      for (let i = rangeStart; i <= rangeEnd; i++) {
        const bucketNumber = Math.floor(i / configData.bucketSize)
        const dataIndex = i % configData.bucketSize
        const fetchedBucketNumber =
          bucketNumber - Math.floor(rangeStart / configData.bucketSize)
        queryResults.push(bucketList[fetchedBucketNumber][dataIndex])
      }
      return queryResults
    },

    length(partition: string): number {
      const formattedPartition = checkAndFormatPartition(partition)
      const partitionMetadata = configData.partitions[formattedPartition]
      return partitionMetadata?.length ?? 0
    },

    async dumpData(partition: string): Promise<DataDump<CountBaseConfig, K[]>> {
      const data = await out.query(partition, 0, out.length(partition) - 1)
      return {
        config: configData,
        data
      }
    }
  }

  return out
}

export async function createCountBase<K>(
  storage: Disklet | Memlet,
  options: CountBaseOptions
): Promise<CountBase<K>> {
  const memlet = getOrMakeMemlet(storage)

  const { name: databaseName, bucketSize } = options
  if (!isPositiveInteger(bucketSize)) {
    throw new Error(`bucketSize must be a number greater than 0`)
  }

  const dbName = checkDatabaseName(databaseName)
  const databaseExists = await doesDatabaseExist(memlet, dbName)
  if (databaseExists) {
    throw new Error(`database ${dbName} already exists`)
  }

  const configData: CountBaseConfig = {
    type: BaseType.CountBase,
    bucketSize: Math.floor(bucketSize),
    partitions: {
      '': {
        length: 0
      }
    }
  }
  await setConfig(memlet, dbName, configData)

  return openCountBase(memlet, dbName)
}

export async function createOrOpenCountBase<K>(
  disklet: Disklet,
  options: CountBaseOptions
): Promise<CountBase<K>> {
  try {
    return await createCountBase(disklet, options)
  } catch (err) {
    if (err instanceof Error && !err.message.includes('already exists')) {
      throw err
    }
    return openCountBase(disklet, options.name)
  }
}
