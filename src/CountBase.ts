import { Disklet } from 'disklet'

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
import { BaseletConfig, BaseType, DataDump } from './types'

interface CountBaseConfig extends BaseletConfig {
  bucketSize: number
  partitions: {
    [partitionName: string]: {
      length: number
    }
  }
}

export interface CountBase<K = any> {
  databaseName: string
  insert(partition: string, index: number, data: K): Promise<void>
  query(partition: string, rangeStart: number, rangeEnd?: number): Promise<K[]>
  length(partition: string): number
  dumpData(partition: string): Promise<DataDump<CountBaseConfig, K[]>>
}

export function openCountBase<K>(
  disklet: Disklet,
  databaseName: string
): Promise<CountBase<K>> {
  const memlet = getOrMakeMemlet(disklet)

  return getConfig<CountBaseConfig>(disklet, databaseName).then(configData => {
    if (configData.type !== BaseType.CountBase) {
      throw new Error(`Tried to open CountBase, but type is ${configData.type}`)
    }

    const fns: CountBase<K> = {
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
        await memlet
          .getJson(bucketPath)
          .then(
            currentBucketData => currentBucketData,
            // Assume no bucket exists
            () => []
          )
          .then(existingData => {
            const bucketIndex = index % configData.bucketSize
            existingData[bucketIndex] = data
            return memlet.setJson(bucketPath, existingData)
          })
          .then(() => {
            if (metadataChanged) {
              configData.partitions[formattedPartition] = partitionMetadata
            }
            return setConfig(disklet, databaseName, configData)
          })
          .catch(error => {
            throw new Error(`Could not insert data. ${error}`)
          })
      },
      query(
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
          const bucketPath = getBucketPath(
            databaseName,
            partition,
            bucketNumber
          )
          bucketFetchers.push(memlet.getJson(bucketPath).catch(() => []))
        }
        return Promise.all(bucketFetchers).then(bucketList => {
          const queryResults: K[] = []
          for (let i = rangeStart; i <= rangeEnd; i++) {
            const bucketNumber = Math.floor(i / configData.bucketSize)
            const dataIndex = i % configData.bucketSize
            const fetchedBucketNumber =
              bucketNumber - Math.floor(rangeStart / configData.bucketSize)
            queryResults.push(bucketList[fetchedBucketNumber][dataIndex])
          }
          return queryResults
        })
      },
      length(partition: string): number {
        const formattedPartition = checkAndFormatPartition(partition)
        const partitionMetadata = configData.partitions[formattedPartition]
        return partitionMetadata?.length ?? 0
      },
      dumpData(partition: string): Promise<DataDump<CountBaseConfig, K[]>> {
        return fns.query(partition, 0, fns.length(partition) - 1).then(data => {
          return {
            config: configData,
            data
          }
        })
      }
    }

    return fns
  })
}

export function createCountBase<K>(
  disklet: Disklet,
  databaseName: string,
  bucketSize: number
): Promise<CountBase<K>> {
  if (!isPositiveInteger(bucketSize)) {
    throw new Error(`bucketSize must be a number greater than 0`)
  }

  databaseName = checkDatabaseName(databaseName)
  return doesDatabaseExist(disklet, databaseName).then(databaseExists => {
    if (databaseExists) {
      throw new Error(`database ${databaseName} already exists`)
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
    return setConfig(disklet, databaseName, configData).then(() =>
      openCountBase(disklet, databaseName)
    )
  })
}
