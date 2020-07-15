import { Disklet } from 'disklet'

import { checkAndformatPartition } from './helpers'
import { BaseletConfig, BaseType } from './types'

interface CountBaseConfig extends BaseletConfig {
  bucketSize: number
  partitions: {
    [partitionName: string]: {
      length: number
    }
  }
}

export interface CountBase {
  insert(partition: string, index: number, data: any): Promise<unknown>
  query(
    partition: string,
    rangeStart: number,
    rangeEnd?: number
  ): Promise<any[]>
  length(partition: string): Promise<number>
}

export function openCountBase(
  disklet: Disklet,
  databaseName: string
): CountBase {
  // check that the db exists and is of type CountBase

  function getConfig(): Promise<CountBaseConfig> {
    return disklet
      .getText(`${databaseName}/config.json`)
      .then(serializedConfig => JSON.parse(serializedConfig))
  }

  return {
    insert(partition: string, index: number, data: any): Promise<unknown> {
      return getConfig().then(configData => {
        const formattedPartition = checkAndformatPartition(partition)
        let writeConfig = false
        let partitionMetadata = configData.partitions[formattedPartition]
        if (partitionMetadata === undefined) {
          partitionMetadata = { length: 0 }
          writeConfig = true
        }
        const nextIndex = partitionMetadata.length
        console.log(`inserting:
            formattedPartition: ${formattedPartition}
            configData: ${JSON.stringify(configData)}
            partitionMetadata: ${JSON.stringify(partitionMetadata)}
            index: ${index}
            nextIndex: ${nextIndex}
            `)

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

        const bucketNumber = Math.floor(index / configData.bucketSize)
        const bucketIndex = index % configData.bucketSize
        const bucketExists = bucketIndex !== 0
        const bucketPath = `${databaseName}${formattedPartition}/${bucketNumber}.json`
        if (index === nextIndex) {
          ++partitionMetadata.length
          writeConfig = true
        }
        if (bucketExists) {
          return disklet
            .getText(bucketPath)
            .then(currentBucketDataRaw => {
              const currentBucketData = JSON.parse(currentBucketDataRaw)
              currentBucketData[bucketIndex] = data
              return disklet.setText(
                bucketPath,
                JSON.stringify(currentBucketData)
              )
            })
            .then(() => {
              configData.partitions[formattedPartition] = partitionMetadata
              if (writeConfig === true) {
                return disklet.setText(
                  `${databaseName}/config.json`,
                  JSON.stringify(configData)
                )
              } else {
                return Promise.resolve()
              }
            })
            .catch(error => {
              throw new Error(`Could not insert data. ${error}`)
            })
        } else {
          return disklet
            .setText(bucketPath, JSON.stringify([data]))
            .then(() => {
              configData.partitions[formattedPartition] = partitionMetadata
              if (writeConfig === true) {
                return disklet.setText(
                  `${databaseName}/config.json`,
                  JSON.stringify(configData)
                )
              } else {
                return Promise.resolve()
              }
            })
            .catch(error => {
              throw new Error(`Could not insert data. ${error}`)
            })
        }
      })
    },
    query(
      partition: string,
      rangeStart: number = 0,
      rangeEnd: number = rangeStart
    ): Promise<any[]> {
      return getConfig().then(configData => {
        const formattedPartition = checkAndformatPartition(partition)
        const partitionMetadata = configData.partitions[formattedPartition]
        if (partitionMetadata === undefined) {
          return Promise.reject(
            new Error(`Partition ${formattedPartition} does not exist.`)
          )
        }
        if (partitionMetadata.length === 0) {
          return Promise.reject(
            new Error(`Partition ${formattedPartition} is empty.`)
          )
        }
        // sanity check the range
        const bucketFetchers = []
        for (
          let bucketNumber = Math.floor(rangeStart / configData.bucketSize);
          bucketNumber <= Math.floor(rangeEnd / configData.bucketSize);
          bucketNumber++
        ) {
          bucketFetchers.push(
            disklet
              .getText(
                `${databaseName}${formattedPartition}/${bucketNumber}.json`
              )
              .then(rawBucketData => JSON.parse(rawBucketData))
          )
        }
        return Promise.all(bucketFetchers).then(bucketList => {
          const queryResults: any[] = []
          for (let i = rangeStart; i <= rangeEnd; i++) {
            const bucketNumber = Math.floor(i / configData.bucketSize)
            const dataIndex = i % configData.bucketSize
            const fetchedBucketNumber =
              bucketNumber - Math.floor(rangeStart / configData.bucketSize)
            queryResults.push(bucketList[fetchedBucketNumber][dataIndex])
          }
          return queryResults
        })
      })
    },
    length(partition: string): Promise<number> {
      return getConfig().then(configData => {
        const formattedPartition = checkAndformatPartition(partition)
        const partitionMetadata = configData.partitions[formattedPartition]
        if (partitionMetadata === undefined) {
          return Promise.reject(new Error('No partition found with that name'))
        }
        return Promise.resolve(partitionMetadata.length)
      })
    }
  }
}

export function createCountBase(
  disklet: Disklet,
  databaseName: string,
  bucketSize: number
): Promise<CountBase> {
  // check that databaseName only contains letters, numbers, and underscores
  // check if database already exists
  // check that bucketSize is a positive Integer

  // create config file at databaseName/config.json
  const configData: CountBaseConfig = {
    type: BaseType.CountBase,
    bucketSize,
    partitions: {
      '': {
        length: 0
      }
    }
  }
  return disklet
    .setText(`${databaseName}/config.json`, JSON.stringify(configData))
    .then(() => openCountBase(disklet, databaseName))
}
