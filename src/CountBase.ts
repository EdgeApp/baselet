import { Disklet } from 'disklet'

import { BaseletConfig, BaseType } from './types'

export interface CountBase {
  insert(partition: string, index: number, data: any): Promise<unknown>
  query(partition: string, range: number): Promise<any[]>
  length(): Promise<number>
}

interface CountBaseConfig extends BaseletConfig {
  bucketSize: number
  length: number
  partitions: {
    [partitionName: string]: {
      length: number
    }
  }
}

export function openCountBase(
  disklet: Disklet,
  databaseName: string
): Promise<CountBase> {
  // check that the db exists and is of type CountBase

  function getConfig(): Promise<CountBaseConfig> {
    return disklet
      .getText(`${databaseName}/config.json`)
      .then(serializedConfig => JSON.parse(serializedConfig))
  }

  return getConfig().then(configData => {
    return {
      insert(
        partition: string = '/',
        index: number,
        data: any
      ): Promise<unknown> {
        // check that partition only contains letters, numbers, and underscores
        // if no partition, then root

        let partitionMetadata = configData.partitions[partition]
        if (partitionMetadata === undefined) {
          partitionMetadata = { length: 0 }
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

        const bucketNumber = Math.floor(index / configData.bucketSize)
        const bucketIndex = index % configData.bucketSize
        const bucketExists = bucketIndex !== 0
        if (index === nextIndex) {
          ++partitionMetadata.length
        }
        if (bucketExists) {
          return disklet
            .getText(`${databaseName}/${partition}/${bucketNumber}.json`)
            .then(currentBucketDataRaw => {
              const currentBucketData = JSON.parse(currentBucketDataRaw)
              currentBucketData[bucketIndex] = data
              return disklet.setText(
                `${databaseName}/${partition}/${bucketNumber}.json`,
                JSON.stringify(currentBucketData)
              )
            })
            .then(() => {
              configData.partitions[partition] = partitionMetadata
              return disklet.setText(
                `${databaseName}/config.json`,
                JSON.stringify(configData)
              )
            })
            .then(() => null)
            .catch(error => {
              return new Error(`Could not insert data. ${error}`)
            })
        } else {
          return disklet
            .setText(
              `${databaseName}/${partition}/${bucketNumber}.json`,
              JSON.stringify([data])
            )
            .then(() => {
              configData.partitions[partition] = partitionMetadata
              return disklet.setText(
                `${databaseName}/config.json`,
                JSON.stringify(configData)
              )
            })
            .then(() => null)
            .catch(error => {
              return new Error(`Could not insert data. ${error}`)
            })
        }
      },
      query(
        partition: string = '/',
        rangeStart: number = 0,
        rangeEnd: number = rangeStart
      ): Promise<any[]> {
        // check that partition only contains letters, numbers, and underscores
        // sanity check the range
        const partitionMetadata = configData.partitions[partition]
        if (partitionMetadata === undefined) {
          return Promise.reject(
            new Error(`Partition ${partition} does not exist.`)
          )
        }
        if (partitionMetadata.length === 0) {
          return Promise.reject(new Error(`Partition ${partition} is empty.`))
        }
        const bucketFetchers = []
        for (
          let bucketNumber = Math.floor(rangeStart / configData.bucketSize);
          bucketNumber <= Math.floor(rangeEnd / configData.bucketSize);
          bucketNumber++
        ) {
          bucketFetchers.push(
            disklet
              .getText(`${databaseName}/${partition}/${bucketNumber}.json`)
              .then(rawBucketData => JSON.parse(rawBucketData))
              .catch(error => {
                console.log(
                  `Error getting data from bucket ${bucketNumber}. ${error}`
                )
              })
          )
        }
        return Promise.all(bucketFetchers)
          .then(bucketList => {
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
          .catch(error => {
            console.log(`Error fetching data. ${error}`)
            return []
          })
      },
      length(
        partition: keyof typeof configData.partitions = '/'
      ): Promise<number> {
        const partitionMetadata = configData.partitions[partition]
        if (partitionMetadata === undefined) {
          return Promise.reject(new Error('No partition found with that name'))
        }
        return Promise.resolve(partitionMetadata.length)
      }
    }
  })
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
    type: BaseType.COUNT_BASE,
    bucketSize,
    length: 0,
    partitions: {
      '/': {
        length: 0
      }
    }
  }
  return disklet
    .setText(`${databaseName}/config.json`, JSON.stringify(configData))
    .then(() => openCountBase(disklet, databaseName))
}
