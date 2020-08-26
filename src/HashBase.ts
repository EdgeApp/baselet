import { Disklet } from 'disklet'

import {
  checkAndformatPartition,
  checkDatabaseName,
  doesDatabaseExist,
  isPositiveInteger
} from './helpers'
import { BaseletConfig, BaseType } from './types'

export interface HashBase {
  insert(partition: string, hash: string, data: any): Promise<unknown>
  query(partition: string, hashes: string[]): Promise<any[]>
  delete(partition: string, hashes: string[]): Promise<void>
}

interface HashBaseConfig extends BaseletConfig {
  prefixSize: number
}

interface BucketDictionary {
  [bucketName: string]: {
    bucketFetcher: Promise<void>
    bucketPath: string
    bucketData: {
      [hash: string]: any
    }
  }
}

export function openHashBase(
  disklet: Disklet,
  databaseName: string
): Promise<HashBase> {
  function getConfig(): Promise<HashBaseConfig> {
    return disklet.getText(`${databaseName}/config.json`).then(
      serializedConfig => JSON.parse(serializedConfig),
      error => {
        console.log(error)
        throw new Error(
          `The disklet does not have a valid database ${databaseName}`
        )
      }
    )
  }

  return getConfig().then(configData => {
    if (configData.type !== BaseType.HashBase) {
      throw new Error(`Tried to open HashBase, but type is ${configData.type}`)
    }

    return {
      insert(partition: string, hash: string, data: any): Promise<unknown> {
        const { prefixSize } = configData
        const formattedPartition = checkAndformatPartition(partition)
        if (typeof hash !== 'string' || hash.length < prefixSize) {
          return Promise.reject(
            new Error(`hash must be a string of length at least ${prefixSize}`)
          )
        }

        const prefix = hash.substring(0, prefixSize)
        const bucketFilename = `${prefix}.json`
        const bucketPath = `${databaseName}${formattedPartition}/${bucketFilename}`
        return disklet.getText(bucketPath).then(
          serializedBucket => {
            const bucketData = JSON.parse(serializedBucket)
            bucketData[hash] = data
            return disklet.setText(bucketPath, JSON.stringify(bucketData))
          },
          () => {
            // assuming bucket doesnt exist
            return disklet.setText(bucketPath, JSON.stringify({ [hash]: data }))
          }
        )
      },
      query(partition: string, hashes: string[]): Promise<any[]> {
        const { prefixSize } = configData
        if (hashes.length === 0) return Promise.resolve([])

        const formattedPartition = checkAndformatPartition(partition)

        // remove duplicates
        const set = new Set(hashes)
        hashes = Array.from(set)

        const buckets: any = {}
        return Promise.all(
          hashes.map(hash => {
            // make sure hash is as string
            hash = hash.toString()
            if (hash.length < prefixSize)
              throw new Error(
                `Hash length must be at lest length of ${prefixSize}. Got: ${hash}`
              )

            const bucketName = hash.substring(0, prefixSize)

            if (buckets[bucketName] != null) return

            buckets[bucketName] = {}
            return disklet
              .getText(
                `${databaseName}${formattedPartition}/${bucketName}.json`
              )
              .then(JSON.parse, () => ({}))
              .then(data => (buckets[bucketName] = data))
          })
        ).then(
          () => {
            const data = []
            for (let hash of hashes) {
              // make sure hash is string
              hash = hash.toString()
              const bucketName = hash.substring(0, prefixSize)
              const bucket = buckets[bucketName]
              if (bucket[hash] != null) data.push(bucket[hash])
            }
            return data
          },
          () => new Array(hashes.length)
        )
      },
      delete(partition: string, hashes: string[]): Promise<void> {
        const formattedPartition = checkAndformatPartition(partition)
        if (hashes.length === 0) return Promise.resolve()

        const { prefixSize } = configData
        const bucketFetchers = []
        const bucketDict: BucketDictionary = {}
        for (let inputIndex = 0; inputIndex < hashes.length; inputIndex++) {
          // TODO: check to make sure hash length is at least prefixSize
          const bucketName: keyof typeof bucketDict = hashes[
            inputIndex
          ].substring(0, prefixSize)
          const bucketPath = `${databaseName}${formattedPartition}/${bucketName}.json`
          if (bucketDict[bucketName] === undefined) {
            const bucketFetcher = disklet.getText(bucketPath).then(
              serializedBucket => {
                bucketDict[bucketName].bucketData = JSON.parse(serializedBucket)
              },
              () => {
                // assume bucket doesn't exist
              }
            )
            bucketDict[bucketName] = {
              bucketFetcher,
              bucketPath,
              bucketData: {}
            }
            bucketFetchers.push(bucketFetcher)
          }
        }
        return Promise.all(bucketFetchers).then(() => {
          const bucketSavers: Array<Promise<unknown>> = []
          for (let inputIndex = 0; inputIndex < hashes.length; inputIndex++) {
            const bucketName = hashes[inputIndex].substring(0, prefixSize)
            const { bucketPath, bucketData } = bucketDict[bucketName]
            delete bucketData[hashes[inputIndex]]
            const saver = disklet.setText(
              bucketPath,
              JSON.stringify(bucketData)
            )
            bucketSavers.push(saver)
          }

          return Promise.all(bucketSavers).then()
        })
      }
    }
  })
}

export function createHashBase(
  disklet: Disklet,
  databaseName: string,
  prefixSize: number
): Promise<HashBase> {
  databaseName = checkDatabaseName(databaseName)
  if (!isPositiveInteger(prefixSize)) {
    throw new Error(`prefixSize must be a number greater than 0`)
  }

  const configData: HashBaseConfig = {
    type: BaseType.HashBase,
    prefixSize
  }
  return doesDatabaseExist(disklet, databaseName).then(databaseExists => {
    if (databaseExists) {
      throw new Error(`database ${databaseName} already exists`)
    }
    return disklet
      .setText(`${databaseName}/config.json`, JSON.stringify(configData))
      .then(() => openHashBase(disklet, databaseName))
  })
}
