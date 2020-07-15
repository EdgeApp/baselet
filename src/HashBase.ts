import { Disklet } from 'disklet'

import { checkAndformatPartition } from './helpers'
import { BaseletConfig, BaseType } from './types'

export interface HashBase {
  insert(partition: string, hash: string, data: any): Promise<unknown>
  query(partition: string, hashes: string[]): Promise<any[]>
}

interface HashBaseConfig extends BaseletConfig {
  prefixSize: number
}

interface BucketDictionary {
  [bucketName: string]: {
    bucketFetcher: Promise<void>
    bucketData: {
      [hash: string]: any
    }
  }
}

export function openHashBase(
  disklet: Disklet,
  databaseName: string
): Promise<HashBase> {
  // check that the db exists and is of type HashBase

  function getConfig(): Promise<HashBaseConfig> {
    return disklet
      .getText(`${databaseName}/config.json`)
      .then(serializedConfig => JSON.parse(serializedConfig))
  }

  return getConfig().then(configData => {
    return {
      insert(partition: string, hash: string, data: any): Promise<unknown> {
        // check that partition only contains letters, numbers, and underscores
        // if no partition, then root
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
        return disklet
          .getText(bucketPath)
          .then(serializedBucket => JSON.parse(serializedBucket))
          .then(
            bucketData => {
              bucketData[hash] = data
              return disklet.setText(bucketPath, JSON.stringify(bucketData))
            },
            error => {
              console.log(error)
              console.log('assuming bucket doesnt exist')
              return disklet.setText(
                bucketPath,
                JSON.stringify({ [hash]: data })
              )
            }
          )
      },
      query(partition: string, hashes: string[]): Promise<any[]> {
        const formattedPartition = checkAndformatPartition(partition)
        if (hashes.length < 1) {
          return Promise.reject(
            new Error('At least one hash is required to query database.')
          )
        }
        const { prefixSize } = configData
        const bucketFetchers = []
        const bucketDict: BucketDictionary = {}
        for (let inputIndex = 0; inputIndex < hashes.length; inputIndex++) {
          // check to make sure hash length is at least prefixSize
          const bucketName: keyof typeof bucketDict = hashes[
            inputIndex
          ].substring(0, prefixSize)
          if (bucketDict[bucketName] === undefined) {
            const bucketFetcher = disklet
              .getText(
                `${databaseName}${formattedPartition}/${bucketName}.json`
              )
              .then(serializedBucket => JSON.parse(serializedBucket))
              .then(bucketData => {
                bucketDict[bucketName].bucketData = bucketData
              })
            bucketDict[bucketName] = {
              bucketFetcher,
              bucketData: {}
            }
            bucketFetchers.push(bucketFetcher)
          }
        }
        return Promise.all(bucketFetchers).then(() => {
          const results = []
          for (let inputIndex = 0; inputIndex < hashes.length; inputIndex++) {
            const bucketName = hashes[inputIndex].substring(0, prefixSize)
            const bucketData = bucketDict[bucketName].bucketData
            const hashData = bucketData[hashes[inputIndex]]
            results.push(hashData)
          }
          return results
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
  // check that databaseName only contains letters, numbers, and underscores
  // check if database already exists
  // check that prefixSize is a positive Integer

  // create config file at databaseName/config.json
  const configData: HashBaseConfig = {
    type: BaseType.HashBase,
    prefixSize
  }
  return disklet
    .setText(`${databaseName}/config.json`, JSON.stringify(configData))
    .then(() => openHashBase(disklet, databaseName))
}
