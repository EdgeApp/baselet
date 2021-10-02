import { Disklet, navigateDisklet } from 'disklet'

import {
  checkDatabaseName,
  doesDatabaseExist,
  getBucketPath,
  getConfig,
  getConfigPath,
  getOrMakeMemlet,
  getPartitionPath,
  isPositiveInteger,
  setConfig
} from './helpers'
import { BaseletConfig, BaseType } from './types'

export interface HashBase {
  databaseName: string
  insert(partition: string, hash: string, data: any): Promise<unknown>
  query(partition: string, hashes: string[]): Promise<any[]>
  delete(partition: string, hashes: string[]): Promise<any[]>
  dumpData(partition: string): Promise<any>
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

export async function openHashBase(
  disklet: Disklet,
  databaseName: string
): Promise<HashBase> {
  const memlet = getOrMakeMemlet(disklet)

  const configData = await getConfig<HashBaseConfig>(disklet, databaseName)
  if (configData.type !== BaseType.HashBase) {
    throw new Error(`Tried to open HashBase, but type is ${configData.type}`)
  }

  async function find(
    partition: string,
    hashes: string[],
    remove = false
  ): Promise<any[]> {
    if (hashes.length < 1) {
      return Promise.reject(
        new Error('At least one hash is required to query database.')
      )
    }
    const { prefixSize } = configData
    const bucketFetchers = []
    const bucketDict: BucketDictionary = {}
    for (let inputIndex = 0; inputIndex < hashes.length; inputIndex++) {
      const hash = hashes[inputIndex]
      if (hash.length < prefixSize) continue

      const bucketName: keyof typeof bucketDict = hash.substring(0, prefixSize)
      if (bucketDict[bucketName] === undefined) {
        const bucketPath = getBucketPath(databaseName, partition, bucketName)
        const bucketFetcher = memlet.getJson(bucketPath).then(
          bucketData => (bucketDict[bucketName].bucketData = bucketData),
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
    await Promise.all(bucketFetchers)

    const results: any[] = []
    const bucketNames = new Set<string>()
    for (let inputIndex = 0; inputIndex < hashes.length; inputIndex++) {
      const bucketName = hashes[inputIndex].substring(0, prefixSize)
      bucketNames.add(bucketName)
      const bucketData = bucketDict[bucketName].bucketData
      const hashData = bucketData[hashes[inputIndex]]

      if (remove) {
        delete bucketData[hashes[inputIndex]]
      }

      results.push(hashData)
    }

    let resultPromise: Promise<unknown> = Promise.resolve()
    if (remove) {
      const deletePromises = Array.from(bucketNames).map(bucketName => {
        const { bucketPath, bucketData } = bucketDict[bucketName]
        return memlet.setJson(bucketPath, bucketData)
      })
      resultPromise = Promise.all(deletePromises)
    }

    await resultPromise
    return results
  }

  const out: HashBase = {
    databaseName,

    insert(partition: string, hash: string, data: any): Promise<unknown> {
      const { prefixSize } = configData
      if (hash.length < prefixSize) {
        return Promise.reject(
          new Error(`hash must be a string of length at least ${prefixSize}`)
        )
      }

      const prefix = hash.substring(0, prefixSize)
      const bucketPath = getBucketPath(databaseName, partition, prefix)
      const setNewData = (oldData = {}): any =>
        memlet.setJson(bucketPath, Object.assign(oldData, { [hash]: data }))
      return memlet.getJson(bucketPath).then(
        bucketData => setNewData(bucketData),
        // assuming bucket doesnt exist
        () => setNewData()
      )
    },

    query(partition: string, hashes: string[]): Promise<any[]> {
      return find(partition, hashes)
    },

    delete(partition: string, hashes: string[]): Promise<any[]> {
      return find(partition, hashes, true)
    },

    async dumpData(partition: string): Promise<any> {
      const dump = (d: Disklet, data: any = {}): Promise<any> => {
        return d.list().then(listing => {
          return Promise.all(
            Object.keys(listing).map(path => {
              if (getConfigPath(databaseName).includes(path)) {
                return
              }

              const type = listing[path]
              if (type === 'folder') {
                return dump(navigateDisklet(d, path), data[path]).then(
                  folderData => {
                    data[path] = folderData
                  }
                )
              }
              if (type === 'file') {
                return d.getText(path).then(fileData => {
                  data = { ...data, ...JSON.parse(fileData) }
                })
              }
            })
          ).then(() => data)
        })
      }

      const partitionDisklet = navigateDisklet(
        disklet,
        getPartitionPath(databaseName, partition)
      )
      return dump(partitionDisklet).then(data => {
        return {
          config: configData,
          data
        }
      })
    }
  }

  return out
}

export async function createHashBase(
  disklet: Disklet,
  databaseName: string,
  prefixSize: number
): Promise<HashBase> {
  const dbName = checkDatabaseName(databaseName)
  if (!isPositiveInteger(prefixSize)) {
    throw new Error(`prefixSize must be a number greater than 0`)
  }

  const databaseExists = await doesDatabaseExist(disklet, dbName)
  if (databaseExists) {
    throw new Error(`database ${dbName} already exists`)
  }

  const configData: HashBaseConfig = {
    type: BaseType.HashBase,
    prefixSize: prefixSize
  }
  await setConfig(disklet, dbName, configData)

  return openHashBase(disklet, dbName)
}
