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
import { BaseletConfig, BaseType, DataDump } from './types'

export interface HashBase<K = any> {
  databaseName: string
  insert(partition: string, hash: string, data: K): Promise<void>
  query(partition: string, hashes: string[]): Promise<Array<K | undefined>>
  delete(partition: string, hashes: string[]): Promise<Array<K | undefined>>
  dumpData(
    partition: string
  ): Promise<DataDump<HashBaseConfig, DataDumpDataset<K>>>
}

interface HashBaseConfig extends BaseletConfig {
  prefixSize: number
}

interface BucketDictionary<K> {
  [bucketName: string]: {
    bucketFetcher: Promise<void>
    bucketPath: string
    bucketData: {
      [hash: string]: K
    }
  }
}

interface DataDumpDataset<K> {
  [partition: string]: { [path: string]: K }
}

export function openHashBase<K>(
  disklet: Disklet,
  databaseName: string
): Promise<HashBase<K>> {
  const memlet = getOrMakeMemlet(disklet)

  return getConfig<HashBaseConfig>(disklet, databaseName).then(configData => {
    if (configData.type !== BaseType.HashBase) {
      throw new Error(`Tried to open HashBase, but type is ${configData.type}`)
    }

    function find(
      partition: string,
      hashes: string[],
      remove = false
    ): Promise<Array<K | undefined>> {
      if (hashes.length < 1) {
        return Promise.reject(
          new Error('At least one hash is required to query database.')
        )
      }
      const { prefixSize } = configData
      const bucketFetchers = []
      const bucketDict: BucketDictionary<K> = {}
      for (let inputIndex = 0; inputIndex < hashes.length; inputIndex++) {
        const hash = hashes[inputIndex]
        if (hash.length < prefixSize) continue

        const bucketName: keyof typeof bucketDict = hash.substring(
          0,
          prefixSize
        )
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
      return Promise.all(bucketFetchers).then(() => {
        const results: Array<K | undefined> = []
        const bucketNames = new Set<string>()
        for (let inputIndex = 0; inputIndex < hashes.length; inputIndex++) {
          const bucketName = hashes[inputIndex].substring(0, prefixSize)
          bucketNames.add(bucketName)
          const bucketData = bucketDict[bucketName].bucketData
          const hashData: K | undefined = bucketData[hashes[inputIndex]]

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

        return resultPromise.then(() => results)
      })
    }

    const fns: HashBase<K> = {
      databaseName,
      async insert(partition: string, hash: string, data: K): Promise<void> {
        const { prefixSize } = configData
        if (hash.length < prefixSize) {
          return Promise.reject(
            new Error(`hash must be a string of length at least ${prefixSize}`)
          )
        }

        const prefix = hash.substring(0, prefixSize)
        const bucketPath = getBucketPath(databaseName, partition, prefix)
        const setNewData = (oldData = {}): Promise<void> =>
          memlet.setJson(bucketPath, Object.assign(oldData, { [hash]: data }))
        await memlet.getJson(bucketPath).then(
          bucketData => setNewData(bucketData),
          // assuming bucket doesn't exist
          () => setNewData()
        )
      },
      query(partition: string, hashes: string[]) {
        return find(partition, hashes)
      },
      delete(partition: string, hashes: string[]) {
        return find(partition, hashes, true)
      },
      async dumpData(
        partition: string = ''
      ): Promise<DataDump<HashBaseConfig, DataDumpDataset<K>>> {
        const datatDumpDataset: DataDumpDataset<K> = {}

        // Recursive function for reading files/folders in the partition
        // disklet to accumulate data as a dataDumpDataset
        const dump = async (
          d: Disklet,
          partition: string = ''
        ): Promise<void> => {
          const listing = await d.list()
          const promises = Object.keys(listing).map(async path => {
            if (getConfigPath(databaseName).includes(path)) {
              return
            }

            const type = listing[path]
            if (type === 'folder') {
              // Assert that the partition is not defined because we should
              // only expect to recurse one folder level in the disklet.
              if (partition !== '')
                throw new Error('Unexpected partition hierarchy')

              // Recurse into folder using the path as the partition key.
              return dump(navigateDisklet(d, path), path)
            }
            if (type === 'file') {
              // Write the file to the dataDumpDataset
              const fileData = await d.getText(path)
              datatDumpDataset[partition] = {
                ...datatDumpDataset[partition],
                ...JSON.parse(fileData)
              }
              return
            }

            throw new Error(`Unknown listing type ${type}`)
          })
          await Promise.all(promises)
        }

        const partitionDisklet = navigateDisklet(
          disklet,
          getPartitionPath(databaseName, partition)
        )

        await dump(partitionDisklet, partition)

        return {
          config: configData,
          data: datatDumpDataset
        }
      }
    }

    return fns
  })
}

export function createHashBase<K>(
  disklet: Disklet,
  databaseName: string,
  prefixSize: number
): Promise<HashBase<K>> {
  databaseName = checkDatabaseName(databaseName)
  if (!isPositiveInteger(prefixSize)) {
    throw new Error(`prefixSize must be a number greater than 0`)
  }

  return doesDatabaseExist(disklet, databaseName).then(databaseExists => {
    if (databaseExists) {
      throw new Error(`database ${databaseName} already exists`)
    }

    const configData: HashBaseConfig = {
      type: BaseType.HashBase,
      prefixSize: prefixSize
    }
    return setConfig(disklet, databaseName, configData).then(() =>
      openHashBase<K>(disklet, databaseName)
    )
  })
}
