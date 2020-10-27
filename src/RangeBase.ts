import { Disklet } from 'disklet'
import { MemletConfig } from 'memlet'

import { createHashBase, openHashBase } from './HashBase'
import {
  checkDatabaseName,
  doesDatabaseExist,
  getBucketPath,
  getConfig,
  getOrMakeMemlet,
  getPartitionPath,
  isMemlet,
  isPositiveInteger,
  setConfig
} from './helpers'
import { BaseletConfig, BaseType } from './types'

export interface RangeBase {
  insert(partition: string, data: any): Promise<unknown>
  query(
    partition: string,
    rangeStart: number,
    rangeEnd?: number
  ): Promise<any[]>
  queryById(partition: string, idKey: string): Promise<any>
  delete(partition: string, idKey: string): Promise<any>
  move(partition: string, newData: any): Promise<unknown>
  min(partition: string): undefined | number
  max(partition: string): undefined | number
}

export interface RangeBaseData {
  [someKey: string]: any
}

interface RangeBaseConfig extends BaseletConfig {
  bucketSize: number
  idDatabaseName: string
  rangeKey: string
  idKey: string
  idPrefixLength: number
  limits: PartitionLimits
}

interface PartitionLimits {
  [partition: string]: undefined | PartitionLimit
}
interface PartitionLimit {
  minRange?: number
  maxRange?: number
}

export function openRangeBase(
  disklet: Disklet,
  databaseName: string,
  memletConfig?: Partial<MemletConfig>
): Promise<RangeBase> {
  const memlet = getOrMakeMemlet(disklet, memletConfig)

  // uses binary search
  function getIndex(
    input: number | string,
    keyName: string,
    bucket: RangeBaseData[],
    startIndex: number = 0,
    endIndex: number = bucket.length - 1,
    findLastOccurrence: boolean = false
  ): {
    found: boolean
    index: number
  } {
    if (bucket.length === 0) return { found: false, index: 0 }

    let minIndex: number = startIndex
    let maxIndex: number = endIndex
    let currentIndex: number = 0
    let currentElement

    if (
      !Number.isInteger(startIndex) ||
      !Number.isInteger(endIndex) ||
      startIndex < 0 ||
      endIndex > bucket.length - 1 ||
      endIndex < startIndex
    ) {
      throw new Error(
        'indices must be valid bucket index values and endIndex >= startIndex'
      )
    }

    while (minIndex <= maxIndex) {
      currentIndex = ~~((minIndex + maxIndex) / 2)
      currentElement = bucket[currentIndex][keyName]

      if (currentElement < input) {
        minIndex = currentIndex + 1
      } else if (currentElement > input) {
        maxIndex = currentIndex - 1
      } else {
        if (findLastOccurrence) {
          const isLastOccurrence =
            currentIndex === maxIndex ||
            bucket[currentIndex + 1][keyName] !== input
          if (isLastOccurrence) {
            return {
              found: true,
              index: currentIndex
            }
          } else {
            minIndex = currentIndex + 1
          }
        } else {
          const isFirstOccurrence =
            currentIndex === minIndex ||
            bucket[currentIndex - 1][keyName] !== input
          if (isFirstOccurrence) {
            return {
              found: true,
              index: currentIndex
            }
          } else {
            minIndex = currentIndex + 1
          }
        }
      }
    }
    return {
      found: false,
      index: currentElement < input ? currentIndex + 1 : currentIndex
    }
  }

  return getConfig<RangeBaseConfig>(disklet, databaseName).then(configData => {
    if (configData.type !== BaseType.RangeBase) {
      throw new Error(`Tried to open RangeBase, but type is ${configData.type}`)
    }

    const idHashBasePromise = isMemlet(disklet)
      ? openHashBase(disklet, configData.idDatabaseName)
      : openHashBase(disklet, configData.idDatabaseName, memletConfig)
    return idHashBasePromise.then(idDb => {
      /**
       * Calculate and save the new minimum or maximum range limit for a partition.
       * @param partition
       * @param max
       */
      function findNewLimit(
        partition: string,
        max: boolean
      ): Promise<number | undefined> {
        const { rangeKey } = configData

        const partitionPath = getPartitionPath(databaseName, partition)
        return memlet.list(partitionPath).then(list => {
          let limitBucketNumber: number | undefined
          for (const path in list) {
            if (/config\.json$/.test(path)) continue

            const chucks = path.split('/')
            const bucketNumber = Number(chucks[chucks.length - 1].split('.')[0])
            if (limitBucketNumber == null) {
              limitBucketNumber = bucketNumber
            } else if (max) {
              if (bucketNumber > limitBucketNumber) {
                limitBucketNumber = bucketNumber
              }
            } else {
              if (bucketNumber < limitBucketNumber) {
                limitBucketNumber = bucketNumber
              }
            }
          }

          // If this is still not set then no buckets were found.
          if (limitBucketNumber == null) return

          return fetchBucketData(partition, limitBucketNumber).then(
            bucketData => {
              const index = max ? bucketData.length - 1 : 0
              return Number(bucketData[index][rangeKey])
            }
          )
        })
      }

      /**
       * Updates the config file with the most up to date min or max value.
       * The data object passed in should already have been saved/deleted from the database.
       * @param partition
       * @param data
       * @param deleted
       */
      function updateMinMax(
        partition: string,
        data: any,
        deleted: boolean
      ): Promise<unknown> | undefined {
        const { rangeKey, limits } = configData
        const range: number = data[rangeKey]
        const partitionLimits = limits[partition] ?? (limits[partition] = {})

        if (deleted) {
          // Check if this item was the only one at the min or max range
          const isMin = range === partitionLimits.minRange
          const isMax = range === partitionLimits.maxRange
          if (isMin || isMax) {
            return fns.query(partition, range).then(items => {
              // If it was, find what the new min or max is supposed to be and save it in the config
              if (items.length === 0) {
                return findNewLimit(partition, isMax).then(minOrMax => {
                  if (isMin) {
                    partitionLimits.minRange = minOrMax
                  }
                  if (isMax) {
                    partitionLimits.maxRange = minOrMax
                  }
                  return setConfig(disklet, databaseName, configData)
                })
              }
            })
          }
        } else {
          const { minRange, maxRange } = partitionLimits
          if (minRange == null || range < minRange) {
            partitionLimits.minRange = range
          }
          if (maxRange == null || range > maxRange) {
            partitionLimits.maxRange = range
          }
          return setConfig(disklet, databaseName, configData)
        }
      }

      /**
       * Finds an item in the database partition for a given id.
       * If the `remove` flag is true then if the item is found, it will be deleted.
       * @param partition
       * @param id
       * @param remove Flag to delete the found item from the database
       */
      function find(
        partition: string,
        id: string,
        remove = false
      ): Promise<any> {
        return idDb.query(partition, [id]).then(([range]) => {
          if (range == null) return

          const { bucketSize, rangeKey, idKey } = configData
          const bucketNumber = Math.floor(range / bucketSize)

          return fetchBucketData(partition, bucketNumber).then(
            (bucketData: any[]) => {
              const firstRangeOccurence = getIndex(range, rangeKey, bucketData)
              const lastRangeOccurence = getIndex(
                range,
                rangeKey,
                bucketData,
                firstRangeOccurence.index,
                bucketData.length - 1,
                true
              )
              const targetIndex = getIndex(
                id,
                idKey,
                bucketData,
                firstRangeOccurence.index,
                lastRangeOccurence.index
              )
              if (targetIndex.found) {
                if (remove) {
                  // Remove from the id table and bucket, then save and return removed data
                  return idDb.delete(partition, [id]).then(() => {
                    const [removedData] = bucketData.splice(
                      targetIndex.index,
                      1
                    )
                    return saveBucket(partition, bucketNumber, bucketData)
                      .then(() => updateMinMax(partition, removedData, true))
                      .then(() => removedData)
                  })
                } else {
                  return bucketData[targetIndex.index]
                }
              }
            }
          )
        })
      }

      /**
       * Fetches the data from a bucket. If the bucket file does not exists, it returns an empty array.
       * @param partition
       * @param num Bucket number to fetch
       * @return Array of items from the bucket
       */
      function fetchBucketData(partition: string, num: number): Promise<any[]> {
        const path = getBucketPath(databaseName, partition, num)
        return memlet.getJson(path).catch(() => [])
      }

      /**
       * Saves the data to a bucket.
       * @param partition
       * @param num Bucket number to fetch
       * @param data Array of items to save
       */
      function saveBucket(
        partition: string,
        num: number,
        data: any[]
      ): Promise<unknown> {
        const path = getBucketPath(databaseName, partition, num)
        if (data.length === 0) {
          return memlet.delete(path)
        } else {
          return memlet.setJson(path, data)
        }
      }

      const fns: RangeBase = {
        insert(partition: string, data: any): Promise<unknown> {
          const { bucketSize, rangeKey, idKey } = configData
          if (
            !(
              Object.prototype.hasOwnProperty.call(data, rangeKey) &&
              Object.prototype.hasOwnProperty.call(data, idKey)
            )
          ) {
            return Promise.reject(
              new Error(`data must have properties ${rangeKey} and ${idKey}`)
            )
          }

          // first check if the id exists in the id database
          return idDb.query(partition, [data[idKey]]).then(([range]) => {
            if (range != null) {
              throw new Error('Cannot insert data because id already exists')
            }

            const bucketNumber = Math.floor(data[rangeKey] / bucketSize)
            return (
              fetchBucketData(partition, bucketNumber)
                .then(
                  bucketData => {
                    const firstRangeOccurence = getIndex(
                      data[rangeKey],
                      rangeKey,
                      bucketData
                    )
                    if (!firstRangeOccurence.found) {
                      bucketData.splice(firstRangeOccurence.index, 0, data)
                    } else {
                      const lastRangeOccurence = getIndex(
                        data[rangeKey],
                        rangeKey,
                        bucketData,
                        firstRangeOccurence.index,
                        bucketData.length - 1,
                        true
                      )
                      const targetIndex = getIndex(
                        data[idKey],
                        idKey,
                        bucketData,
                        firstRangeOccurence.index,
                        lastRangeOccurence.index
                      )
                      bucketData.splice(targetIndex.index, 0, data)
                    }
                    return saveBucket(partition, bucketNumber, bucketData)
                  },
                  () => {
                    // assuming bucket doesnt exist
                    return saveBucket(partition, bucketNumber, [data])
                  }
                )
                // Save the id
                .then(() => idDb.insert(partition, data[idKey], data[rangeKey]))
                .then(() => updateMinMax(partition, data, false))
            )
          })
        },
        query(
          partition: string = '/',
          rangeStart: number,
          rangeEnd: number = rangeStart
        ): Promise<any[]> {
          const { bucketSize, rangeKey } = configData
          const bucketFetchers: any[] = []

          if (rangeEnd < rangeStart) {
            throw new Error('rangeStart must be larger than rangeEnd')
          }

          for (
            let bucketNumber = Math.floor(rangeStart / bucketSize);
            bucketNumber <= Math.floor(rangeEnd / bucketSize);
            bucketNumber++
          ) {
            bucketFetchers.push(fetchBucketData(partition, bucketNumber))
          }

          return Promise.all(bucketFetchers).then(bucketList => {
            let queryResults: any[] = []
            if (bucketList.length === 1) {
              // only one bucket
              const firstRangeIndex = getIndex(
                rangeStart,
                rangeKey,
                bucketList[0]
              )
              const lastRangeIndex = getIndex(
                rangeEnd,
                rangeKey,
                bucketList[0],
                0,
                bucketList[0].length - 1,
                true
              )
              queryResults = bucketList[0].slice(
                firstRangeIndex.index,
                lastRangeIndex.found
                  ? lastRangeIndex.index + 1
                  : lastRangeIndex.index
              )
            } else {
              for (let i = 0; i < bucketList.length; i++) {
                if (i === 0) {
                  // is first bucket
                  const firstRangeIndex = getIndex(
                    rangeStart,
                    rangeKey,
                    bucketList[i]
                  )
                  queryResults = bucketList[i].slice(firstRangeIndex.index)
                } else if (i === bucketList.length - 1) {
                  // is last bucket
                  const lastRangeIndex = getIndex(
                    rangeEnd,
                    rangeKey,
                    bucketList[i],
                    0,
                    bucketList[i].length - 1,
                    true
                  )
                  Array.prototype.push.apply(
                    queryResults,
                    bucketList[i].slice(
                      0,
                      lastRangeIndex.found
                        ? lastRangeIndex.index + 1
                        : lastRangeIndex.index
                    )
                  )
                } else {
                  // is bucket in between range values
                  queryResults.push.apply(queryResults, bucketList[i])
                }
              }
            }
            return queryResults
          })
        },
        queryById(partition: string, id: string): Promise<any> {
          return find(partition, id)
        },
        delete(partition: string, id: string): Promise<any> {
          return find(partition, id, true)
        },
        move(partition: string, newData: any): Promise<unknown> {
          return Promise.resolve()
            .then(() => fns.delete(partition, newData[configData.idKey]))
            .then(() => fns.insert(partition, newData))
        },
        min(partition: string): undefined | number {
          return configData.limits[partition]?.minRange
        },
        max(partition: string): undefined | number {
          return configData.limits[partition]?.maxRange
        }
      }

      return fns
    })
  })
}

export function createRangeBase(
  disklet: Disklet,
  databaseName: string,
  bucketSize: number,
  rangeKey: string,
  idKey: string,
  idPrefixLength = 1
): Promise<RangeBase> {
  if (!isPositiveInteger(bucketSize)) {
    throw new Error(`bucketSize must be a number greater than 0`)
  }
  databaseName = checkDatabaseName(databaseName)
  return doesDatabaseExist(disklet, databaseName).then(databaseExists => {
    if (databaseExists) {
      throw new Error(`database ${databaseName} already exists`)
    }

    const configData: RangeBaseConfig = {
      type: BaseType.RangeBase,
      bucketSize: Math.floor(bucketSize),
      idDatabaseName: `${databaseName}_ids`,
      rangeKey,
      idKey,
      idPrefixLength,
      limits: {}
    }
    return createHashBase(disklet, configData.idDatabaseName, idPrefixLength)
      .then(() => setConfig(disklet, databaseName, configData))
      .then(() => openRangeBase(disklet, databaseName))
  })
}
