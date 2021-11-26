import { Disklet } from 'disklet'
import { Memlet } from 'memlet'

import {
  checkDatabaseName,
  doesDatabaseExist,
  getBucketPath,
  getConfig,
  getOrMakeMemlet,
  getPartitionPath,
  isPositiveInteger,
  setConfig
} from './helpers'
import { BaseType, DataDump, RangeBaseConfig, RangeBaseOptions } from './types'

export type RangeRecord<K, RangeKey extends string, IdKey extends string> = {
  [key in RangeKey]: number
} &
  { [key in IdKey]: string } &
  K

export interface RangeBase<
  K extends RangeRecord<any, RangeKey, IdKey>,
  RangeKey extends string,
  IdKey extends string
> {
  rangeKey: RangeKey
  idKey: IdKey
  databaseName: string
  insert(
    partition: string,
    data: RangeRecord<K, RangeKey, IdKey>
  ): Promise<void>
  query(
    partition: string,
    rangeStart: number,
    rangeEnd?: number
  ): Promise<Array<RangeRecord<K, RangeKey, IdKey>>>
  queryById(
    partition: string,
    range: number,
    id: string
  ): Promise<RangeRecord<K, RangeKey, IdKey>>
  queryByCount(
    partition: string,
    count: number,
    offset: number
  ): Promise<Array<RangeRecord<K, RangeKey, IdKey>>>
  delete(
    partition: string,
    range: number,
    id: string
  ): Promise<RangeRecord<K, RangeKey, IdKey>>
  update(
    partition: string,
    oldRange: number,
    newData: RangeRecord<K, RangeKey, IdKey>
  ): Promise<void>
  min(partition: string): number
  max(partition: string): number
  size(partition: string): number
  dumpData(
    partition: string
  ): Promise<
    DataDump<
      RangeBaseConfig<RangeBase<K, RangeKey, IdKey>>,
      Array<RangeRecord<K, RangeKey, IdKey>>
    >
  >
}

export async function openRangeBase<
  K extends RangeRecord<any, RangeKey, IdKey>,
  RangeKey extends string,
  IdKey extends string
>(
  storage: Disklet | Memlet,
  databaseName: string
): Promise<RangeBase<K, RangeKey, IdKey>> {
  const memlet = getOrMakeMemlet(storage)

  // uses binary search
  function getIndex(
    input: number | string,
    keyName: RangeKey | IdKey,
    bucket: Array<RangeRecord<K, RangeKey, IdKey>>,
    startIndex: number = 0,
    endIndex: number = bucket.length - 1,
    findLastOccurrence: boolean = false
  ): {
    found: boolean
    index: number
  } {
    if (bucket.length === 0) return { found: false, index: 0 }

    let minIndex = startIndex
    let maxIndex = endIndex
    let currentIndex = 0
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
            maxIndex = currentIndex - 1
          }
        }
      }
    }
    return {
      found: false,
      index:
        currentElement != null && currentElement < input
          ? currentIndex + 1
          : currentIndex
    }
  }

  const configData: RangeBaseConfig<
    RangeBase<K, RangeKey, IdKey>
  > = await getConfig(memlet, databaseName)
  if (configData.type !== BaseType.RangeBase) {
    throw new Error(`Tried to open RangeBase, but type is ${configData.type}`)
  }

  /**
   * Calculate and save the new minimum or maximum range limit for a partition.
   * @param partition
   * @param max
   */
  async function findLimit(
    partition: string,
    max: boolean
  ): Promise<number | undefined> {
    const { rangeKey } = configData

    const partitionPath = getPartitionPath(databaseName, partition)
    const list = await memlet.list(partitionPath)
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

    const bucketData = await fetchBucketData(partition, limitBucketNumber)
    const index = max ? bucketData.length - 1 : 0
    return Number(bucketData[index][rangeKey])
  }

  /**
   * Updates the config file with the most up to date min or max value.
   * The data object passed in should already have been saved/deleted from the database.
   * @param partition
   * @param data
   * @param deleted
   */
  async function updateMinMax(
    partition: string,
    data: any,
    deleted: boolean
  ): Promise<void> {
    const { rangeKey, limits } = configData
    const range: number = data[rangeKey]
    const partitionLimits = limits[partition] ?? (limits[partition] = {})

    if (deleted) {
      // Check if this item was the only one at the min or max range
      const isMin = range === partitionLimits.minRange
      const isMax = range === partitionLimits.maxRange
      if (isMin || isMax) {
        return out.query(partition, range).then(items => {
          // If it was, find what the new min or max is supposed to be and save it in the config
          if (items.length === 0) {
            return findLimit(partition, isMax).then(minOrMax => {
              if (isMin) {
                partitionLimits.minRange = minOrMax
              }
              if (isMax) {
                partitionLimits.maxRange = minOrMax
              }
              return setConfig(memlet, databaseName, configData)
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
      return setConfig(memlet, databaseName, configData)
    }
  }

  /**
   * Finds an item in the database partition for a given id.
   * If the `remove` flag is true then if the item is found, it will be deleted.
   * @param partition
   * @param range
   * @param id
   * @param remove Flag to delete the found item from the database
   */
  async function find(
    partition: string,
    range: number,
    id: string,
    remove = false
  ): Promise<any> {
    const { bucketSize, rangeKey, idKey } = configData
    const bucketNumber = Math.floor(range / bucketSize)

    const bucketData: any[] = await fetchBucketData(partition, bucketNumber)
    const firstRangeOccurrence = getIndex(range, rangeKey, bucketData)
    if (firstRangeOccurrence.found) {
      const lastRangeOccurrence = getIndex(
        range,
        rangeKey,
        bucketData,
        firstRangeOccurrence.index,
        bucketData.length - 1,
        true
      )
      const targetIndex = getIndex(
        id,
        idKey,
        bucketData,
        firstRangeOccurrence.index,
        lastRangeOccurrence.index
      )
      if (targetIndex.found) {
        if (remove) {
          const [removedData] = bucketData.splice(targetIndex.index, 1)
          await saveBucket(partition, bucketNumber, bucketData)
          await updateMinMax(partition, removedData, true)
          return removedData
        } else {
          return bucketData[targetIndex.index]
        }
      }
    }
  }

  async function queryByCount(
    partition: string,
    count: number,
    offset: number
  ): Promise<any[]> {
    const nums = await fetchSortedBucketNumbers(partition)
    let data: any[] = []
    let offsetCount = offset
    async function fetchBucket(index: number): Promise<void> {
      if (count === data.length || index === nums.length)
        return Promise.resolve()

      const bucketData = await fetchBucketData(partition, nums[index])
      if (bucketData.length <= offsetCount) {
        offsetCount -= bucketData.length
        return fetchBucket(++index)
      }

      const end = bucketData.length - offsetCount
      const startIndex = end + data.length - count
      const start = startIndex > 0 ? startIndex : 0
      data = [...data, ...bucketData.slice(start, end).reverse()]

      offsetCount = 0
      return fetchBucket(++index)
    }

    await fetchBucket(0)
    return data
  }

  async function fetchSortedBucketNumbers(
    partition: string
  ): Promise<string[]> {
    const partitionPath = getPartitionPath(databaseName, partition)
    const list = await memlet.list(partitionPath)
    const numMap: { [bucketNumber: string]: true } = {}
    for (const path in list) {
      if (!Object.hasOwnProperty.call(list, path)) continue
      if (list[path] === 'folder') continue
      if (/config\.json$/.test(path)) continue

      const chucks = path.split('/')
      const bucketNumber = Number(chucks[chucks.length - 1].split('.')[0])
      numMap[bucketNumber] = true
    }

    return Object.keys(numMap).sort((a, b) => (a < b ? 1 : -1))
  }

  /**
   * Fetches the data from a bucket. If the bucket file does not exists, it returns an empty array.
   * @param partition
   * @param num Bucket number to fetch
   * @return Array of items from the bucket
   */
  function fetchBucketData(
    partition: string,
    num: string | number
  ): Promise<Array<RangeRecord<K, RangeKey, IdKey>>> {
    const path = getBucketPath(databaseName, partition, num)
    return memlet.getJson(path).catch(() => [])
  }

  /**
   * Saves the data to a bucket.
   * @param partition
   * @param num Bucket number to fetch
   * @param data Array of items to save
   */
  async function saveBucket(
    partition: string,
    num: number,
    data: Array<RangeRecord<K, RangeKey, IdKey>>
  ): Promise<void> {
    const path = getBucketPath(databaseName, partition, num)
    if (data.length === 0) {
      return memlet.delete(path)
    } else {
      return memlet.setJson(path, data)
    }
  }

  const out: RangeBase<K, RangeKey, IdKey> = {
    rangeKey: configData.rangeKey,
    idKey: configData.idKey,
    databaseName,

    async insert(partition, data) {
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

      const existingData = await find(partition, data[rangeKey], data[idKey])
      if (existingData != null) {
        throw new Error('Cannot insert data because id already exists')
      }

      const bucketNumber = Math.floor(data[rangeKey] / bucketSize)
      await fetchBucketData(partition, bucketNumber).then(
        bucketData => {
          const firstRangeOccurrence = getIndex(
            data[rangeKey],
            rangeKey,
            bucketData
          )
          if (!firstRangeOccurrence.found) {
            bucketData.splice(firstRangeOccurrence.index, 0, data)
          } else {
            const lastRangeOccurrence = getIndex(
              data[rangeKey],
              rangeKey,
              bucketData,
              firstRangeOccurrence.index,
              bucketData.length - 1,
              true
            )
            const targetIndex = getIndex(
              data[idKey],
              idKey,
              bucketData,
              firstRangeOccurrence.index,
              lastRangeOccurrence.index
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
      await updateMinMax(partition, data, false)
      const size = configData.sizes[partition] ?? 0
      configData.sizes[partition] = size + 1
      await setConfig(memlet, databaseName, configData)
    },

    async query(partition = '/', rangeStart, rangeEnd = rangeStart) {
      const { bucketSize, rangeKey } = configData
      const bucketFetchers: Array<
        Promise<Array<RangeRecord<K, RangeKey, IdKey>>>
      > = []

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

      const bucketList = await Promise.all(bucketFetchers)
      let queryResults: Array<RangeRecord<K, RangeKey, IdKey>> = []
      if (bucketList.length === 1) {
        // only one bucket
        const firstRangeIndex = getIndex(rangeStart, rangeKey, bucketList[0])
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
          lastRangeIndex.found ? lastRangeIndex.index + 1 : lastRangeIndex.index
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
    },

    queryById(partition, range, id) {
      return find(partition, range, id)
    },

    queryByCount(partition, count, offset = 0) {
      return queryByCount(partition, count, offset)
    },

    delete(partition, range, id) {
      return find(partition, range, id, true).then(data => {
        const size = configData.sizes[partition] ?? 0
        configData.sizes[partition] = size - 1
        return setConfig(memlet, databaseName, configData).then(() => data)
      })
    },

    async update(partition, oldRangeKey, newData) {
      const existingData = await out.delete(
        partition,
        oldRangeKey,
        newData[configData.idKey]
      )
      if (existingData == null) {
        throw new Error('Cannot update a non-existing element')
      }

      await out.insert(partition, newData)
    },

    min(partition) {
      return configData.limits[partition]?.minRange ?? 0
    },

    max(partition) {
      return configData.limits[partition]?.maxRange ?? 0
    },

    size(partition) {
      return configData.sizes[partition] ?? 0
    },

    async dumpData(partition) {
      const min = out.min(partition) ?? 0
      const data = await out.query(partition, min, out.max(partition))
      return {
        config: configData,
        data
      }
    }
  }

  return out
}

export async function createRangeBase<
  K extends RangeRecord<any, RangeKey, IdKey>,
  RangeKey extends string,
  IdKey extends string
>(
  storage: Disklet | Memlet,
  options: RangeBaseOptions<RangeKey, IdKey>
): Promise<RangeBase<K, RangeKey, IdKey>> {
  const memlet = getOrMakeMemlet(storage)
  const { name, bucketSize, rangeKey, idKey, idPrefixLength = 1 } = options

  if (!isPositiveInteger(bucketSize)) {
    throw new Error(`bucketSize must be a number greater than 0`)
  }
  const dbName = checkDatabaseName(name)
  const databaseExists = await doesDatabaseExist(memlet, dbName)
  if (databaseExists) {
    throw new Error(`database ${dbName} already exists`)
  }

  const configData: RangeBaseConfig<RangeBase<K, RangeKey, IdKey>> = {
    type: BaseType.RangeBase,
    bucketSize: Math.floor(bucketSize),
    rangeKey,
    idKey,
    idPrefixLength,
    limits: {},
    sizes: {}
  }
  await setConfig(memlet, dbName, configData)

  return openRangeBase(memlet, dbName)
}

export async function createOrOpenRangeBase<
  K extends RangeRecord<any, RangeKey, IdKey>,
  RangeKey extends string,
  IdKey extends string
>(
  storage: Disklet | Memlet,
  options: RangeBaseOptions<RangeKey, IdKey>
): Promise<RangeBase<K, RangeKey, IdKey>> {
  const memlet = getOrMakeMemlet(storage)
  try {
    return await createRangeBase(memlet, options)
  } catch (error) {
    if (error instanceof Error && !error.message.includes('already exists')) {
      throw error
    }
    return openRangeBase(memlet, options.name)
  }
}
