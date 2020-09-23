import { Disklet } from 'disklet'

import { createHashBase, openHashBase } from './HashBase'
import {
  checkAndformatPartition,
  checkDatabaseName,
  doesDatabaseExist,
  isPositiveInteger
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
}

export function openRangeBase(
  disklet: Disklet,
  databaseName: string
): Promise<RangeBase> {
  function getConfig(): Promise<RangeBaseConfig> {
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

  return getConfig().then(configData => {
    if (configData.type !== BaseType.RangeBase) {
      throw new Error(`Tried to open RangeBase, but type is ${configData.type}`)
    }

    return openHashBase(disklet, configData.idDatabaseName).then(idDb => {
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
          const formattedPartition = checkAndformatPartition(partition)
          const bucketNumber = Math.floor(range / bucketSize)
          const bucketFilename = `${bucketNumber}.json`
          const bucketPath = `${databaseName}${formattedPartition}/${bucketFilename}`

          return disklet
            .getText(bucketPath)
            .then((rawBucketData: string) => JSON.parse(rawBucketData))
            .then((bucketData: any[]) => {
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
                    return disklet
                      .setText(bucketPath, JSON.stringify(bucketData))
                      .then(() => removedData)
                  })
                } else {
                  return bucketData[targetIndex.index]
                }
              }
            })
        })
      }

      const fns: RangeBase = {
        insert(partition: string, data: any): Promise<unknown> {
          const { bucketSize, rangeKey, idKey } = configData
          const formattedPartition = checkAndformatPartition(partition)
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
            const bucketFilename = `${bucketNumber}.json`
            const bucketPath = `${databaseName}${formattedPartition}/${bucketFilename}`

            return (
              disklet
                .getText(bucketPath)
                .then(
                  serializedBucket => {
                    const bucketData: any[] = JSON.parse(serializedBucket)
                    const firstRangeOccurence = getIndex(
                      data[rangeKey],
                      rangeKey,
                      bucketData
                    )
                    if (firstRangeOccurence.found === false) {
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
                    return disklet.setText(
                      bucketPath,
                      JSON.stringify(bucketData)
                    )
                  },
                  () => {
                    // assuming bucket doesnt exist
                    return disklet.setText(bucketPath, JSON.stringify([data]))
                  }
                )
                // Save the id
                .then(() => idDb.insert(partition, data[idKey], data[rangeKey]))
            )
          })
        },
        query(
          partition: string = '/',
          rangeStart: number,
          rangeEnd: number = rangeStart
        ): Promise<any[]> {
          const { bucketSize, rangeKey } = configData
          const formattedPartition = checkAndformatPartition(partition)
          const bucketFetchers: any[] = []

          if (rangeEnd < rangeStart) {
            throw new Error('rangeStart must be larger than rangeEnd')
          }

          for (
            let bucketNumber = Math.floor(rangeStart / bucketSize);
            bucketNumber <= Math.floor(rangeEnd / bucketSize);
            bucketNumber++
          ) {
            bucketFetchers.push(
              disklet
                .getText(
                  `${databaseName}${formattedPartition}/${bucketNumber}.json`
                )
                .then(
                  rawBucketData => JSON.parse(rawBucketData),
                  () => []
                )
            )
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
  const configData: RangeBaseConfig = {
    type: BaseType.RangeBase,
    bucketSize: Math.floor(bucketSize),
    idDatabaseName: `${databaseName}_ids`,
    rangeKey,
    idKey,
    idPrefixLength,
  }

  return doesDatabaseExist(disklet, databaseName).then(databaseExists => {
    if (databaseExists) {
      throw new Error(`database ${databaseName} already exists`)
    }

    return createHashBase(
      disklet,
      configData.idDatabaseName,
      idPrefixLength
    ).then(idHashBase => {
      return disklet
        .setText(`${databaseName}/config.json`, JSON.stringify(configData))
        .then(() => openRangeBase(disklet, databaseName))
    })
  })
}
