import { Disklet } from 'disklet'

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
  queryById(partition: string, rangeKey: number, idKey: string): Promise<any>
}

export interface RangeBaseData {
  [someKey: string]: any
}

interface RangeBaseConfig extends BaseletConfig {
  bucketSize: number
  rangeKey: string
  idKey: string
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

    return {
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
        const bucketNumber = Math.floor(data[rangeKey] / bucketSize)
        const bucketFilename = `${bucketNumber}.json`
        const bucketPath = `${databaseName}${formattedPartition}/${bucketFilename}`

        return disklet.getText(bucketPath).then(
          serializedBucket => {
            const bucketData = JSON.parse(serializedBucket)
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
              if (targetIndex.found === true) {
                throw new Error('Cannot insert data because id already exists')
              }
              bucketData.splice(targetIndex.index, 0, data)
            }
            return disklet.setText(bucketPath, JSON.stringify(bucketData))
          },
          () => {
            // assuming bucket doesnt exist
            return disklet.setText(bucketPath, JSON.stringify([data]))
          }
        )
      },
      query(
        partition: string = '/',
        rangeStart: number,
        rangeEnd: number = rangeStart
      ): Promise<any[]> {
        const { bucketSize, rangeKey } = configData
        const formattedPartition = checkAndformatPartition(partition)
        const bucketFetchers = []

        if (
          !Number.isInteger(rangeStart) ||
          !Number.isInteger(rangeEnd) ||
          rangeEnd < rangeStart
        ) {
          throw new Error(
            'range values must be integers and rangeEnd >= rangeStart'
          )
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
          for (let i = 0; i < bucketList.length; i++) {
            if (i === bucketList.length - 1) {
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
            } else if (i === 0) {
              // is first bucket
              const firstRangeIndex = getIndex(
                rangeStart,
                rangeKey,
                bucketList[i]
              )
              queryResults = bucketList[i].slice(firstRangeIndex.index)
            } else {
              // is bucket in between range values
              Array.prototype.push.apply(queryResults, bucketList[i])
            }
          }
          return queryResults
        })
      },
      queryById(partition: string, range: number, id: string): Promise<any> {
        const { bucketSize, rangeKey, idKey } = configData
        const formattedPartition = checkAndformatPartition(partition)
        const bucketNumber = Math.floor(range / bucketSize)
        const bucketFilename = `${bucketNumber}.json`
        const bucketPath = `${databaseName}${formattedPartition}/${bucketFilename}`

        return disklet
          .getText(bucketPath)
          .then(rawBucketData => JSON.parse(rawBucketData))
          .then(bucketData => {
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
            if (targetIndex.found === false) {
              throw new Error(`Data with id ${id} not found.`)
            }
            return Promise.resolve(bucketData[targetIndex.index])
          })
      }
    }
  })
}

export function createRangeBase(
  disklet: Disklet,
  databaseName: string,
  bucketSize: number,
  rangeKey: string,
  idKey: string
): Promise<RangeBase> {
  if (!isPositiveInteger(bucketSize)) {
    throw new Error(`bucketSize must be a number greater than 0`)
  }
  databaseName = checkDatabaseName(databaseName)
  const configData: RangeBaseConfig = {
    type: BaseType.RangeBase,
    bucketSize: Math.floor(bucketSize),
    rangeKey,
    idKey
  }

  return doesDatabaseExist(disklet, databaseName).then(databaseExists => {
    if (databaseExists) {
      throw new Error(`database ${databaseName} already exists`)
    }
    return disklet
      .setText(`${databaseName}/config.json`, JSON.stringify(configData))
      .then(() => openRangeBase(disklet, databaseName))
  })
}
