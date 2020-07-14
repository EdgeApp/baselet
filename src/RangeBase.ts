// import { Disklet } from 'disklet'
//
// import { checkAndformatPartition } from './helpers'
import { BaseletConfig } from './types'

export interface RangeBase {
  insert(partition: string, data: any): Promise<unknown>
  query(partition: string, rangeStart: number, rangeEnd: number): Promise<any[]>
  queryById(partition: string, rangeKey: number, idKey: string): Promise<any>
}

interface RangeBaseData {
  [someKey: string]: number
}

interface RangeBaseConfig extends BaseletConfig {
  bucketSize: number
  rangeKey: string
  idKey: string
}

// export function openRangeBase(
//   disklet: Disklet,
//   databaseName: string
// ): Promise<RangeBase> {
//   // check that the db exists and is of type RangeBase
//
//   function getConfig(): Promise<RangeBaseConfig> {
//     return disklet
//       .getText(`${databaseName}/config.json`)
//       .then(serializedConfig => JSON.parse(serializedConfig))
//   }
//
//   function getDestinationIndex(
//     input: number | string,
//     keyName: string,
//     bucket: object[]
//   ): object {
//     let minIndex: number = 0
//     let maxIndex: number = bucket.length - 1
//     let currentIndex: number
//     let currentElement
//
//     while (minIndex <= maxIndex) {
//       currentIndex = ~~((minIndex + maxIndex) / 2)
//       currentElement = bucket[currentIndex][keyName]
//
//       if (currentElement < input[keyName]) {
//         minIndex = currentIndex + 1
//       } else if (currentElement > input[keyName]) {
//         maxIndex = currentIndex - 1
//       } else {
//         return {
//           found: true,
//           index: currentIndex
//         }
//       }
//     }
//
//     return {
//       found: false,
//       index: currentElement < input[keyName] ? currentIndex + 1 : currentIndex
//     }
//   }
//
//   return getConfig().then(configData => {
//     return {
//       insert(partition: string, data: any): Promise<unknown> {
//         const { bucketSize, rangeKey, idKey } = configData
//         const formattedPartition = checkAndformatPartition(partition)
//
//         if (
//           !(
//             Object.prototype.hasOwnProperty.call(data, rangeKey) &&
//             Object.prototype.hasOwnProperty.call(data, idKey)
//           )
//         ) {
//           return Promise.reject(
//             new Error(`data must have properties ${rangeKey} and ${idKey}`)
//           )
//         }
//         const dataRange = Number(data[rangeKey])
//         const dataId = data[idKey]
//         const bucketNumber = dataRange / bucketSize
//         const bucketFilename = `${bucketNumber}.json`
//         const bucketPath = `${databaseName}${formattedPartition}/${bucketFilename}`
//
//         disklet
//           .getText(bucketPath)
//           .then(serializedBucket => JSON.parse(serializedBucket))
//           .then(bucketData => {
//             const targetRangeIndex = getDestinationIndex(
//               data,
//               rangeKey,
//               bucketData
//             )
//             if (targetRangeIndex.found === true) {
//               const targetIdIndex = getDestinationIndex(
//                 data,
//                 idKey,
//                 bucketData[targetRangeIndex.index]
//               )
//               if (targetIdIndex.found === true) {
//                 throw new Error('Cannot insert data because id already exists')
//               } else {
//                 bucketData[targetRangeIndex.index].items.splice(
//                   targetIdIndex.index,
//                   0,
//                   data
//                 )
//               }
//             } else {
//               bucketData.splice(targetRangeIndex.index, 0, {
//                 [rangeKey]: data.rangeKey,
//                 items: [data]
//               })
//             }
//             return disklet.setText(bucketPath, JSON.stringify(bucketData))
//           })
//           .catch(error => {
//             console.log(error)
//             // we must be sure bucket doesnt exist, else we'll overwrite
//             // we have to identify the error
//             const bucketData = [
//               {
//                 [rangeKey]: data[rangeKey],
//                 items: [data]
//               }
//             ]
//             return disklet.setText(bucketPath, JSON.stringify(bucketData))
//           })
//       },
//       query(
//         partition: string = '/',
//         rangeStart: number,
//         rangeEnd: number
//       ): Promise<any[]> {
//         const formattedPartition = checkAndformatPartition(partition)
//         // sanity check the range
//         const bucketFetchers = []
//         for (
//           let bucketNumber = Math.floor(rangeStart / configData.bucketSize);
//           bucketNumber <= Math.floor(rangeEnd / configData.bucketSize);
//           bucketNumber++
//         ) {
//           bucketFetchers.push(
//             disklet
//               .getText(
//                 `${databaseName}${formattedPartition}/${bucketNumber}.json`
//               )
//               .then(rawBucketData => JSON.parse(rawBucketData))
//               .catch(error => {
//                 console.log(
//                   `Error getting data from bucket ${bucketNumber}. ${error}`
//                 )
//                 return []
//               })
//           )
//         }
//         return Promise.all(bucketFetchers).then(bucketList => {
//           const queryResults: any[] = []
//           for (let i = rangeStart; i <= rangeEnd; i++) {
//             const bucketNumber = Math.floor(i / configData.bucketSize)
//             const dataIndex = i % configData.bucketSize
//             const fetchedBucketNumber =
//               bucketNumber - Math.floor(rangeStart / configData.bucketSize)
//             queryResults.push(bucketList[fetchedBucketNumber][dataIndex])
//           }
//           return queryResults
//         })
//       },
//       queryById(
//         partition: string,
//         rangeKey: number,
//         idKey: string
//       ): Promise<any> {
//         return Promise.resolve()
//       }
//     }
//   })
// }
//
// export function createRangeBase(
//   disklet: Disklet,
//   databaseName: string,
//   bucketSize: number,
//   rangeKey: string,
//   idKey: string
// ): Promise<RangeBase> {
//   // check that databaseName only contains letters, numbers, and underscores
//   // check if database already exists
//
//   const configData: RangeBaseConfig = {
//     type: BaseType.RangeBase,
//     bucketSize,
//     rangeKey,
//     idKey
//   }
//   return disklet
//     .setText(`${databaseName}/config.json`, JSON.stringify(configData))
//     .then(() => openRangeBase(disklet, databaseName))
// }
