// import { Disklet } from 'disklet'

import { BaseletConfig } from './types'

export interface RangeBase {
  insert(partition: string, data: any): Promise<unknown>
  query(partition: string, rangeStart: number, rangeEnd: number): Promise<any[]>
  queryByte(partition: string, range: number, id: string): Promise<any[]>
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
//   return getConfig().then(configData => {
//     return {
//       insert(
//         partition: string = '/',
//         data: any
//       ): Promise<unknown> {
//         // check that partition only contains letters, numbers, and underscores
//         // if no partition, then root
//         const { bucketSize, rangeKey, idKey } = configData
//
//         if (!(data.hasOwnProperty(rangeKey) && data.hasOwnProperty(idKey))) {
//           return Promise.reject(
//             new Error(`data must have properties ${rangeKey} and ${idKey}`)
//           )
//         }
//       },
//       query(partition: string = '/', rangeStart: number, rangeEnd: number): Promise<any[]> {
//         // check that partition only contains letters, numbers, and underscores
//         // and that it exists
//
//       },
//       queryByte(partition: string = '/', range: number, id: string): Promise<any[]> {
//
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
//   // check that prefixSize is a positive Integer
//
//   // create config file at databaseName/config.json
//   const configData: RangeBaseConfig = {
//     type: BaseType.RANGE_BASE,
//     prefixSize
//   }
//   return disklet
//     .setText(`${databaseName}/config.json`, JSON.stringify(configData))
//     .then(() => openRangeBase(disklet, databaseName))
// }
