import { Disklet } from 'disklet'
import {
  BaseTypes
} from './types'

export interface CountBase {
  insert(partition: string, index: number, data: any): Promise<unknown>
  query(partition: string, range: number): Promise<any>
  length(): number
}

export function createCountBase(disklet: Disklet, databaseName: string, bucketSize: number): CountBase {
  // check that databaseName only contains letters, numbers, and underscores
  // check if database already exists

  // check that bucketSize is a positive Integer

  // create config file at databaseName/config.json
  const configData = {
    type: BaseTypes.COUNT_BASE,
    bucketSize,
    length: 0,
    partitions: {
      '/': {
        length: 0
      }
    }
  }
  disklet.setText(`${databaseName}/config.json`, JSON.stringify(configData))

  return {
    insert(partition: string = '/', index: number, data: any): Promise<unknown> {
      // check that partition only contains letters, numbers, and underscores
      // if no partition, then root

      let partitionMetadata = configData.partitions[partition] || {
        length: 0
      }
      const nextIndex = partitionMetadata.length;
      if (!Number.isNaN(index) || index < 0) {
        return Promise.reject(new Error('index must be a Number greater than 0'))
      }
      if (index > nextIndex) {
        return Promise.reject(new Error('index is larger than next index in partition'))
      }
      if (index === nextIndex) {
        ++partitionMetadata.length;
      }
      let bucketNumber = index / bucketSize;
      const bucketExists = bucketNumber * bucketSize < nextIndex;
      let bucketData = [data];
      if (bucketExists) {
        return disklet.getText(
          `${databaseName}/${partition}/${currentBucket}.json`
        )
          .then(currentBucketData => {
            const bucketData = JSON.parse(currentBucketData)
            bucketData[index] = data
            return disklet.setText(
              `${databaseName}/${partition}/${currentBucket}.json`,
              JSON.stringify(bucketData)
            ).then(() => {
              configData.partitions[partition] = partitionMetadata;
              return disklet.setText(
                `${databaseName}/config.json`,
                JSON.stringify(configData)
              )
            }).catch(error => {

            })
          })
          .catch(error => {

          })
      } else {
        return disklet.setText(
          `${databaseName}/${partition}/${currentBucket}.json`,
          JSON.stringify(bucketData)
        ).then(() => {
          configData.partitions[partition] = partitionMetadata;
          return disklet.setText(
            `${databaseName}/config.json`,
            JSON.stringify(configData)
          )
        }).catch(error => {

        })
      }
    },
    query(partition: string, range: number): Promise<any> {
      
    },
    length(): number {

    }
  }
}
