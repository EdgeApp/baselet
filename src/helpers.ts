import { Disklet } from 'disklet'
import { makeMemlet, Memlet } from 'memlet'

import { BaseletConfig } from './types'

export function isMemlet(object: Disklet | Memlet): object is Memlet {
  return (
    Object.prototype.hasOwnProperty.call(object, 'getJson') ||
    Object.prototype.hasOwnProperty.call(object, 'setJson')
  )
}

export function getOrMakeMemlet(storage: Disklet | Memlet): Memlet {
  return isMemlet(storage) ? storage : makeMemlet(storage)
}

export function getPartitionPath(
  databaseName: string,
  partition: string
): string {
  return `${databaseName}/${checkAndFormatPartition(partition)}`
}

export function getBucketPath(
  databaseName: string,
  partition: string,
  bucketName: string | number
): string {
  return `${getPartitionPath(databaseName, partition)}/${bucketName}.json`
}

export function getConfigPath(databaseName: string): string {
  return `${databaseName}/config.json`
}

export function getConfig<T extends BaseletConfig>(
  disklet: Disklet,
  databaseName: string
): Promise<T> {
  return disklet.getText(getConfigPath(databaseName)).then(JSON.parse)
}

export async function setConfig(
  disklet: Disklet,
  databaseName: string,
  config: BaseletConfig
): Promise<void> {
  await disklet.setText(getConfigPath(databaseName), JSON.stringify(config))
}

export function checkAndFormatPartition(partition: string = ''): string {
  const validExpression = /^[a-z0-9_]*$/i
  const fPartition = partition.trim()
  if (!validExpression.test(fPartition)) {
    throw new Error(
      'partitions may only contain alphanumeric and underscore characters'
    )
  }
  return fPartition
}

export function checkDatabaseName(databaseName: string): string {
  if (databaseName === '') {
    throw new Error('database name cannot be empty')
  }
  const validExpression = /^[a-z0-9_]*$/i
  const fDatabase = databaseName.trim()
  if (!validExpression.test(fDatabase)) {
    throw new Error(
      'database name may only contain alphanumeric and underscore characters'
    )
  }
  return fDatabase
}

export function doesDatabaseExist(
  disklet: Disklet,
  name: string
): Promise<boolean> {
  return disklet
    .list('./')
    .then(existingFiles => existingFiles[name] === 'folder')
}

export function isPositiveInteger(num: number): boolean {
  return Number.isInteger(num) && num >= 1
}
