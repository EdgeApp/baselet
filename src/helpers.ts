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

export async function getConfig<T extends BaseletConfig>(
  memlet: Memlet,
  databaseName: string
): Promise<T> {
  return await memlet.getJson(getConfigPath(databaseName))
}

export async function setConfig(
  memlet: Memlet,
  databaseName: string,
  config: BaseletConfig
): Promise<void> {
  await memlet.setJson(getConfigPath(databaseName), config)
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

export async function doesDatabaseExist(
  memlet: Memlet,
  name: string
): Promise<boolean> {
  return await memlet
    .list('./')
    .then(existingFiles => existingFiles[name] === 'folder')
}

export function isPositiveInteger(num: number): boolean {
  return Number.isInteger(num) && num >= 1
}
