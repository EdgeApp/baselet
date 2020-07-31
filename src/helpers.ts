import { Disklet } from 'disklet'

export function checkAndformatPartition(partition: string = ''): string {
  if (typeof partition !== 'string') {
    throw new TypeError('partition must be of type string')
  }
  const validExpression = /^[a-z0-9_]*$/i
  const fPartition = partition.trim()
  if (!validExpression.test(fPartition)) {
    throw new Error(
      'partions may only contain alphanumeric and underscore characters'
    )
  }
  return fPartition === '' ? fPartition : `/${fPartition}`
}

export function checkDatabaseName(databaseName: string): string {
  if (typeof databaseName !== 'string' || databaseName === '') {
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
    .then(existingFiles => existingFiles[`${name}`] === 'folder')
}

export function isPositiveInteger(num: number): boolean {
  return Number.isInteger(num) && num >= 1
}
