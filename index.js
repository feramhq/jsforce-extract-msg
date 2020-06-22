const jsforce = require('jsforce')
const { oneLine, oneLineTrim } = require('common-tags')
const fs = require('fs')
const fetch = require("node-fetch")
const msg2txt = require('msg2txt')
const path = require('path')

module.exports = {
  main,
}

const logNames = {
  missingVersionData: `missingVersionData.txt`,
  requestErrors: `requestErrors.txt`,
  convertErrors: 'convertErros.txt',
  fetchErrors: 'fetchErrors.txt',
  uploadErrors: 'uploadErrors.txt',
  createLinkErrors: 'createLinkErrors.txt',
  undoErrors: 'undoErrors.txt',
  updateErrors: 'updateErrors.txt',
  getTimestampErrors: 'getTimestampErros.txt',
}
const today = new Date();
const dateOpts = {
  timeZone: 'Europe/Berlin',
  hour12: false
}
const timestamp =
  today.toLocaleDateString('en-CA', dateOpts) + ' ' +
  today.toLocaleTimeString('de', dateOpts)

const logdir = `log_${timestamp}`

Object.entries(logNames).map(([key, value]) => {
  logNames[key] = `./${logdir}/${value}`
})

const filedir = './files'

async function downloadDocument(connection, serverFilename, localFilename) {
  const headers = {
    'Authorization': 'Bearer ' + connection.accessToken,
    'Content-Type': 'application/octet-stream',
  }
  const options = {
    method: 'GET',
    headers: headers,
  }
  return new Promise((resolve, reject) => {
    fetch(connection.instanceUrl + serverFilename, options)
      .then(result => {
        if (!result.ok) {
          reject(new Error(result.statusText))
        }
        result.body.pipe(fs.createWriteStream(localFilename))
          .on('close', () => resolve())
      })
      .catch(error => {
        reject(error)
      })
  })
    .catch(error => {
      fs.appendFileSync(
        logNames.fetchErrors,
        `${localFilename}: ${error}\n`,
      )
      throw error
    })
}

async function uploadDocument(connection, localFilename, timestamp, msgId) {

  const file = fs.readFileSync(localFilename)
  const versionRecord = {
    Title: timestamp +
      ' - ' +
      path.basename(localFilename, path.extname(localFilename)),
    Description: JSON.stringify({ msgId: msgId }),
    PathOnClient: localFilename,
    VersionData: file.toString('base64'),
  }
  const headers = {
    'Authorization': 'Bearer ' + connection.accessToken,
    'Content-Type': 'application/json',
  }
  const options = {
    method: 'POST',
    headers: headers,
    body: JSON.stringify(versionRecord),
  }
  const uploadPath = `/services/data/v${connection.version}/sobjects/ContentVersion/`

  return await fetch(connection.instanceUrl + uploadPath, options)
    .then(result => {
      if (!result.ok) {
        throw result.statusText
      }
      return result.json()
    })
    .then(result => {
      return result.id
    })
    .catch(error => {
      fs.appendFileSync(
        logNames.uploadErrors,
        `${localFilename}: ${JSON.stringify(error)}\n`
      )
      throw error
    })
}

async function createDocumentLinkRecords(
  connection,
  currentUserId,
  msgFileId,
  newVersionRecordId,
) {
  const linksQuery = oneLine`
    SELECT LinkedEntityId, ShareType, Visibility
    FROM ContentDocumentLink
    WHERE ContentDocumentId = '${msgFileId}'
  `
  const documentIdQuery = oneLine`
    SELECT ContentDocumentId
    FROM ContentVersion
    WHERE Id = '${newVersionRecordId}'
  `
  try{
    const documentId = await connection.queryAll(documentIdQuery)
      .then(result => result.records[0].ContentDocumentId)
    const records = await connection.queryAll(linksQuery)
      .then(result => result.records)
    const newLinkRecords = records
      .filter(record => {
        return currentUserId !== record.LinkedEntityId
      })
      .map(record => {
        return {
          ContentDocumentId: documentId,
          LinkedEntityId: record.LinkedEntityId,
          ShareType: record.ShareType,
          Visibility: record.Visibility,
        }
      })

    await connection.create('ContentDocumentLink', newLinkRecords)
  }
  catch(error) {
    fs.appendFileSync(
      logNames.createLinkErrors,
      oneLine`
        msgId: ${msgFileId},
        VersionId: ${newVersionRecordId},
        error: ${JSON.stringify(error, null, 2)}\n
      `
    )
    throw error
  }
}

async function getMsgFiles (connection, fileLimit) {
  let query = oneLine`
    SELECT
      Id,
      Title,
      FileExtension,
      LatestPublishedVersion.VersionData,
      LatestPublishedVersion.Id
    FROM ContentDocument
    WHERE
      FileExtension = 'msg' AND
      (NOT Title LIKE 'backup_%') AND
      (NOT Title LIKE '%_backup')
    `
  if (!fileLimit === false) {
    query += ' LIMIT 30'
  }
  return await connection.queryAll(query)
    .then(result => {
      return result.records
        .filter(versionDataExists)
    })
}

function versionDataExists(record) {
  if (
    !record.LatestPublishedVersion ||
    !record.LatestPublishedVersion.VersionData
  ) {
    fs.appendFileSync(
      logNames.missingVersionData,
      `${record.Id}\t${record.Title}\n`,
    )
    return false
  }
  return true
}

function extractMsgFile(localFilename) {
  try {
    msg2txt(localFilename)
  }
  catch (error) {
    fs.appendFileSync(
      logNames.convertErrors,
      `${localFilename}: ${error}\n`,
    )
    throw error
  }
}

async function getDocumentsWithVersionIds(connection, versionIds) {
  const query = oneLine`
    SELECT ContentDocumentId
    FROM ContentVersion
    WHERE Id IN ${toSOQLList(versionIds)}
  `
  return connection.queryAll(query)
    .then(result => {
      return result.records.map(value => {
        return value.ContentDocumentId
      })
    })
}

async function deleteDocumentsWithVersionIds(connection, versionIds) {
  const documentIds = await getDocumentsWithVersionIds(connection, versionIds)

  await connection.destroy('ContentDocument', documentIds)
}

function toSOQLList(list) {
  return "('" + list.join("', '") + "')"
}

function extractMailTimestamp (msgId) {
  try {
    const timestamp = fs.readFileSync(`${filedir}/${msgId}/Email.txt`)
      .toString()
      .split('\n')
      .find(line => line.match(/^(Date|Datum): /))
      .replace(/^(Date|Datum): (.*)\r?$/, '$2')
      .trim()
    if (!timestamp) {
      throw new Error(`Could't retrieve timestamp from mail`)
    }
    return timestamp
  }
  catch(error) {
    fs.appendFileSync(
        logNames.getTimestampErrors,
        `${msgId}: ${JSON.stringify(error, null, 2)}\n`,
      )
    throw error
  }
}

async function buildMsgDescription(connection, versionIds) {
  const msgIds = await connection.retrieve('ContentVersion', versionIds)
    .then(versionRecords => versionRecords.map(record => {
      return record.ContentDocumentId
    }))
  return JSON.stringify({ includedFiles: msgIds })
}

async function main(credentials, fileLimit) {

  if (!(fileLimit === false)) {
    fileLimit = true
  }

  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  fs.mkdirSync(logdir)

  try {
    fs.rmdirSync(filedir, {recursive: true})
    fs.mkdirSync(filedir)
  }
  catch(error) {
    if (error.errno === -2) fs.mkdirSync(filedir) // Should never execute
    if (error.errno !== -17) {
      throw new Error('Uknown error while creating the log dir')
    }
  }

  const userId = await connection
    .login(credentials.user, credentials.pw + credentials.token)
    .then(loginInformation => loginInformation.id)

  const msgFileRecords = await getMsgFiles(connection, fileLimit)
  if (msgFileRecords.length === 0) {
    console.info('There are no msg files to extract.')
    return
  }
  for (let msgFileRecord of msgFileRecords) {
    const localFilename = oneLineTrim`
        ${filedir}/${msgFileRecord.Id}
        .${msgFileRecord.FileExtension}
      `
    const serverFilename = msgFileRecord.LatestPublishedVersion.VersionData
    const extractionDir = `${filedir}/${msgFileRecord.Id}`
    let timestamp
    try {
      console.info(`downloading ${msgFileRecord.Id}`)
      await downloadDocument(connection, serverFilename, localFilename)
      console.info(`extracting ${msgFileRecord.Id}`)
      await extractMsgFile(localFilename)
      timestamp = extractMailTimestamp(msgFileRecord.Id)
    }
    catch(error) {
      continue
    }
    const uploadedVersionIds = []
    try{
      for (const i of fs.readdirSync(extractionDir)) {
        const newFilename = extractionDir + `/${i}`
        console.info(`>>> uploading ${newFilename}`)
        const newVersionId = await uploadDocument(
            connection,
            newFilename,
            timestamp,
            msgFileRecord.Id,
          )
        uploadedVersionIds.push(newVersionId)
        console.info(`>>> create Links for ${newFilename}`)
        await createDocumentLinkRecords(
            connection,
            userId,
            msgFileRecord.Id,
            newVersionId,
          )
      }
      console.info(`updating ${msgFileRecord.Id}`)
      const description = await buildMsgDescription(connection, uploadedVersionIds)

      await connection.update(
          'ContentDocument',
          {
            Id: msgFileRecord.Id,
            Title: `${timestamp} - ${msgFileRecord.Title}_backup`,
            Description: description,
          }
        )
      try {
        console.info('deleting local files')
        fs.unlinkSync(localFilename)
        fs.rmdirSync(extractionDir, {recursive: true})
      }
      catch(error) {
        console.error(error)
      }
    }
    catch(error) {
      console.info('!!! error while creating documents')
      if (uploadedVersionIds.length > 0) {
        try{
          await deleteDocumentsWithVersionIds(connection, uploadedVersionIds)
          console.info('!!! cleaned up everything!')
        }
        catch(error) {
          console.info(oneLine`
            Major Error! Couldn't clean up after error while uploading Files!
          `)
          throw error
        }
      }
    }
    finally {
      console.info('\n')
    }
  }

  //--------------------------------------------------------------------------//
}

async function undoMyWork(credentials) {

  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  Object.entries(logNames).map(([_, value]) => {
    fs.unlink(value, () => {})
  })

  await connection.login(credentials.user, credentials.pw + credentials.token)
  const msgFilesQuery = oneLine`
    SELECT Id, Title, Description
    FROM ContentDocument
    WHERE FileExtension = 'msg' AND
      Title LIKE '%_backup'
  `
  const records = await connection.queryAll(msgFilesQuery)
    .then(result => {
      return result.records
    })
  for (let record of records) {

    console.info(`Deleting files of ${record.Id}`)
    try{
      const filesToDelete = JSON.parse(record.Description)
      console.info(`Ids of files to delete:`)
      console.info(filesToDelete)
      connection.destroy('ContentDocument', filesToDelete)
      console.info(`Updating ${record.Id}`)
      await connection.update(
        'ContentDocument',
        {
          Id: record.Id,
          Title: record.Title.slice(7),
          Description: '',
        }
      )
    }
    catch(error) {
      console.error(error)
      fs.appendFileSync(
        logNames.undoErrors,
        `${record.id}\n${filesToDelete}\n${error}\n\n`,
      )
      continue
    }
    console.info()
  }

}

async function hardDelete(credentials) {
  // do NOT use this function! Only for debugging in the sandbox database!
  // This will delete ANY File without a .msg extension!

  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  fs.mkdirSync(logdir)

  await connection.login(credentials.user, credentials.pw + credentials.token)
  const nonMsgQuery = oneLine`
    SELECT Id
    FROM ContentDocument
    WHERE FileExtension != 'msg'
  `
  const msgQuery = oneLine`
    SELECT Id, Title
    FROM ContentDocument
    WHERE FileExtension = 'msg' AND
      Title LIKE 'backup_%'
  `
  const nonMsgFiles = await connection.queryAll(nonMsgQuery)
    .then(result => result.records.map(value => value.Id))
  const msgFiles = await connection.queryAll(msgQuery)
    .then(result => {
      result.records.map(record => {
        record.Title = record.Title.slice(7)
      })
      return result.records
    })

  await connection.destroy('ContentDocument', nonMsgFiles)
  await connection.update('ContentDocument', msgFiles)
}

async function justUpload(credentials, dir) {
  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  fs.mkdirSync(logdir)

  const userId = await connection
    .login(credentials.user, credentials.pw + credentials.token)
    .then(loginInformation => loginInformation.id)

  const ids = fs.readdirSync('files')
  const msgRecords = await connection.retrieve('ContentDocument', ids)
  for (const msgRecord of msgRecords) {
    console.info(`uploading for: ${msgRecord.Id}`)
    const uploadedVersionIds = []
    const extractionDir = 'files/' + msgRecord.Id
    try{
      for (const i of fs.readdirSync(extractionDir)) {
        const newFilename = extractionDir + `/${i}`
        console.info(`>>> uploading ${newFilename}`)
        const newVersionId = await uploadDocument(connection, newFilename)
        uploadedVersionIds.push(newVersionId)
        console.info(`>>> create Links for ${newFilename}`)
        await createDocumentLinkRecords(connection, userId, msgRecord.Id, newVersionId)
      }
      console.info(`updating ${msgRecord.Id}`)
      const description = JSON.stringify(await getDocumentsWithVersionIds(
          connection,
          uploadedVersionIds,
        ))

      await connection.update(
        'ContentDocument',
        {
          Id: msgRecord.Id,
          Title: 'backup_' + msgRecord.Title,
          Description: description
        }
      )
      fs.rmdirSync(extractionDir, {recursive: true})
    }
    catch(error) {
      console.info('!!! error while creating documents')
      console.error(error)
      if (uploadedVersionIds.length > 0) {
        try{
          await deleteDocumentsWithVersionIds(connection, uploadedVersionIds)
          console.info('!!! cleaned up everything!')
        }
        catch(error) {
          console.info(oneLine`
            Major Error! Couldn't clean up after error while uploading Files!
          `)
          console.error(error)
          throw error
        }
      }
    }
    finally {
      console.info('\n')
    }
  }
}

async function updateMails(credentials) {

  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  fs.mkdirSync(logdir)

  await connection
    .login(credentials.user, credentials.pw + credentials.token)

  const msgRecords = await connection.queryAll(oneLine`
    SELECT Id, Description, Title
    FROM ContentDocument
    WHERE FileExtension = 'msg' AND
      Title LIKE 'backup_%'
  `)
    .then(result => result.records)

  for (const msgRecord of msgRecords) {
    try {
      const parseDescription = JSON.parse(msgRecord.Description)
      let fileIds
      if (parseDescription.includedFiles) {
        fileIds = parseDescription.includedFiles
      }
      else {
        fileIds = parseDescription
      }
      const fileRecords = await connection.retrieve('ContentDocument',fileIds)
      if (fileRecords.some(value => !value)) {
        throw new Error('Some file records are null')
      }
      const nachrichtRecord = fileRecords.find(value => value.Title === 'Email')
      if (!nachrichtRecord) {
        throw new Error (`${msgRecord.Id}: No 'nachricht.txt found`)
      }
      const versionData = await connection.retrieve(
          'ContentVersion',
          nachrichtRecord.LatestPublishedVersionId,
        )
        .then(result => result.VersionData)

      const nachrichtData = await connection.request(versionData)
      const nachrichtEntries =
        Object.fromEntries(
          nachrichtData
            .split('\n')
            .map(line => line.split(': '))
        )
      const nachrichtDate = nachrichtEntries.Datum || nachrichtEntries.Date
      if (!nachrichtDate) {
        throw new Error('Couldn\'t extract date from txt file')
      }
      const updateRecords = []
      for (const fileRecord of fileRecords) {
        updateRecords.push({
          Id: fileRecord.Id,
          Title: `${nachrichtDate} - ${fileRecord.Title.replace('nachricht', 'Email')}`
        })
      }
      await connection.update('ContentDocument',updateRecords)
      const description = JSON.stringify({ includedFiles: fileIds })
      await connection.update('ContentDocument', {
        Id: msgRecord.Id,
        Title: nachrichtDate + ' - ' + msgRecord.Title.slice(7) + '_backup',
        Description: description,
      })
    }
    catch(error) {
      console.error(error)
      fs.appendFileSync(
        logNames.updateErrors,
        `${msgRecord.Id}: ${error}\n`)
    }
  }

  await connection.logout()
}

async function reuploadMessage(credentials) {

  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  fs.mkdirSync(logdir)

  const userId = await connection
    .login(credentials.user, credentials.pw + credentials.token)
    .then(loginInformation => loginInformation.id)

  const msgRecords = await connection.queryAll(oneLine`
    SELECT Id, Description, Title
    FROM ContentDocument
    WHERE FileExtension = 'msg' AND
      Title LIKE 'backup_%'
  `)
    .then(result => result.records)

  for (const msgRecord of msgRecords) {

      console.info(msgRecord.Id)
      const fileIds = JSON.parse(msgRecord.Description)
      if (fileIds.includedFiles) {
        continue
      }
      const fileRecords = await connection.retrieve('ContentDocument',fileIds)
      if (fileRecords.some(value => !value)) {
        // throw new Error('Some file records are null')
        continue
      }
      const messageRecord = fileRecords.find(value => value.Title === 'message')
      if (!messageRecord) {
        // throw new Error (`${msgRecord.Id}: No 'message.text found`)
        continue
      }
      const ogTextId = messageRecord.Id

      let newDocumentId
      let newDescriptionList
      try {
        fs.appendFileSync(
          logdir + '/prevDesc.txt',
          `${msgRecord.Id}: ${msgRecord.Description}`)
        console.info('>>> get version')
        const version = await connection.retrieve('ContentVersion', messageRecord.LatestPublishedVersionId)
        console.info('>>> get version data')
        const messageData = await connection.request(version.VersionData)
        const headerDate = Object.fromEntries(messageData
          .split('\r\n')
          .map(line => line.split(': '))).Date
        const messageDate = getTimestamp(headerDate)
        const newData = messageData.replace(/Date: .*/, `Date: ${messageDate}`)
        const filename = 'nachricht.txt'
        fs.writeFileSync(filename, newData)
        console.info('>>> uploading')
        const newVersionId = await uploadDocument(connection, filename)
        console.info('>>> get new document id')
        newDocumentId = await connection.retrieve('ContentVersion', newVersionId)
          .then(result => result.ContentDocumentId)
        console.info('>>> create links')
        await createDocumentLinkRecords(connection, userId, msgRecord.Id, newVersionId)
        newDescriptionList = fileIds
          .filter(id => id !== ogTextId)
        newDescriptionList.push(newDocumentId)
        const newDescription = { includedFiles: newDescriptionList}
        console.info('>>> update msg record')
        await connection.update('ContentDocument', {
          Id: msgRecord.Id,
          Description: JSON.stringify(newDescription),
        })
      }
      catch(error) {
        console.error(error)
        fs.appendFileSync(
          logNames.updateErrors,
          `${JSON.stringify(error, null, 2)}\n`
        )
        if (newDocumentId) {
          connection.destroy('ContentDocument', newDocumentId)
        }
        if (newDescriptionList) {
          await connection.update('ContentDocument', {
            Id: msgRecord.Id,
            Description: msgRecord.Description,
          })
        }
      }

  }

  await connection.logout()
}

function getTimestamp(dateString) {
  function getNumericOffset (zone, now) {
    const tzNum = Intl
      .DateTimeFormat('en-US', {
        timeZone: zone,
        timeZoneName: 'short',
      })
      .format(now)
      .split(', ')[1]
      .replace('GMT', '')

    const timeZoneIsNumeric = /^[0-9:+-]*$/.test(tzNum)

    const offset =
      (!timeZoneIsNumeric || tzNum.length === 3)
      ? tzNum
      : tzNum.length === 0
        ? '+00'
        : (tzNum.length === 2 || tzNum.length === 5)
          ? tzNum.replace('+', '+0').replace('-', '-0')
          : tzNum.padStart(5, '0')

    return offset
      .replace('UTC', '+00')
  }

  const opt = {
    hours12: false,
    timeZone: 'Europe/Berlin',
  }
  const date = new Date(Date.parse(dateString))
  return oneLine`
    ${date.toLocaleDateString('en-CA', opt)}
    ${date.toLocaleTimeString('de', opt)}
    ${getNumericOffset('Europe/Berlin', date)}
  `
}

async function resolveUndefined(credentials) {

  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  fs.mkdirSync(logdir)

  const userId = await connection
    .login(credentials.user, credentials.pw + credentials.token)
    .then(loginInformation => loginInformation.id)

  const undefinedRecords = await connection.queryAll(oneLine`
    SELECT Id, Title
    FROM ContentDocument
    WHERE Title LIKE 'undefined%'
    LIMIT 200
  `)
    .then(result => result.records)

  const prefix = /^undefined - /
  const suffix = /_backup$/
  const newRecords = undefinedRecords.map(record => {
    let newTitle = record.Title.replace(prefix, '')
    if (record.Title.match(suffix)) {
      newTitle = 'backup_' + newTitle.replace(suffix, '')
    }
    return {
      Id: record.Id,
      Title: newTitle
    }
  })

  await connection.update('ContentDocument', newRecords)

}
