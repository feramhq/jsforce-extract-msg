const jsforce = require('jsforce')
const { oneLine, oneLineTrim } = require('common-tags')
const fs = require('fs')
const fetch = require("node-fetch")
const msg2txt = require('msg2txt')
const path = require('path')
const date = require('')

module.exports = {
  main,
  undoMyWork,
}

const logNames = {
  missingVersionData: `missingVersionData.txt`,
  requestErrors: `requestErrors.txt`,
  convertErrors: 'convertErros.txt',
  fetchErrors: 'fetchErrors.txt',
  uploadErrors: 'uploadErrors.txt',
  createLinkErrors: 'createLinkErrors.txt',
}

const timestamp = new Date()
const logdir = `log_${timestamp.toISOString()}`
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

async function uploadDocument(connection, localFilename) {

  const file = fs.readFileSync(localFilename)
  const versionRecord = {
    Title: path.basename(localFilename, path.extname(localFilename)),
    Description: '',
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
      (NOT Title LIKE 'backup_%')
    `
  if (!fileLimit === false) {
    query += ' LIMIT 30'
  }
  return await connection.queryAll(query)
    .then(result => {
      return result.records
        .filter(versionDataExists)
        .filter(notABackup)
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

function notABackup(record) {
  return !record.Title.startsWith('backup_')
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

async function deleteExtractedDocuments(connection, msgDocumentId) {
  const query = oneLine`
    SELECT ContentDocumentId
    FROM ContentDocumentLink
    WHERE LinkedEntityId = '${msgDocumentId}'
  `
  const ids = await connection.queryAll(query)
    .then(result => {
      return result.records.map(record => {
        return record.ContentDocumentId
      })
    })
  console.log(ids)
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

async function revertAllExtractions(connection) {
}

async function main(credentials, fileLimit) {

  if (!fileLimit === false) {
    fileLimit = true
  }

  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  fs.mkdirSync(logdir)

  fs.rmdirSync(filedir, {recursive: true})
  fs.mkdirSync(filedir)

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
    try {
      console.info(`downloading ${msgFileRecord.Id}`)
      await downloadDocument(connection, serverFilename, localFilename)
      console.info(`extracting ${msgFileRecord.Id}`)
      await extractMsgFile(localFilename)
    }
    catch(error) {
      continue
    }
    const uploadedVersionIds = []
    try{
      for (const i of fs.readdirSync(extractionDir)) {
        const newFilename = extractionDir + `/${i}`
        console.info(`>>> uploading ${newFilename}`)
        const newVersionId = await uploadDocument(connection, newFilename)
        uploadedVersionIds.push(newVersionId)
        console.info(`>>> create Links for ${newFilename}`)
        await createDocumentLinkRecords(connection, userId, msgFileRecord.Id, newVersionId)
      }
      console.info(`updating ${msgFileRecord.Id}`)
      const description = JSON.stringify(await getDocumentsWithVersionIds(
          connection,
          uploadedVersionIds,
        ))

      await connection.update(
        'ContentDocument',
        {
          Id: msgFileRecord.Id,
          Title: 'backup_' + msgFileRecord.Title,
          Description: description
        }
      )
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
      Title LIKE 'backup_%'
  `
  const records = await connection.queryAll(msgFilesQuery)
    .then(result => {
      return result.records
    })
  for (let record of records) {
    try{
      const filesToDelete = JSON.parse(record.Description)
      connection.destroy('ContentDocument', filesToDelete)
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
      continue
    }
  }

}

async function hardDelete(credentials) {
  // do NOT use this function! Only for debugging in the sandbox database!
  // This will delete ANY File without a .msg extension!

  const connection = new jsforce.Connection({
    loginUrl: credentials.url,
  })

  Object.entries(logNames).map(([_, value]) => {
    fs.unlink(value, () => {})
  })

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
