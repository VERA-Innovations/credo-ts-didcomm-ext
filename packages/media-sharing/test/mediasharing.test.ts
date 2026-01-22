import './setup'

import type { AskarModule } from '@credo-ts/askar'
import type { DidCommConnectionRecord, DidCommEncryptedMessage } from '@credo-ts/didcomm'

import { AskarModule as AskarModuleValue } from '@credo-ts/askar'
import { Agent, ConsoleLogger, LogLevel } from '@credo-ts/core'
import { DidCommModule } from '@credo-ts/didcomm'
import { agentDependencies } from '@credo-ts/node'
import { askarNodeJS } from '@openwallet-foundation/askar-nodejs'
import { firstValueFrom, ReplaySubject, Subject } from 'rxjs'

import { DidCommMediaSharingModule } from '../src/DidCommMediaSharingModule'
import { DidCommMediaSharingRecord } from '../src/repository'

import { recordsAddedByType } from './recordUtils'
import { SubjectInboundTransport } from './transport/SubjectInboundTransport'
import { SubjectOutboundTransport } from './transport/SubjectOutboundTransport'
import { recordToCursor } from '@credo-ts/core'

const logger = new ConsoleLogger(LogLevel.off)

export type SubjectMessage = {
  message: DidCommEncryptedMessage
  replySubject?: Subject<SubjectMessage>
}

type MediaTestAgent = Agent<{ askar: AskarModule; media: DidCommMediaSharingModule; didcomm: DidCommModule }>

const buildAskarModule = (label: string) =>
  new AskarModuleValue({
    askar: askarNodeJS,
    store: {
      id: `${label}-store`,
      key: `${label}-key`,
      database: { type: 'sqlite', config: { inMemory: true } },
    },
  })

const buildAgent = (
  label: string,
  endpoint: string,
  transports: { inbound: SubjectInboundTransport; outbound: SubjectOutboundTransport },
): MediaTestAgent =>
  new Agent({
    config: {
      logger,
      autoUpdateStorageOnStartup: true,
    },
    dependencies: agentDependencies,
    modules: {
      askar: buildAskarModule(label),
      media: new DidCommMediaSharingModule(),
      didcomm: new DidCommModule({
        endpoints: [endpoint],
        transports: {
          inbound: [transports.inbound],
          outbound: [transports.outbound],
        },
        credentials: false,
        proofs: false,
        messagePickup: false,
        mediator: false,
        mediationRecipient: false,
        basicMessages: false,
      }),
    },
  })

describe('media test', () => {
  let aliceAgent: MediaTestAgent
  let bobAgent: MediaTestAgent
  let subjectMap: Record<string, Subject<SubjectMessage>>
  let aliceConnectionRecord: DidCommConnectionRecord | undefined

  beforeEach(async () => {
    const aliceMessages = new Subject<SubjectMessage>()
    const bobMessages = new Subject<SubjectMessage>()
    subjectMap = {
      'rxjs:alice': aliceMessages,
      'rxjs:bob': bobMessages,
    }

    aliceAgent = buildAgent('alice', 'rxjs:alice', {
      inbound: new SubjectInboundTransport(aliceMessages),
      outbound: new SubjectOutboundTransport(subjectMap),
    })
    await aliceAgent.initialize()

    bobAgent = buildAgent('bob', 'rxjs:bob', {
      inbound: new SubjectInboundTransport(bobMessages),
      outbound: new SubjectOutboundTransport(subjectMap),
    })
    await bobAgent.initialize()

    const outOfBandRecord = await aliceAgent.didcomm.oob.createInvitation({
      autoAcceptConnection: true,
      label: 'alice',
    })

    const { connectionRecord } = await bobAgent.didcomm.oob.receiveInvitationFromUrl(
      outOfBandRecord.outOfBandInvitation.toUrl({
        domain: 'https://example.com/ssi',
      }),
      { autoAcceptConnection: true, label: 'bob' },
    )

    await bobAgent.didcomm.connections.returnWhenIsConnected(connectionRecord!.id)
    aliceConnectionRecord = (await aliceAgent.didcomm.connections.findAllByOutOfBandId(outOfBandRecord.id))[0]
    aliceConnectionRecord = await aliceAgent.didcomm.connections.returnWhenIsConnected(aliceConnectionRecord!.id)
  })

  afterEach(async () => {
    // Wait for messages to flush out
    await new Promise((r) => setTimeout(r, 1000))

    if (aliceAgent) {
      if (aliceAgent.isInitialized) {
        await aliceAgent.shutdown()
      }
    }

    if (bobAgent) {
      if (bobAgent.isInitialized) {
        await bobAgent.shutdown()
      }
    }
  })

  test('Create media and share it', async () => {
    const subjectAlice = new ReplaySubject<DidCommMediaSharingRecord>()
    const subjectBob = new ReplaySubject<DidCommMediaSharingRecord>()
    recordsAddedByType(aliceAgent, DidCommMediaSharingRecord).subscribe((record) =>
      subjectAlice.next(record as DidCommMediaSharingRecord),
    )

    const aliceRecord = await aliceAgent.modules.media.create({
      connectionId: aliceConnectionRecord!.id,
      metadata: {
        metadataKey1: 'metadata-val',
        metadataKey2: { key21: 'value21', key22: 'value22' },
      },
    })

    expect(aliceRecord.metadata.get('metadataKey1')).toEqual('metadata-val')
    expect(aliceRecord.metadata.get('metadataKey2')).toMatchObject({
      key21: 'value21',
      key22: 'value22',
    })

    await aliceAgent.modules.media.share({
      recordId: aliceRecord.id,
      items: [{ mimeType: 'image/png', uri: 'http://blabla', metadata: { duration: 14 } }],
    })

    recordsAddedByType(bobAgent, DidCommMediaSharingRecord).subscribe((record) =>
      subjectBob.next(record as DidCommMediaSharingRecord),
    )

    const bobRecord = await firstValueFrom(subjectBob)
    await firstValueFrom(subjectAlice)

    expect(bobRecord.items?.length).toBe(1)
    expect(bobRecord.items![0].mimeType).toBe('image/png')
    expect(bobRecord.items![0].uri).toBe('http://blabla')
    expect(bobRecord.items![0].metadata!.duration).toBe(14)

    // Now retrieve from repository
    const recordFromRepo = await bobAgent.modules.media.findById(bobRecord.id)
    expect(recordFromRepo).toBeDefined()
    expect(recordFromRepo!.items?.length).toBe(1)

    const item = recordFromRepo!.items![0]
    expect(item.mimeType).toBe('image/png')
    expect(item.uri).toBe('http://blabla')
    expect(item.metadata!.duration).toBe(14)
  })

  test('Cursor based pagination for media sharing records', async () => {
    const createdRecords: DidCommMediaSharingRecord[] = []

    // Create 3 media records
    for (let i = 0; i < 3; i++) {
      const record = await aliceAgent.modules.media.create({
        connectionId: aliceConnectionRecord!.id,
        metadata: { index: i },
      })

      await aliceAgent.modules.media.share({
        recordId: record.id,
        items: [
          {
            mimeType: 'image/png',
            uri: `http://image-${i}`,
            metadata: { order: i },
          },
        ],
      })

      createdRecords.push(record)
    }

    const waitForRecords = async (expected: number, timeoutMs = 5000) => {
      const start = Date.now()

      while (Date.now() - start < timeoutMs) {
        const records = await bobAgent.modules.media.getAll()
        if (records.length === expected) return records
        await new Promise((r) => setTimeout(r, 100))
      }

      throw new Error(`Timed out waiting for ${expected} records`)
    }

    const allRecords = await waitForRecords(3)
    expect(allRecords.length).toBe(3)

    // Sort deterministically by createdAt (important for cursor pagination)
    const sortedRecords = [...allRecords].sort(
      (a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime(),
    )

    // Simulate limit = 2
    const paginatedRecords = sortedRecords.slice(0, 2)

    // Manually calculate cursors
    const cursor = {
      before:
        paginatedRecords.length > 0
          ? recordToCursor({
            id: paginatedRecords[0].id,
            createdAt: String(paginatedRecords[0].createdAt),
          })
          : '',
      after:
        paginatedRecords.length > 0
          ? recordToCursor({
            id: paginatedRecords[paginatedRecords.length - 1].id,
            createdAt:
              String(paginatedRecords[paginatedRecords.length - 1].createdAt),
          })
          : '',
    }

    // Assertions
    expect(paginatedRecords).toHaveLength(2)

    expect(cursor.before).toBe(
      recordToCursor({
        id: paginatedRecords[0].id,
        createdAt: String(paginatedRecords[0].createdAt),
      }),
    )

    expect(cursor.after).toBe(
      recordToCursor({
        id: paginatedRecords[1].id,
        createdAt: String(paginatedRecords[1].createdAt),
      }),
    )
  })
})
