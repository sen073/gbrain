import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { runExtract } from '../src/commands/extract.ts';
import type { PageInput } from '../src/core/types.ts';

let engine: PGLiteEngine;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
}, 60_000);

afterAll(async () => {
  await engine.disconnect();
}, 60_000);

async function truncateAll() {
  for (const t of ['content_chunks', 'links', 'tags', 'raw_data', 'timeline_entries', 'page_versions', 'ingest_log', 'pages']) {
    await (engine as any).db.exec(`DELETE FROM ${t}`);
  }
}

const personPage = (title: string, body = ''): PageInput => ({
  type: 'person', title, compiled_truth: body, timeline: '',
});

const companyPage = (title: string, body = ''): PageInput => ({
  type: 'company', title, compiled_truth: body, timeline: '',
});

const meetingPage = (title: string, body = ''): PageInput => ({
  type: 'meeting', title, compiled_truth: body, timeline: '',
});

describe('gbrain extract links --source db', () => {
  beforeEach(truncateAll);

  test('extracts links from meeting page with attendee refs', async () => {
    await engine.putPage('people/alice', personPage('Alice'));
    await engine.putPage('people/bob', personPage('Bob'));
    await engine.putPage('meetings/standup', meetingPage(
      'Standup',
      'Attendees: [Alice](people/alice), [Bob](people/bob).',
    ));

    await runExtract(engine, ['links', '--source', 'db']);

    const links = await engine.getLinks('meetings/standup');
    expect(links.length).toBe(2);
    expect(new Set(links.map(l => l.to_slug))).toEqual(new Set(['people/alice', 'people/bob']));
    expect(links.every(l => l.link_type === 'attended')).toBe(true);
  });

  test('infers works_at type from CEO context', async () => {
    await engine.putPage('companies/acme', companyPage('Acme'));
    await engine.putPage('people/alice', personPage(
      'Alice',
      '[Alice](people/alice) is the CEO of [Acme](companies/acme).',
    ));

    await runExtract(engine, ['links', '--source', 'db']);
    const links = await engine.getLinks('people/alice');
    const acmeLink = links.find(l => l.to_slug === 'companies/acme');
    expect(acmeLink?.link_type).toBe('works_at');
  });

  test('idempotent: running twice produces same link count', async () => {
    await engine.putPage('people/alice', personPage('Alice'));
    await engine.putPage('companies/acme', companyPage('Acme', '[Alice](people/alice) advises us.'));

    await runExtract(engine, ['links', '--source', 'db']);
    const after1 = await engine.getLinks('companies/acme');

    await runExtract(engine, ['links', '--source', 'db']);
    const after2 = await engine.getLinks('companies/acme');
    expect(after2.length).toBe(after1.length);
  });

  test('skips refs to non-existent target pages', async () => {
    await engine.putPage('people/alice', personPage(
      'Alice',
      'Met [Phantom](people/phantom-ghost) at the event.',
    ));
    await runExtract(engine, ['links', '--source', 'db']);
    const links = await engine.getLinks('people/alice');
    expect(links.length).toBe(0);
  });

  test('--dry-run --json outputs JSON lines and writes nothing', async () => {
    await engine.putPage('people/alice', personPage('Alice'));
    await engine.putPage('companies/acme', companyPage(
      'Acme',
      '[Alice](people/alice) joined as CEO.',
    ));

    const lines: string[] = [];
    const originalWrite = process.stdout.write.bind(process.stdout);
    process.stdout.write = ((chunk: string | Uint8Array): boolean => {
      const str = typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf-8');
      lines.push(str);
      return true;
    }) as any;

    try {
      await runExtract(engine, ['links', '--source', 'db', '--dry-run', '--json']);
    } finally {
      process.stdout.write = originalWrite;
    }

    const jsonLines = lines.filter(l => l.trim().startsWith('{'));
    expect(jsonLines.length).toBeGreaterThan(0);
    const parsed = JSON.parse(jsonLines[0].trim());
    expect(parsed.action).toBe('add_link');
    expect(parsed.from).toBeTruthy();
    expect(parsed.to).toBeTruthy();
    expect(parsed.type).toBeTruthy();

    const links = await engine.getLinks('companies/acme');
    expect(links.length).toBe(0);
  });

  test('--type filter only processes matching pages', async () => {
    await engine.putPage('people/alice', personPage('Alice'));
    await engine.putPage('people/bob', personPage('Bob', '[Alice](people/alice) is great.'));
    await engine.putPage('companies/acme', companyPage('Acme', '[Alice](people/alice) joined.'));

    await runExtract(engine, ['links', '--source', 'db', '--type', 'person']);

    const bobLinks = await engine.getLinks('people/bob');
    expect(bobLinks.length).toBe(1);
    const acmeLinks = await engine.getLinks('companies/acme');
    expect(acmeLinks.length).toBe(0);
  });

  test('scoped source only creates links within the active source', async () => {
    const db = (engine as any).db;
    await db.query(`INSERT INTO sources (id, name) VALUES ('wiki', 'wiki') ON CONFLICT (id) DO NOTHING`);

    await engine.putPage('people/alice', personPage('Alice (default)'));
    await engine.putPage('people/alice', { ...personPage('Alice (wiki)'), source_id: 'wiki' });
    await engine.putPage('companies/acme', companyPage('Acme (default)', '[Alice](people/alice) joined.'));
    await engine.putPage('companies/acme', {
      ...companyPage('Acme (wiki)', '[Alice](people/alice) joined.'),
      source_id: 'wiki',
    });

    const previousSource = process.env.GBRAIN_SOURCE;
    process.env.GBRAIN_SOURCE = 'wiki';
    try {
      await runExtract(engine, ['links', '--source', 'db']);
    } finally {
      if (previousSource === undefined) delete process.env.GBRAIN_SOURCE;
      else process.env.GBRAIN_SOURCE = previousSource;
    }

    const defaultLinks = await engine.executeRaw<{ from_slug: string; to_slug: string; from_source_id: string; to_source_id: string }>(
      `SELECT f.slug AS from_slug, t.slug AS to_slug, f.source_id AS from_source_id, t.source_id AS to_source_id
       FROM links l
       JOIN pages f ON f.id = l.from_page_id
       JOIN pages t ON t.id = l.to_page_id
       WHERE f.slug = $1 AND f.source_id = $2`,
      ['companies/acme', 'default'],
    );
    const wikiLinks = await engine.executeRaw<{ from_slug: string; to_slug: string; from_source_id: string; to_source_id: string }>(
      `SELECT f.slug AS from_slug, t.slug AS to_slug, f.source_id AS from_source_id, t.source_id AS to_source_id
       FROM links l
       JOIN pages f ON f.id = l.from_page_id
       JOIN pages t ON t.id = l.to_page_id
       WHERE f.slug = $1 AND f.source_id = $2`,
      ['companies/acme', 'wiki'],
    );

    expect(defaultLinks).toHaveLength(0);
    expect(wikiLinks).toHaveLength(1);
    expect(wikiLinks[0].to_slug).toBe('people/alice');
    expect(wikiLinks[0].from_source_id).toBe('wiki');
    expect(wikiLinks[0].to_source_id).toBe('wiki');
  });

  test('same-slug multi-source links resolve to the current page source', async () => {
    const db = (engine as any).db;
    await db.query(`INSERT INTO sources (id, name) VALUES ('wiki', 'wiki') ON CONFLICT (id) DO NOTHING`);

    await engine.putPage('people/alice', personPage('Alice (default)'));
    await engine.putPage('people/alice', { ...personPage('Alice (wiki)'), source_id: 'wiki' });
    await engine.putPage('companies/acme', {
      ...companyPage('Acme (wiki)', '[Alice](people/alice) joined as CEO.'),
      source_id: 'wiki',
    });

    const previousSource = process.env.GBRAIN_SOURCE;
    process.env.GBRAIN_SOURCE = 'wiki';
    try {
      await runExtract(engine, ['links', '--source', 'db']);
    } finally {
      if (previousSource === undefined) delete process.env.GBRAIN_SOURCE;
      else process.env.GBRAIN_SOURCE = previousSource;
    }

    const wikiLinks = await engine.executeRaw<{ to_slug: string; from_source_id: string; to_source_id: string }>(
      `SELECT t.slug AS to_slug, f.source_id AS from_source_id, t.source_id AS to_source_id
       FROM links l
       JOIN pages f ON f.id = l.from_page_id
       JOIN pages t ON t.id = l.to_page_id
       WHERE f.slug = $1 AND f.source_id = $2`,
      ['companies/acme', 'wiki'],
    );

    expect(wikiLinks).toHaveLength(1);
    expect(wikiLinks[0].to_slug).toBe('people/alice');
    expect(wikiLinks[0].from_source_id).toBe('wiki');
    expect(wikiLinks[0].to_source_id).toBe('wiki');
  });
});

describe('gbrain extract timeline --source db', () => {
  beforeEach(truncateAll);

  test('extracts dated timeline entries from page content', async () => {
    await engine.putPage('people/alice', {
      type: 'person', title: 'Alice',
      compiled_truth: 'Alice is the CEO.',
      timeline: `## Timeline
- **2026-01-15** | Joined as CEO
- **2026-02-20** | Closed Series A`,
    });

    await runExtract(engine, ['timeline', '--source', 'db']);

    const entries = await engine.getTimeline('people/alice');
    expect(entries.length).toBe(2);
    expect(entries.map(e => e.summary).sort()).toEqual(['Closed Series A', 'Joined as CEO']);
  });

  test('projects timeline entries from a note onto the single linked entity even when note is inserted first', async () => {
    await engine.putPage('sessions/standup', {
      type: 'note' as any,
      title: 'Standup',
      compiled_truth: '[Alice](people/alice) discussed roadmap.',
      timeline: '- **2026-01-15** | Discussed roadmap',
    });
    await engine.putPage('people/alice', {
      type: 'person',
      title: 'Alice',
      compiled_truth: '',
      timeline: '',
    });

    await runExtract(engine, ['timeline', '--source', 'db']);

    const noteEntries = await engine.getTimeline('sessions/standup');
    const personEntries = await engine.getTimeline('people/alice');
    expect(noteEntries.map(e => e.summary)).toEqual(['Discussed roadmap']);
    expect(personEntries.map(e => e.summary)).toEqual(['Discussed roadmap']);
  });

  test('scoped extraction reads GBRAIN_SOURCE and projects only within that source', async () => {
    const db = (engine as any).db;
    await db.query(`INSERT INTO sources (id, name) VALUES ('wiki', 'wiki') ON CONFLICT (id) DO NOTHING`);

    await engine.putPage('people/alice', {
      type: 'person',
      title: 'Alice (default)',
      compiled_truth: '',
      timeline: '',
    });
    await engine.putPage('people/alice', {
      type: 'person',
      title: 'Alice (wiki)',
      compiled_truth: '',
      timeline: '',
      source_id: 'wiki',
    });
    await engine.putPage('sessions/wiki-standup', {
      type: 'note' as any,
      title: 'Wiki Standup',
      compiled_truth: '[Alice](people/alice) discussed roadmap.',
      timeline: '- **2026-01-15** | Scoped roadmap',
      source_id: 'wiki',
    });

    const previousSource = process.env.GBRAIN_SOURCE;
    process.env.GBRAIN_SOURCE = 'wiki';
    try {
      await runExtract(engine, ['timeline', '--source', 'db']);
    } finally {
      if (previousSource === undefined) delete process.env.GBRAIN_SOURCE;
      else process.env.GBRAIN_SOURCE = previousSource;
    }

    const defaultEntries = await engine.executeRaw<{ slug: string; source_id: string; summary: string }>(
      `SELECT p.slug, p.source_id, te.summary
       FROM timeline_entries te
       JOIN pages p ON p.id = te.page_id
       WHERE p.slug = $1 AND p.source_id = $2`,
      ['people/alice', 'default'],
    );
    const wikiEntries = await engine.executeRaw<{ slug: string; source_id: string; summary: string }>(
      `SELECT p.slug, p.source_id, te.summary
       FROM timeline_entries te
       JOIN pages p ON p.id = te.page_id
       WHERE p.slug = $1 AND p.source_id = $2`,
      ['people/alice', 'wiki'],
    );
    expect(defaultEntries.map(e => e.summary)).toEqual([]);
    expect(wikiEntries.map(e => e.summary)).toEqual(['Scoped roadmap']);
  });

  test('does not project note timeline when multiple entities are linked', async () => {
    await engine.putPage('people/alice', { type: 'person', title: 'Alice', compiled_truth: '', timeline: '' });
    await engine.putPage('companies/acme', { type: 'company', title: 'Acme', compiled_truth: '', timeline: '' });
    await engine.putPage('sessions/board-sync', {
      type: 'note' as any,
      title: 'Board Sync',
      compiled_truth: '[Alice](people/alice) met with [Acme](companies/acme).',
      timeline: '- **2026-01-15** | Reviewed growth plan',
    });

    await runExtract(engine, ['timeline', '--source', 'db']);

    const aliceEntries = await engine.getTimeline('people/alice');
    const acmeEntries = await engine.getTimeline('companies/acme');
    expect(aliceEntries).toHaveLength(0);
    expect(acmeEntries).toHaveLength(0);
  });

  test('idempotent via DB constraint', async () => {
    await engine.putPage('people/alice', {
      type: 'person', title: 'Alice', compiled_truth: '',
      timeline: '- **2026-01-15** | Same event',
    });
    await runExtract(engine, ['timeline', '--source', 'db']);
    await runExtract(engine, ['timeline', '--source', 'db']);
    const entries = await engine.getTimeline('people/alice');
    expect(entries.length).toBe(1);
  });

  test('skips invalid dates', async () => {
    await engine.putPage('people/alice', {
      type: 'person', title: 'Alice', compiled_truth: '',
      timeline: `- **2026-01-15** | Valid
- **2026-13-45** | Invalid month/day
- **2026-02-30** | Feb 30 doesnt exist`,
    });
    await runExtract(engine, ['timeline', '--source', 'db']);
    const entries = await engine.getTimeline('people/alice');
    expect(entries.length).toBe(1);
    expect(entries[0].summary).toBe('Valid');
  });

  test('handles multiple date format variants', async () => {
    await engine.putPage('people/alice', {
      type: 'person', title: 'Alice', compiled_truth: '',
      timeline: `- **2026-01-15** | Pipe variant
- **2026-02-20** -- Double dash variant
- **2026-03-10** - Single dash variant`,
    });
    await runExtract(engine, ['timeline', '--source', 'db']);
    const entries = await engine.getTimeline('people/alice');
    expect(entries.length).toBe(3);
  });

  test('--dry-run --json emits JSON, no DB writes', async () => {
    await engine.putPage('people/alice', {
      type: 'person', title: 'Alice', compiled_truth: '',
      timeline: '- **2026-01-15** | Test event',
    });

    const lines: string[] = [];
    const originalWrite = process.stdout.write.bind(process.stdout);
    process.stdout.write = ((chunk: string | Uint8Array): boolean => {
      const str = typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf-8');
      lines.push(str);
      return true;
    }) as any;
    try {
      await runExtract(engine, ['timeline', '--source', 'db', '--dry-run', '--json']);
    } finally {
      process.stdout.write = originalWrite;
    }

    const jsonLines = lines.filter(l => l.trim().startsWith('{'));
    expect(jsonLines.length).toBeGreaterThan(0);
    const parsed = JSON.parse(jsonLines[0].trim());
    expect(parsed.action).toBe('add_timeline');
    expect(parsed.date).toBe('2026-01-15');
    expect(parsed.summary).toBe('Test event');

    const entries = await engine.getTimeline('people/alice');
    expect(entries.length).toBe(0);
  });
});

describe('gbrain extract all --source db', () => {
  beforeEach(truncateAll);

  test('runs both links and timeline in one command', async () => {
    await engine.putPage('people/alice', personPage('Alice'));
    await engine.putPage('companies/acme', {
      type: 'company', title: 'Acme',
      compiled_truth: '[Alice](people/alice) joined as CEO.',
      timeline: '- **2026-01-15** | Funding closed',
    });

    await runExtract(engine, ['all', '--source', 'db']);

    const links = await engine.getLinks('companies/acme');
    const timeline = await engine.getTimeline('companies/acme');
    expect(links.length).toBe(1);
    expect(timeline.length).toBe(1);
    expect(timeline[0].summary).toBe('Funding closed');
  });
});
