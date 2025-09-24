#!/usr/bin/env -S deno run -A
import * as path from "https://deno.land/std@0.208.0/path/mod.ts";
import * as posix from "https://deno.land/std@0.208.0/path/posix/mod.ts";
import { walk } from "https://deno.land/std@0.208.0/fs/walk.ts";

interface Note {
  relPath: string;
  absPath: string;
  content: string;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

class BacklinksSectionError extends Error {
  constructor(noteRelPath: string | undefined, detail: string) {
    const location = noteRelPath ? ` in ${noteRelPath}` : "";
    super(`Unexpected content under "## Backlinks"${location}: ${detail}`);
    this.name = "BacklinksSectionError";
  }
}

async function pandocToJson(content: string): Promise<unknown> {
  const command = new Deno.Command("pandoc", {
    args: ["-f", "markdown", "-t", "json"],
    stdin: "piped",
    stdout: "piped",
    stderr: "piped",
  });

  let child: Deno.ChildProcess;
  try {
    child = command.spawn();
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) {
      throw new Error(
        "Pandoc executable not found. Install Pandoc to generate backlinks.",
      );
    }
    throw error;
  }

  if (child.stdin) {
    const writer = child.stdin.getWriter();
    try {
      await writer.write(textEncoder.encode(content));
    } finally {
      await writer.close();
    }
  }

  const output = await child.output();

  if (!output.success) {
    const stderr = textDecoder.decode(output.stderr).trim();
    const reason = stderr || `Pandoc exited with code ${output.code}`;
    throw new Error(reason);
  }

  const json = textDecoder.decode(output.stdout);
  return JSON.parse(json);
}

function assertValidBacklinksBody(
  backlinksBody: string,
  noteRelPath: string,
): void {
  const trimmed = backlinksBody.trim();
  if (!trimmed) {
    return;
  }

  const lines = trimmed.split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) {
      continue;
    }

    const match = /^- \[[^\]]+\]\(([^)]+)\)$/.exec(line);
    if (!match) {
      throw new BacklinksSectionError(
        noteRelPath,
        `Backlink line must look like "- [Note](path.md)", but got:\n${line}`,
      );
    }

    const target = match[1].trim();
    if (!target.endsWith(".md")) {
      throw new BacklinksSectionError(
        noteRelPath,
        `Backlink link target must point to a Markdown note (found "${target}").`,
      );
    }
  }
}

function removeExistingBacklinks(content: string, noteRelPath: string): string {
  const removed = (() => {
    const headingPattern = /(^|\n)## Backlinks([^\n]*)\n/;
    const match = headingPattern.exec(content);
    if (!match) {
      return content;
    }

    const headingSuffix = match[2] ?? "";
    if (headingSuffix.trim()) {
      throw new BacklinksSectionError(
        noteRelPath,
        `Heading must be exactly "## Backlinks" (found "${`## Backlinks${headingSuffix}`.trim()}")`,
      );
    }

    const sectionStart = match.index;
    const bodyStart = sectionStart + match[0].length;
    const backlinksBody = content.slice(bodyStart);
    assertValidBacklinksBody(backlinksBody, noteRelPath);

    return content.slice(0, sectionStart).replace(/\s*$/, "");
  })();

  // 末尾の空白（特に改行）を削除
  return removed.replace(/\s*$/, "");
}

function ensureTrailingNewline(content: string): string {
  return content.endsWith("\n") ? content : `${content}\n`;
}

async function hasPandoc(): Promise<boolean> {
  try {
    const { success } = await new Deno.Command("pandoc", {
      args: ["--version"],
    }).output();
    return success;
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) {
      return false;
    }
    throw error;
  }
}

async function parseLinkTargets(content: string): Promise<string[]> {
  if (!content.trim()) {
    return [];
  }

  const document = await pandocToJson(content);
  const targets: string[] = [];

  function visit(node: unknown): void {
    if (Array.isArray(node)) {
      for (const item of node) {
        visit(item);
      }
      return;
    }

    if (!node || typeof node !== "object") {
      return;
    }

    const record = node as Record<string, unknown>;
    if (
      record.t === "Link" &&
      Array.isArray(record.c) &&
      record.c.length >= 3
    ) {
      const target = record.c[2];
      if (Array.isArray(target) && typeof target[0] === "string") {
        targets.push(target[0]);
      }
    }

    for (const value of Object.values(record)) {
      visit(value);
    }
  }

  visit(document);

  return targets;
}

function resolveRelative(
  sourceRel: string,
  target: string,
  notes: Map<string, Note>,
): string | null {
  const sourceDir = posix.dirname(sourceRel);
  const normalized = target.replace(/\\/g, "/");
  const withoutFragment = normalized.split("#")[0];
  const trimmed = withoutFragment.trim();
  if (!trimmed) {
    return null;
  }

  const candidates = new Set<string>();
  const withExt = trimmed.endsWith(".md") ? trimmed : `${trimmed}.md`;

  const fromSource = posix.normalize(
    sourceDir === "." ? withExt : posix.join(sourceDir, withExt),
  );
  candidates.add(fromSource);
  candidates.add(posix.normalize(withExt));

  for (const candidate of candidates) {
    if (notes.has(candidate)) {
      return candidate;
    }
  }

  return null;
}

function ensureBacklinkSet(
  backlinks: Map<string, Set<string>>,
  target: string,
): Set<string> {
  if (!backlinks.has(target)) {
    backlinks.set(target, new Set());
  }
  return backlinks.get(target)!;
}

async function collectLinks(
  note: Note,
  notes: Map<string, Note>,
): Promise<Set<string>> {
  const content = removeExistingBacklinks(note.content, note.relPath);
  const targets: string[] = await parseLinkTargets(content);

  const seen = new Set<string>();
  for (const rawTarget of targets) {
    if (/^[a-z]+:\/\//i.test(rawTarget) || rawTarget.startsWith("mailto:")) {
      // スキーマ付きでローカルのリンクじゃない
      continue;
    }

    const candidates = new Set<string>([rawTarget]);
    try {
      // URLエンコードされている可能性を考慮
      const decoded = decodeURI(rawTarget);
      candidates.add(decoded);
    } catch {
      // Ignore decoding errors and keep raw target only.
    }

    for (const candidate of candidates) {
      // 両方成功するということはない、はず
      const resolved = resolveRelative(note.relPath, candidate, notes);
      if (resolved && resolved !== note.relPath) {
        seen.add(resolved);
        break;
      }
    }
  }

  return seen;
}

function formatBacklinks(
  target: string,
  backlinks: Map<string, Set<string>>,
): string | null {
  const sources = backlinks.get(target);
  if (!sources || sources.size === 0) {
    return null;
  }
  const sorted = Array.from(sources).sort((a, b) => a.localeCompare(b, "en"));
  const lines = sorted.map((relPath) => {
    const display = posix.basename(relPath).replace(/\.md$/i, "");
    return `- [${display}](${relPath})`;
  });
  return `## Backlinks\n\n${lines.join("\n")}`;
}

async function listNotes(notesRootPath: string): Promise<Map<string, Note>> {
  const notes = new Map<string, Note>();
  for await (const entry of walk(notesRootPath, {
    includeDirs: false,
    exts: [".md"],
  })) {
    const absPath = entry.path;
    const relPath = path.relative(notesRootPath, absPath);
    const content = await Deno.readTextFile(absPath);
    notes.set(relPath, { relPath, absPath, content });
  }
  return notes;
}

async function gatherBacklinks(
  notes: Map<string, Note>,
): Promise<Map<string, Set<string>>> {
  const backlinks = new Map<string, Set<string>>();
  for (const note of notes.values()) {
    const targets = await collectLinks(note, notes);
    for (const target of targets) {
      ensureBacklinkSet(backlinks, target).add(note.relPath);
    }
  }
  return backlinks;
}

// return if true if `note` is updated
async function generateBacklinksForNote(
  backlinks: Map<string, Set<string>>,
  note: Note,
): Promise<boolean> {
  const baseContent = removeExistingBacklinks(note.content, note.relPath);
  const formattedBacklinks = formatBacklinks(note.relPath, backlinks);

  let nextContent = baseContent;
  if (formattedBacklinks) {
    nextContent = ensureTrailingNewline(
      `${baseContent}\n\n${formattedBacklinks}`,
    );
  } else {
    nextContent = ensureTrailingNewline(baseContent);
  }

  if (nextContent !== note.content) {
    await Deno.writeTextFile(note.absPath, nextContent);
    return true;
  } else {
    return false;
  }
}

type GenerateResult = {
  changes: number;
  noteCount: number;
};

async function generateBacklinks(
  notesRootPath: string,
): Promise<GenerateResult> {
  const notes = await listNotes(notesRootPath);
  const backlinks = await gatherBacklinks(notes);

  let changes = 0;
  for (const note of notes.values()) {
    const updated = await generateBacklinksForNote(backlinks, note);
    if (updated) {
      changes += 1;
    }
  }

  return { changes, noteCount: notes.size };
}

async function main() {
  const notesRootArg = Deno.args[0] ?? "notes";
  const notesRootPath = path.resolve(notesRootArg);

  let stat: Deno.FileInfo;
  try {
    stat = await Deno.stat(notesRootPath);
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) {
      console.error(`Notes directory not found: ${notesRootPath}`);
      Deno.exit(1);
    }
    throw error;
  }

  if (!stat.isDirectory) {
    console.error(`Notes path is not a directory: ${notesRootPath}`);
    Deno.exit(1);
  }

  const { changes, noteCount } = await generateBacklinks(notesRootPath);
  if (noteCount === 0) {
    console.log(`No Markdown notes found under ${notesRootPath}`);
    return;
  }

  console.log(`Updated ${changes} note${changes === 1 ? "" : "s"}.`);
}

if (import.meta.main) {
  try {
    await main();
    Deno.exit(0);
  } catch (error) {
    if (error instanceof BacklinksSectionError) {
      console.error(error.message);
      Deno.exit(1);
    }
    throw error;
  }
}

// -----------------------------------------------------------------------------
// Test
// -----------------------------------------------------------------------------

function assertEquals<T>(actual: T, expected: T, message?: string): void {
  const actualJson = JSON.stringify(actual);
  const expectedJson = JSON.stringify(expected);
  if (actualJson !== expectedJson) {
    throw new Error(
      message ??
        `Assertion failed: expected ${expectedJson}, got ${actualJson}`,
    );
  }
}

function assertStringIncludes(
  actual: string,
  expected: string,
  message?: string,
): void {
  if (!actual.includes(expected)) {
    throw new Error(
      message ??
        `Assertion failed: expected "${expected}" to be in "${actual}"`,
    );
  }
}

const pandocAvailable = await hasPandoc();

Deno.test({
  name: "removeExistingBacklinks removes backlinks block",
  ignore: !pandocAvailable,
  fn() {
    const source = "# Title\nBody text.\n\n## Backlinks\n\n- [A](a.md)\n";
    const stripped = removeExistingBacklinks(source, "note.md");
    assertEquals(stripped.trimEnd(), "# Title\nBody text.");
  },
});

Deno.test({
  name: "removeExistingBacklinks rejects non-backlink content",
  ignore: !pandocAvailable,
  fn() {
    const source =
      "# Title\n\n## Backlinks\n\n- [A](a.md)\nNotes that should not be here.\n";
    let threw = false;
    try {
      removeExistingBacklinks(source, "note.md");
    } catch (error) {
      threw = error instanceof BacklinksSectionError;
    }
    if (!threw) {
      throw new Error("Expected BacklinksSectionError for stray content");
    }
  },
});

Deno.test({
  name: "parseLinkTargets extracts link destinations via Pandoc",
  ignore: !pandocAvailable,
  async fn() {
    const result = await parseLinkTargets(
      "See [Doc](./guide.md) and [Encoded](Folder%20Note.md)",
    );
    assertEquals(result, ["./guide.md", "Folder%20Note.md"]);
  },
});

Deno.test({
  name: "generateBacklinks writes backlink sections idempotently",
  ignore: !pandocAvailable,
  async fn() {
    const tmpDir = await Deno.makeTempDir();
    try {
      const notesRoot = path.join(tmpDir, "notes");
      await Deno.mkdir(path.join(notesRoot, "sub"), { recursive: true });

      await Deno.writeTextFile(
        path.join(notesRoot, "a.md"),
        "A links to [B](sub/b.md) and [C](C%20note.md).\n",
      );
      await Deno.writeTextFile(
        path.join(notesRoot, "sub/b.md"),
        "B has no forward links.\n",
      );
      await Deno.writeTextFile(
        path.join(notesRoot, "C note.md"),
        "C mentions [A](a.md).\n",
      );

      const first = await generateBacklinks(notesRoot);
      assertEquals(first.noteCount, 3);
      assertEquals(first.changes, 3);

      const bContent = await Deno.readTextFile(
        path.join(notesRoot, "sub/b.md"),
      );
      assertStringIncludes(bContent, "## Backlinks");
      assertStringIncludes(bContent, "- [a](a.md)");

      const cContent = await Deno.readTextFile(
        path.join(notesRoot, "C note.md"),
      );
      assertStringIncludes(cContent, "- [a](a.md)");

      const second = await generateBacklinks(notesRoot);
      assertEquals(second.changes, 0);
    } finally {
      await Deno.remove(tmpDir, { recursive: true });
    }
  },
});
