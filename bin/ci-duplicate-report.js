#!/usr/bin/env node
/**
 * Combined duplicate detection report - runs PMD CPD and jscpd, compares
 * both against the base branch, and posts a single unified PR comment.
 *
 * Expected files:
 *   cpd-pr.xml, cpd-base.xml                     (PMD CPD XML output)
 *   jscpd-report/jscpd-report.json               (jscpd JSON - PR)
 *   jscpd-base/jscpd-report.json                 (jscpd JSON - base)
 */
const fs = require('fs');
const crypto = require('crypto');
const { execSync } = require('child_process');

// Per-engine thresholds. Each engine produces different numbers, so the
// ceilings are tuned to their respective baselines (set just above current
// measured value). The increase rule is the real safety net - it catches
// regressions regardless of the absolute baseline.
const THRESHOLDS = {
  cpd:   { maxPct: 5, maxIncrease: 0.1 },   // PMD CPD baseline ~3.94%
  jscpd: { maxPct: 4, maxIncrease: 0.1 },   // jscpd baseline ~2.65%
};
const DIRS = ['parallel-consumer-core/src', 'parallel-consumer-vertx/src', 'parallel-consumer-reactor/src', 'parallel-consumer-mutiny/src'];

// ── Shared helpers ────────────────────────────────────────────────────

function contentHash(fragment) {
  return crypto.createHash('md5').update(fragment || '').digest('hex');
}

function relPath(p) {
  return p.replace(/^.*parallel-consumer\//, '');
}

function countTotalLines(dirs) {
  try {
    const result = execSync(
      `find ${dirs.join(' ')} -name '*.java' -type f 2>/dev/null | xargs wc -l 2>/dev/null | tail -1 | awk '{print $1}'`,
      { encoding: 'utf8' }
    );
    return parseInt(result.trim()) || 0;
  } catch {
    return 0;
  }
}

function deltaIcon(d) {
  return d > 0 ? ':small_red_triangle:' : d < 0 ? ':small_red_triangle_down:' : ':heavy_minus_sign:';
}

function fmtDelta(d, suffix) {
  if (d === 0) return `${deltaIcon(d)} 0`;
  const s = suffix || '';
  return d > 0 ? `${deltaIcon(d)} +${d}${s}` : `${deltaIcon(d)} ${d}${s}`;
}

// ── PMD CPD ───────────────────────────────────────────────────────────

async function parseCpdXml(path) {
  if (!fs.existsSync(path) || fs.statSync(path).size === 0) return null;
  const xml2js = require('xml2js');
  try {
    const result = await new xml2js.Parser().parseStringPromise(fs.readFileSync(path, 'utf8'));
    const root = result['pmd-cpd'];
    if (!root || !root.duplication) return { duplicates: [] };
    return {
      duplicates: root.duplication.map(d => ({
        lines: parseInt(d.$.lines),
        tokens: parseInt(d.$.tokens),
        files: (d.file || []).map(f => ({
          name: f.$.path,
          startLine: parseInt(f.$.line),
          endLine: parseInt(f.$.endline),
        })),
        fragment: (d.codefragment && d.codefragment[0]) || '',
      })),
    };
  } catch (err) {
    console.log(`Failed to parse ${path}: ${err.message}`);
    return null;
  }
}

function cpdStats(report, totalLines) {
  if (!report) return null;
  const clones = report.duplicates.length;
  const duplicatedLines = report.duplicates.reduce((sum, d) => sum + d.lines * d.files.length, 0);
  const percentage = totalLines > 0 ? (duplicatedLines / totalLines) * 100 : 0;
  return { clones, duplicatedLines, percentage };
}

function cpdNewClones(prReport, baseReport) {
  if (!baseReport) return prReport.duplicates || [];
  const baseHashes = new Set(baseReport.duplicates.map(d => contentHash(d.fragment)));
  return (prReport.duplicates || []).filter(d => !baseHashes.has(contentHash(d.fragment)));
}

// ── jscpd ─────────────────────────────────────────────────────────────

function loadJscpd(path) {
  if (!fs.existsSync(path)) return null;
  try {
    return JSON.parse(fs.readFileSync(path, 'utf8'));
  } catch {
    return null;
  }
}

function jscpdNewClones(prReport, baseReport) {
  if (!baseReport || !baseReport.duplicates) return prReport.duplicates || [];
  const baseHashes = new Set(baseReport.duplicates.map(d => contentHash(d.fragment)));
  return (prReport.duplicates || []).filter(d => !baseHashes.has(contentHash(d.fragment)));
}

// ── Report rendering ──────────────────────────────────────────────────

function renderEngineSection(title, prStats, baseStats, newClones, formatClone, check, thresholds) {
  const icon = check.shouldFail ? ':x:' : ':white_check_mark:';
  let md = `### ${icon} ${title}\n\n`;
  md += `| | PR | Base | Change |\n|---|--:|--:|--:|\n`;

  const cloneDelta = baseStats ? prStats.clones - baseStats.clones : 0;
  const linesDelta = baseStats ? prStats.duplicatedLines - baseStats.duplicatedLines : 0;

  md += `| **Clones** | ${prStats.clones} | ${baseStats ? baseStats.clones : '-'} | ${baseStats ? fmtDelta(cloneDelta) : '-'} |\n`;
  md += `| **Duplicated lines** | ${prStats.duplicatedLines} | ${baseStats ? baseStats.duplicatedLines : '-'} | ${baseStats ? fmtDelta(linesDelta) : '-'} |\n`;
  md += `| **Duplication** | ${prStats.percentage.toFixed(2)}% | ${baseStats ? baseStats.percentage.toFixed(2) + '%' : '-'} | ${baseStats ? fmtDelta(parseFloat(check.pctDelta.toFixed(2)), '%') : '-'} |\n\n`;

  md += `| Rule | Limit | Status |\n|------|-------|--------|\n`;
  md += `| Max duplication | ${thresholds.maxPct}% | ${check.pctFail ? ':x: FAIL' : ':white_check_mark: Pass'} (${prStats.percentage.toFixed(2)}%) |\n`;
  md += `| Max increase vs base | +${thresholds.maxIncrease}% | ${check.increaseFail ? ':x: FAIL' : ':white_check_mark: Pass'} (${baseStats ? (check.pctDelta >= 0 ? '+' : '') + check.pctDelta.toFixed(2) + '%' : 'no base'}) |\n\n`;

  if (newClones.length > 0) {
    md += `<details><summary>:warning: ${newClones.length} new clones introduced</summary>\n\n`;
    for (const clone of newClones.slice(0, 20)) {
      md += formatClone(clone);
    }
    if (newClones.length > 20) md += `\n...and ${newClones.length - 20} more\n`;
    md += `\n</details>\n\n`;
  } else if (baseStats) {
    md += `No new clones introduced by this PR.\n\n`;
  }

  return md;
}

function checkFail(stats, baseStats, thresholds) {
  if (!stats) return { shouldFail: false, pctFail: false, increaseFail: false, pctDelta: 0 };
  const pctDelta = baseStats ? stats.percentage - baseStats.percentage : 0;
  const pctFail = stats.percentage > thresholds.maxPct;
  const increaseFail = baseStats && pctDelta > thresholds.maxIncrease;
  return { shouldFail: pctFail || increaseFail, pctFail, increaseFail, pctDelta };
}

// ── Main entrypoint ───────────────────────────────────────────────────

async function analyzeAndReport({ github, context, core, prXmlPath, baseXmlPath, prJscpdPath, baseJscpdPath }) {
  const totalLines = countTotalLines(DIRS);

  // PMD CPD
  const cpdPr = await parseCpdXml(prXmlPath);
  const cpdBase = await parseCpdXml(baseXmlPath);
  const cpdPrStats = cpdStats(cpdPr, totalLines);
  const cpdBaseStats = cpdStats(cpdBase, totalLines);
  const cpdNew = cpdPr ? cpdNewClones(cpdPr, cpdBase) : [];
  const cpdCheck = cpdPrStats ? checkFail(cpdPrStats, cpdBaseStats, THRESHOLDS.cpd) : { shouldFail: false };

  // jscpd
  const jscpdPr = loadJscpd(prJscpdPath);
  const jscpdBase = loadJscpd(baseJscpdPath);
  const jscpdPrStats = jscpdPr ? {
    clones: jscpdPr.statistics.total.clones,
    duplicatedLines: jscpdPr.statistics.total.duplicatedLines,
    percentage: parseFloat(jscpdPr.statistics.total.percentage),
  } : null;
  const jscpdBaseStats = jscpdBase ? {
    clones: jscpdBase.statistics.total.clones,
    duplicatedLines: jscpdBase.statistics.total.duplicatedLines,
    percentage: parseFloat(jscpdBase.statistics.total.percentage),
  } : null;
  const jscpdNew = jscpdPr ? jscpdNewClones(jscpdPr, jscpdBase) : [];
  const jscpdCheck = jscpdPrStats ? checkFail(jscpdPrStats, jscpdBaseStats, THRESHOLDS.jscpd) : { shouldFail: false };

  const anyFail = cpdCheck.shouldFail || jscpdCheck.shouldFail;
  const overallIcon = anyFail ? ':x:' : ':white_check_mark:';

  // Build combined comment
  let body = `## ${overallIcon} Duplicate Code Report\n\n`;
  body += `Two engines run in parallel for cross-validation. Each has its own thresholds tuned to its baseline - the real safety net is the per-engine "max increase vs base" check.\n\n`;

  // PMD CPD section
  if (cpdPrStats) {
    const formatClone = (d) => {
      const locations = d.files.map(f => `\`${relPath(f.name)}:${f.startLine}\``).join(' <-> ');
      return `- **${d.lines} lines** (${d.tokens} tokens): ${locations}\n`;
    };
    body += renderEngineSection('PMD CPD (Java-aware)', cpdPrStats, cpdBaseStats, cpdNew, formatClone, cpdCheck, THRESHOLDS.cpd);
  } else {
    body += `### :question: PMD CPD\n\nNo report available.\n\n`;
  }

  // jscpd section
  if (jscpdPrStats) {
    const formatClone = (d) => {
      const f1 = relPath(d.firstFile.name);
      const f2 = relPath(d.secondFile.name);
      return `- **${d.lines} lines**: \`${f1}:${d.firstFile.startLoc.line}\` <-> \`${f2}:${d.secondFile.startLoc.line}\`\n`;
    };
    body += renderEngineSection('jscpd (language-agnostic)', jscpdPrStats, jscpdBaseStats, jscpdNew, formatClone, jscpdCheck, THRESHOLDS.jscpd);
  } else {
    body += `### :question: jscpd\n\nNo report available.\n\n`;
  }

  // Post or update PR comment
  const comments = await github.rest.issues.listComments({
    owner: context.repo.owner, repo: context.repo.repo, issue_number: context.issue.number
  });
  const existing = comments.data.find(c => c.body.startsWith('## ') && c.body.includes('Duplicate Code Report'));
  if (existing) {
    await github.rest.issues.updateComment({
      owner: context.repo.owner, repo: context.repo.repo, comment_id: existing.id, body
    });
  } else {
    await github.rest.issues.createComment({
      owner: context.repo.owner, repo: context.repo.repo, issue_number: context.issue.number, body
    });
  }

  // Annotate new CPD clones on PR diff (prefer CPD since it's more accurate)
  const annotateClones = cpdNew.length > 0 ? cpdNew : [];
  if (annotateClones.length > 0) {
    const { data: files } = await github.rest.pulls.listFiles({
      owner: context.repo.owner, repo: context.repo.repo, pull_number: context.issue.number
    });
    const changedFiles = new Set(files.map(f => f.filename));
    for (const d of annotateClones.slice(0, 10)) {
      for (const file of d.files) {
        const relFile = relPath(file.name);
        if (changedFiles.has(relFile)) {
          const others = d.files.filter(f => f !== file).map(f => `${relPath(f.name)}:${f.startLine}`).join(', ');
          try {
            await github.rest.pulls.createReviewComment({
              owner: context.repo.owner,
              repo: context.repo.repo,
              pull_number: context.issue.number,
              commit_id: context.sha,
              path: relFile,
              line: file.startLine,
              body: `:warning: **Duplicate code detected** - ${d.lines} lines duplicated with \`${others}\``
            });
          } catch (e) {
            console.log(`Could not annotate ${relFile}:${file.startLine} - ${e.message}`);
          }
          break;
        }
      }
    }
  }

  if (anyFail) {
    const msgs = [];
    if (cpdCheck.shouldFail) msgs.push('PMD CPD');
    if (jscpdCheck.shouldFail) msgs.push('jscpd');
    core.setFailed(`Duplicate code check failed (${msgs.join(', ')}) - see PR comment for details`);
  }
}

module.exports = { analyzeAndReport, THRESHOLDS };
