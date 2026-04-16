#!/usr/bin/env node
/**
 * Parses PMD CPD XML output, compares PR vs base branch, and posts a PR comment
 * with the delta. Annotates new clones directly on the PR diff.
 *
 * Expected files:
 *   cpd-pr.xml   - PMD CPD XML output from the PR branch
 *   cpd-base.xml - PMD CPD XML output from the base branch (optional)
 */
const fs = require('fs');
const crypto = require('crypto');
const { execSync } = require('child_process');

const MAX_PCT = 3;
const MAX_PCT_INCREASE = 0.1;

async function parseCpdXml(path) {
  if (!fs.existsSync(path) || fs.statSync(path).size === 0) return null;
  const xml2js = require('xml2js');
  const parser = new xml2js.Parser();
  const content = fs.readFileSync(path, 'utf8');
  try {
    const result = await parser.parseStringPromise(content);
    // CPD XML structure: <pmd-cpd><duplication lines="N" tokens="M"><file path="..." line="N" endline="N"/>... <codefragment>...</codefragment></duplication>...</pmd-cpd>
    const root = result['pmd-cpd'];
    if (!root || !root.duplication) return { duplicates: [] };
    const duplicates = root.duplication.map(d => {
      const attrs = d.$;
      const files = (d.file || []).map(f => ({
        name: f.$.path,
        startLine: parseInt(f.$.line),
        endLine: parseInt(f.$.endline),
      }));
      const fragment = (d.codefragment && d.codefragment[0]) || '';
      return {
        lines: parseInt(attrs.lines),
        tokens: parseInt(attrs.tokens),
        files,
        fragment,
      };
    });
    return { duplicates };
  } catch (err) {
    console.log(`Failed to parse ${path}: ${err.message}`);
    return null;
  }
}

function cloneContentHash(d) {
  return crypto.createHash('md5').update(d.fragment || '').digest('hex');
}

function findNewClones(prReport, baseReport) {
  if (!baseReport) return prReport.duplicates || [];
  const baseHashes = new Set(baseReport.duplicates.map(cloneContentHash));
  return (prReport.duplicates || []).filter(d => !baseHashes.has(cloneContentHash(d)));
}

function countLinesInDirs(dirs) {
  // Count total Java lines for percentage calculation
  try {
    const result = execSync(
      `find ${dirs.join(' ')} -name '*.java' -type f 2>/dev/null | xargs wc -l 2>/dev/null | tail -1 | awk '{print $1}'`,
      { encoding: 'utf8' }
    );
    return parseInt(result.trim()) || 0;
  } catch (err) {
    return 0;
  }
}

function computeStats(report, totalLines) {
  if (!report) return null;
  const clones = report.duplicates.length;
  const duplicatedLines = report.duplicates.reduce((sum, d) => sum + d.lines * (d.files.length), 0);
  const percentage = totalLines > 0 ? (duplicatedLines / totalLines) * 100 : 0;
  return { clones, duplicatedLines, percentage };
}

function relPath(p) {
  return p.replace(/^.*parallel-consumer\//, '');
}

function buildComment(prReport, baseReport, prStats, baseStats, newClones, shouldFail, pctFail, increaseFail, pctDelta) {
  const icon = shouldFail ? ':x:' : ':white_check_mark:';
  const di = (d) => d > 0 ? ':small_red_triangle:' : d < 0 ? ':small_red_triangle_down:' : ':heavy_minus_sign:';
  const fd = (d, s) => {
    if (d === 0) return `${di(d)} 0`;
    return d > 0 ? `${di(d)} +${d}${s || ''}` : `${di(d)} ${d}${s || ''}`;
  };

  let body = `## ${icon} Duplicate Code Report (PMD CPD)\n\n`;
  body += `| | PR | Base | Change |\n|---|--:|--:|--:|\n`;
  body += `| **Clones** | ${prStats.clones} | ${baseStats ? baseStats.clones : '-'} | ${baseStats ? fd(prStats.clones - baseStats.clones) : '-'} |\n`;
  body += `| **Duplicated lines** | ${prStats.duplicatedLines} | ${baseStats ? baseStats.duplicatedLines : '-'} | ${baseStats ? fd(prStats.duplicatedLines - baseStats.duplicatedLines) : '-'} |\n`;
  body += `| **Duplication** | ${prStats.percentage.toFixed(2)}% | ${baseStats ? baseStats.percentage.toFixed(2) + '%' : '-'} | ${baseStats ? fd(parseFloat(pctDelta.toFixed(2)), '%') : '-'} |\n\n`;

  body += `| Rule | Limit | Status |\n|------|-------|--------|\n`;
  body += `| Max duplication | ${MAX_PCT}% | ${pctFail ? ':x: FAIL' : ':white_check_mark: Pass'} (${prStats.percentage.toFixed(2)}%) |\n`;
  body += `| Max increase vs base | +${MAX_PCT_INCREASE}% | ${increaseFail ? ':x: FAIL' : ':white_check_mark: Pass'} (${baseStats ? (pctDelta >= 0 ? '+' : '') + pctDelta.toFixed(2) + '%' : 'no base'}) |\n\n`;

  if (newClones.length > 0) {
    body += `### :warning: New clones introduced by this PR (${newClones.length})\n\n`;
    for (const d of newClones.slice(0, 20)) {
      const locations = d.files.map(f => `\`${relPath(f.name)}:${f.startLine}\``).join(' <-> ');
      body += `- **${d.lines} lines** (${d.tokens} tokens): ${locations}\n`;
    }
    if (newClones.length > 20) body += `\n...and ${newClones.length - 20} more\n`;
    body += '\n';
  } else if (baseReport) {
    body += `No new clones introduced by this PR.\n\n`;
  }

  return body;
}

async function analyzeAndReport({ github, context, core, prXmlPath, baseXmlPath }) {
  const prReport = await parseCpdXml(prXmlPath);
  if (!prReport) {
    console.log('No PR CPD report found');
    return;
  }
  const baseReport = await parseCpdXml(baseXmlPath);

  const dirs = ['parallel-consumer-core/src', 'parallel-consumer-vertx/src', 'parallel-consumer-reactor/src', 'parallel-consumer-mutiny/src'];
  const totalLines = countLinesInDirs(dirs);
  const prStats = computeStats(prReport, totalLines);
  const baseStats = computeStats(baseReport, totalLines);
  const pctDelta = baseStats ? prStats.percentage - baseStats.percentage : 0;

  const pctFail = prStats.percentage > MAX_PCT;
  const increaseFail = baseStats && pctDelta > MAX_PCT_INCREASE;
  const shouldFail = pctFail || increaseFail;

  const newClones = findNewClones(prReport, baseReport);
  const body = buildComment(prReport, baseReport, prStats, baseStats, newClones, shouldFail, pctFail, increaseFail, pctDelta);

  // Post or update PR comment
  const comments = await github.rest.issues.listComments({
    owner: context.repo.owner, repo: context.repo.repo, issue_number: context.issue.number
  });
  const existing = comments.data.find(c => c.body.includes('Duplicate Code Report'));
  if (existing) {
    await github.rest.issues.updateComment({
      owner: context.repo.owner, repo: context.repo.repo, comment_id: existing.id, body
    });
  } else {
    await github.rest.issues.createComment({
      owner: context.repo.owner, repo: context.repo.repo, issue_number: context.issue.number, body
    });
  }

  // Annotate new clones on the PR diff
  if (newClones.length > 0) {
    const { data: files } = await github.rest.pulls.listFiles({
      owner: context.repo.owner, repo: context.repo.repo, pull_number: context.issue.number
    });
    const changedFiles = new Set(files.map(f => f.filename));
    for (const d of newClones.slice(0, 10)) {
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

  if (shouldFail) {
    core.setFailed('Duplicate code check failed - see PR comment for details');
  }
}

module.exports = { analyzeAndReport, parseCpdXml, findNewClones, computeStats, MAX_PCT, MAX_PCT_INCREASE };
