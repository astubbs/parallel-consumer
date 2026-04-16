#!/usr/bin/env node
/**
 * Compares jscpd duplicate detection results between base and PR branches.
 * Posts a PR comment with the comparison and annotates genuinely new clones.
 *
 * Expected files:
 *   jscpd-report/jscpd-report.json  (PR branch results)
 *   jscpd-base/jscpd-report.json    (base branch results, optional)
 */
const fs = require('fs');
const crypto = require('crypto');

const MAX_PCT = 3;
const MAX_PCT_INCREASE = 0.1;

function loadReport(path) {
  if (!fs.existsSync(path)) return null;
  return JSON.parse(fs.readFileSync(path, 'utf8'));
}

function cloneContentHash(d) {
  // Hash the fragment content so we match by what the code IS, not where it is
  const content = d.fragment || '';
  return crypto.createHash('md5').update(content).digest('hex');
}

function findNewClones(prReport, baseReport) {
  if (!baseReport || !baseReport.duplicates) return prReport.duplicates || [];
  const baseHashes = new Set(baseReport.duplicates.map(cloneContentHash));
  return (prReport.duplicates || []).filter(d => !baseHashes.has(cloneContentHash(d)));
}

function buildComment(prReport, baseReport, newClones, shouldFail, pctFail, increaseFail, pctDelta) {
  const prStats = prReport.statistics.total;
  const baseStats = baseReport ? baseReport.statistics.total : null;
  const cloneDelta = baseStats ? prStats.clones - baseStats.clones : 0;

  const icon = shouldFail ? ':x:' : ':white_check_mark:';
  const di = (d) => d > 0 ? ':small_red_triangle:' : d < 0 ? ':small_red_triangle_down:' : ':heavy_minus_sign:';
  const fd = (d, s) => {
    if (d === 0) return `${di(d)} 0`;
    return d > 0 ? `${di(d)} +${d}${s || ''}` : `${di(d)} ${d}${s || ''}`;
  };

  let body = `## ${icon} Duplicate Code Report\n\n`;
  body += `| | PR | Base | Change |\n|---|--:|--:|--:|\n`;
  body += `| **Clones** | ${prStats.clones} | ${baseStats ? baseStats.clones : '-'} | ${baseStats ? fd(cloneDelta) : '-'} |\n`;
  body += `| **Duplicated lines** | ${prStats.duplicatedLines} | ${baseStats ? baseStats.duplicatedLines : '-'} | ${baseStats ? fd(prStats.duplicatedLines - baseStats.duplicatedLines) : '-'} |\n`;
  body += `| **Duplication** | ${prStats.percentage}% | ${baseStats ? baseStats.percentage + '%' : '-'} | ${baseStats ? fd(parseFloat(pctDelta.toFixed(2)), '%') : '-'} |\n\n`;

  body += `| Rule | Limit | Status |\n|------|-------|--------|\n`;
  body += `| Max duplication | ${MAX_PCT}% | ${pctFail ? ':x: FAIL' : ':white_check_mark: Pass'} (${prStats.percentage}%) |\n`;
  body += `| Max increase vs base | +${MAX_PCT_INCREASE}% | ${increaseFail ? ':x: FAIL' : ':white_check_mark: Pass'} (${baseStats ? (pctDelta >= 0 ? '+' : '') + pctDelta.toFixed(2) + '%' : 'no base'}) |\n\n`;

  if (newClones.length > 0) {
    body += `### :warning: New clones introduced by this PR (${newClones.length})\n\n`;
    for (const d of newClones.slice(0, 20)) {
      const f1 = d.firstFile.name.replace(/^.*parallel-consumer\//, '');
      const f2 = d.secondFile.name.replace(/^.*parallel-consumer\//, '');
      body += `- **${d.lines} lines**: \`${f1}:${d.firstFile.startLoc.line}\` <-> \`${f2}:${d.secondFile.startLoc.line}\`\n`;
    }
    if (newClones.length > 20) body += `\n...and ${newClones.length - 20} more\n`;
    body += '\n';
  } else if (baseStats) {
    body += `No new clones introduced by this PR.\n\n`;
  }

  return body;
}

module.exports = { loadReport, findNewClones, buildComment, cloneContentHash, MAX_PCT, MAX_PCT_INCREASE };
