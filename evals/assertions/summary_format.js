module.exports = (output) => {
  const text = String(output).trim();
  const failures = [];

  if (!text) {
    failures.push('summary is empty');
  }

  if (text.length > 700) {
    failures.push('summary exceeds the 700-character limit');
  }

  if (/^(?:here(?:'s| is)|below is|sure[!,]?|summary\s*:|(?:two|2)-sentence summary\s*:)/i.test(text)) {
    failures.push('summary contains a preamble or label');
  }

  if (/\b(?:let me know if|feel free to ask|happy to help|if you need (?:more|other) summaries)\b/i.test(text)) {
    failures.push('summary contains a conversational follow-up or sign-off');
  }

  // Count terminal punctuation only, so decimals such as "1.4x" do not add a sentence.
  const sentenceEndings = text.match(/[.!?]+(?=\s|$)/g) || [];
  if (sentenceEndings.length !== 2) {
    failures.push(`expected exactly two sentences; found ${sentenceEndings.length}`);
  }

  return {
    pass: failures.length === 0,
    score: failures.length === 0 ? 1 : 0,
    reason: failures.length === 0 ? 'Summary has the required format.' : failures.join('; '),
  };
};
