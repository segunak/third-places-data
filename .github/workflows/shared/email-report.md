---
safe-outputs:
  github-token: ${{ secrets.GH_AW_GITHUB_TOKEN }}
  jobs:
    send-email-report:
      description: "Send a private Third Place Alerts email report through Gmail SMTP."
      runs-on: ubuntu-latest
      output: "Email report processed."
      permissions:
        contents: read
      inputs:
        subject:
          description: "Email subject, 160 characters maximum."
          required: true
          type: string
        html_body:
          description: "Sanitized HTML email body, 60000 characters maximum."
          required: true
          type: string
        text_body:
          description: "Plain-text fallback email body, 20000 characters maximum."
          required: true
          type: string
      steps:
        - name: Extract And Validate Email Request
          id: email
          uses: actions/github-script@v8
          with:
            script: |
              const fs = require('fs');
              const path = require('path');
              const outputFile = process.env.GH_AW_AGENT_OUTPUT;

              if (!outputFile || !fs.existsSync(outputFile)) {
                core.info('No agent output file found; no email will be sent.');
                core.setOutput('send', 'false');
                return;
              }

              const agentOutput = JSON.parse(fs.readFileSync(outputFile, 'utf8'));
              const items = (agentOutput.items || []).filter((item) => item.type === 'send_email_report');

              if (items.length === 0) {
                core.info('No send_email_report request found; no email will be sent.');
                core.setOutput('send', 'false');
                return;
              }

              if (items.length > 1) {
                core.setFailed(`Expected at most one send_email_report request, found ${items.length}.`);
                return;
              }

              const item = items[0];
              const subject = String(item.subject || '').trim();
              const rawHtmlBody = String(item.html_body || '');
              const rawTextBody = String(item.text_body || '');

              if (!subject || !rawHtmlBody || !rawTextBody) {
                core.setFailed('subject, html_body, and text_body are required.');
                return;
              }

              if (subject.length > 160) {
                core.setFailed(`subject exceeds 160 characters (${subject.length}).`);
                return;
              }

              if (rawHtmlBody.length > 60000) {
                core.setFailed(`html_body exceeds 60000 characters (${rawHtmlBody.length}).`);
                return;
              }

              if (rawTextBody.length > 20000) {
                core.setFailed(`text_body exceeds 20000 characters (${rawTextBody.length}).`);
                return;
              }

              function escapeHtml(value) {
                return String(value || '')
                  .replace(/&/g, '&amp;')
                  .replace(/</g, '&lt;')
                  .replace(/>/g, '&gt;')
                  .replace(/"/g, '&quot;')
                  .replace(/'/g, '&#39;');
              }

              function formatTextLine(value) {
                return escapeHtml(value).replace(/\bhttps?:\/\/[^\s<]+/g, (url) => `<a href="${url}" style="color: #0f766e; text-decoration: underline;">${url}</a>`);
              }

              function textBodyToHtmlDocument(value) {
                const blocks = [];
                let listItems = [];
                let firstContent = true;
                const flushList = () => {
                  if (listItems.length === 0) return;
                  blocks.push(`<ul style="margin: 12px 0 18px 22px; padding: 0;">${listItems.join('')}</ul>`);
                  listItems = [];
                };

                for (const line of String(value || '').split('\n')) {
                  const trimmed = line.trim();
                  if (!trimmed) {
                    flushList();
                    continue;
                  }

                  const bullet = trimmed.match(/^(?:[-*]|\d+\.)\s+(.+)$/);
                  if (bullet) {
                    listItems.push(`<li style="margin: 0 0 8px 0;">${formatTextLine(bullet[1])}</li>`);
                    continue;
                  }

                  flushList();
                  if (firstContent) {
                    blocks.push(`<h1 style="font-size: 22px; line-height: 1.25; margin: 0 0 16px 0; color: #111827;">${formatTextLine(trimmed)}</h1>`);
                    firstContent = false;
                  } else {
                    blocks.push(`<p style="font-size: 15px; line-height: 1.55; margin: 0 0 12px 0; color: #1f2937;">${formatTextLine(trimmed)}</p>`);
                  }
                }
                flushList();

                return `<!doctype html>\n<html>\n<body>\n<div style="font-family: Arial, sans-serif; color: #1f2937; line-height: 1.5; max-width: 720px; margin: 0 auto; padding: 24px;">${blocks.join('\n')}\n</div>\n</body>\n</html>`;
              }

              function normalizeModelHtml(value) {
                const tagNames = 'html|body|div|table|thead|tbody|tr|td|th|p|h1|h2|h3|h4|h5|h6|a|strong|em|ul|ol|li|span|br|hr';
                let output = String(value || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim();

                output = output.replace(new RegExp(`\\(\\s*\\/\\s*(${tagNames})\\s*\\)`, 'gi'), '</$1>');
                output = output.replace(new RegExp(`\\(\\s*(${tagNames})\\b([^)]*)\\)`, 'gi'), (_match, tag, attrs) => `<${tag}${attrs || ''}>`);
                output = output.replace(new RegExp(`(^|\\n)(\\s*)(${tagNames})\\b([^<>\\n]*?)\\)`, 'gi'), (_match, lineStart, indent, tag, attrs) => `${lineStart}${indent}<${tag}${attrs || ''}>`);
                output = output.replace(/\*\*([^*\n]+)\*\*/g, '<strong>$1</strong>');
                return output;
              }

              const requiredHtmlTagPattern = /<\/?(?:html|body|div|table|tr|td|p|h1|h2|h3|a|strong|em|ul|ol|li|span|br)\b/i;
              let normalizedHtmlBody = normalizeModelHtml(rawHtmlBody);

              const malformedHtmlPatterns = [
                /^\s*(?:div|p|h[1-6]|a|span|strong|em)\b[^<>\n]*[)>]/im,
                /\(\/?(?:div|a|p|h[1-6]|span|strong|em|table|tr|td)\b/i,
                /^\s{0,3}#{1,6}\s+/m,
                /\[[^\]]+\]\([^)]+\)/
              ];

              const blockedPatterns = [
                /<\s*script\b/i,
                /<\s*iframe\b/i,
                /<\s*form\b/i,
                /<\s*img\b/i,
                /<\s*link\b/i,
                /<\s*style\b/i,
                /on\w+\s*=/i,
                /javascript\s*:/i,
                /data\s*:/i
              ];

              for (const pattern of blockedPatterns) {
                if (pattern.test(rawHtmlBody) || pattern.test(normalizedHtmlBody)) {
                  core.setFailed(`html_body contains blocked HTML or attribute pattern: ${pattern}`);
                  return;
                }
              }

              const needsHtmlFallback = !requiredHtmlTagPattern.test(normalizedHtmlBody) || malformedHtmlPatterns.some((pattern) => pattern.test(normalizedHtmlBody));
              const normalizedTextBody = rawTextBody.replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim();
              if (needsHtmlFallback) {
                core.warning('html_body was Markdown or malformed pseudo-HTML after normalization; using a sanitized HTML rendering of text_body.');
                normalizedHtmlBody = textBodyToHtmlDocument(normalizedTextBody);
              }

              const htmlDocument = /<html[\s>]/i.test(normalizedHtmlBody)
                ? normalizedHtmlBody
                : `<!doctype html>\n<html>\n<body>\n${normalizedHtmlBody}\n</body>\n</html>`;

              if (htmlDocument.length > 60000) {
                core.setFailed(`normalized html_body exceeds 60000 characters (${htmlDocument.length}).`);
                return;
              }

              const outputDir = path.join(process.env.RUNNER_TEMP || '/tmp', 'third-place-alert-email');
              fs.mkdirSync(outputDir, { recursive: true });
              const htmlPath = path.join(outputDir, 'body.html');
              const textPath = path.join(outputDir, 'body.txt');
              fs.writeFileSync(htmlPath, htmlDocument, 'utf8');
              fs.writeFileSync(textPath, normalizedTextBody, 'utf8');

              core.setOutput('send', 'true');
              core.setOutput('subject', subject);
              core.setOutput('html_body_file', `file://${htmlPath}`);
              core.setOutput('text_body_file', `file://${textPath}`);

        - name: Send Email
          if: ${{ steps.email.outputs.send == 'true' }}
          uses: dawidd6/action-send-mail@v18
          with:
            server_address: smtp.gmail.com
            server_port: 465
            secure: true
            username: ${{ secrets.MAIL_USERNAME }}
            password: ${{ secrets.MAIL_PASSWORD }}
            subject: ${{ steps.email.outputs.subject }}
            to: segun@charlottethirdplaces.com
            from: Charlotte Third Places Alerts <${{ secrets.MAIL_USERNAME }}>
            body: ${{ steps.email.outputs.text_body_file }}
            html_body: ${{ steps.email.outputs.html_body_file }}
---

<!--
Shared safe-output component for Third Place Alerts.

Agents call `send_email_report` with `subject`, `html_body`, and `text_body`.
The agent never receives Gmail credentials; this post-agent safe-output job sends
the message using `MAIL_USERNAME` and `MAIL_PASSWORD` repository secrets.
-->