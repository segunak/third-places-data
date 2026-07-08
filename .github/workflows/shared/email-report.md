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
          description: "Legacy input ignored by the email renderer. HTML is generated from text_body."
          required: false
          type: string
        text_body:
          description: "Plain-text email report body, 20000 characters maximum."
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
              const rawTextBody = String(item.text_body || '');

              if (!subject || !rawTextBody) {
                core.setFailed('subject and text_body are required.');
                return;
              }

              if (subject.length > 160) {
                core.setFailed(`subject exceeds 160 characters (${subject.length}).`);
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
                const links = [];
                let text = String(value || '').replace(/\[\]\([^)]*\)/g, '');
                text = text.replace(/\[([^\]\n]+)\]\((https?:\/\/[^)\s]+)\)/g, (_match, label, url) => {
                  const token = `@@LINK_${links.length}@@`;
                  links.push(`<a href="${escapeHtml(url)}" style="color: #0f766e; text-decoration: underline;">${escapeHtml(label)}</a>`);
                  return token;
                });

                let output = escapeHtml(text)
                  .replace(/\*\*([^*\n]+)\*\*/g, '<strong>$1</strong>')
                  .replace(/\*([^*\n]+)\*/g, '<em>$1</em>')
                  .replace(/\bhttps?:\/\/[^\s<]+/g, (url) => `<a href="${url}" style="color: #0f766e; text-decoration: underline;">${url}</a>`);

                links.forEach((link, index) => {
                  output = output.replace(`@@LINK_${index}@@`, link);
                });
                return output.trim();
              }

              function shouldSkipTextLine(value) {
                const trimmed = String(value || '').trim();
                return !trimmed
                  || /^(?:-{3,}|\*{3,}|_{3,})$/.test(trimmed)
                  || /^\(?\s*!doctype\s+html\s*\)?$/i.test(trimmed)
                  || /^<!doctype\s+html>$/i.test(trimmed)
                  || /^<\/?(?:html|body)\b[^>]*>$/i.test(trimmed)
                  || /^\(\/?(?:html|body)\b[^)]*\)$/i.test(trimmed)
                  || /^\[\]\([^)]*\)$/.test(trimmed);
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
                  if (shouldSkipTextLine(trimmed)) {
                    flushList();
                    continue;
                  }

                  const heading = trimmed.match(/^(#{1,3})\s+(.+)$/);
                  if (heading) {
                    flushList();
                    const level = Math.min(heading[1].length, 3);
                    const styles = {
                      1: 'font-size: 22px; line-height: 1.25; margin: 0 0 16px 0; color: #111827;',
                      2: 'font-size: 18px; line-height: 1.35; margin: 22px 0 10px 0; color: #111827;',
                      3: 'font-size: 16px; line-height: 1.4; margin: 18px 0 8px 0; color: #111827;'
                    };
                    blocks.push(`<h${level} style="${styles[level]}">${formatTextLine(heading[2])}</h${level}>`);
                    firstContent = false;
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

              const normalizedTextBody = rawTextBody.replace(/\r\n/g, '\n').replace(/\r/g, '\n').trim();
              const htmlDocument = textBodyToHtmlDocument(normalizedTextBody);

              if (htmlDocument.length > 60000) {
                core.setFailed(`generated HTML body exceeds 60000 characters (${htmlDocument.length}).`);
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

Agents call `send_email_report` with `subject` and `text_body`. The shared
job generates the Gmail-compatible HTML body deterministically from `text_body`.
The agent never receives Gmail credentials; this post-agent safe-output job sends
the message using `MAIL_USERNAME` and `MAIL_PASSWORD` repository secrets.
-->