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
              const htmlBody = String(item.html_body || '');
              const textBody = String(item.text_body || '');

              if (!subject || !htmlBody || !textBody) {
                core.setFailed('subject, html_body, and text_body are required.');
                return;
              }

              if (subject.length > 160) {
                core.setFailed(`subject exceeds 160 characters (${subject.length}).`);
                return;
              }

              if (htmlBody.length > 60000) {
                core.setFailed(`html_body exceeds 60000 characters (${htmlBody.length}).`);
                return;
              }

              if (textBody.length > 20000) {
                core.setFailed(`text_body exceeds 20000 characters (${textBody.length}).`);
                return;
              }

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
                if (pattern.test(htmlBody)) {
                  core.setFailed(`html_body contains blocked HTML or attribute pattern: ${pattern}`);
                  return;
                }
              }

              core.setOutput('send', 'true');
              core.setOutput('subject', subject);
              core.setOutput('html_body', htmlBody);
              core.setOutput('text_body', textBody);

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
            body: ${{ steps.email.outputs.text_body }}
            html_body: ${{ steps.email.outputs.html_body }}
---

<!--
Shared safe-output component for Third Place Alerts.

Agents call `send_email_report` with `subject`, `html_body`, and `text_body`.
The agent never receives Gmail credentials; this post-agent safe-output job sends
the message using `MAIL_USERNAME` and `MAIL_PASSWORD` repository secrets.
-->