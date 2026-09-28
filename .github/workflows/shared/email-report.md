---
# gh-aw v0.88.7 always gates custom safe jobs on the retained output type.
# This read-only job also validates noops, empty output and discarded requests.
jobs:
  validate_alert_output:
    needs: [agent, detection]
    if: always() && !cancelled() && needs.agent.result == 'success' && (needs.detection.result == 'success' || needs.detection.result == 'skipped')
    runs-on: ubuntu-latest
    permissions:
      contents: read
    steps:
      - name: Checkout Trusted Validation Code
        uses: actions/checkout@v7.0.1
        with:
          ref: ${{ github.sha }}
          persist-credentials: false
          sparse-checkout: |
            scripts
            data/alert-recovery
      - name: Download Raw And Ingested Output
        uses: actions/download-artifact@v8.0.1
        with:
          pattern: "{agent,agent-output-fallback}"
          merge-multiple: true
          path: ${{ runner.temp }}/alert-validation
      - name: Download Immutable Pre-Agent Candidates
        uses: actions/download-artifact@v8.0.1
        with:
          name: alert-inputs-${{ github.run_attempt }}
          path: ${{ runner.temp }}/alert-inputs
      - name: Reject Invalid Or Dropped Reports
        env:
          ALERT_KIND: ${{ github.workflow == 'News Alerts' && 'news' || 'reddit' }}
          GH_TOKEN: ${{ github.token }}
          GH_AW_AGENT_OUTPUT: ${{ runner.temp }}/alert-validation/agent_output.json
        run: |
          python3 scripts/alert_delivery.py validate --kind "$ALERT_KIND" --directory "$RUNNER_TEMP/alert-inputs"
safe-outputs:
  github-token: ${{ secrets.GH_AW_GITHUB_TOKEN }}
  jobs:
    send-email-report:
      description: "Submit one final structured selection for trusted rendering, SMTP delivery and delivery-bound history. Success only means queued."
      runs-on: ubuntu-latest
      # Validation is repeated here before SMTP. v0.88.7 cannot depend on a
      # normal custom job; the parallel read-only job covers absent output types.
      if: needs.agent.result == 'success' && (needs.detection.result == 'success' || needs.detection.result == 'skipped')
      output: "Delivery validation completed."
      permissions:
        contents: write
      inputs:
        selection:
          description: 'Base64-encoded UTF-8 JSON list of 1-20 objects: {"id":"candidate title_hash (news) or id (Reddit)","category":"approved category","evidence":"verbatim candidate quote"}. Encoding preserves quotes through gh-aw mention sanitization. No email body or mode.'
          required: true
          type: string
      steps:
        - name: Checkout Trusted Delivery Code
          uses: actions/checkout@v7.0.1
          with:
            ref: ${{ github.sha }}
            persist-credentials: false
            sparse-checkout: |
              scripts
              data/alert-recovery
        - name: Download Immutable Pre-Agent Candidates
          uses: actions/download-artifact@v8.0.1
          with:
            name: alert-inputs-${{ github.run_attempt }}
            path: ${{ runner.temp }}/alert-inputs
        - name: Validate Deliver And Record Receipt
          env:
            ALERT_KIND: ${{ github.workflow == 'News Alerts' && 'news' || 'reddit' }}
            GH_TOKEN: ${{ secrets.GH_AW_GITHUB_TOKEN }}
            MAIL_USERNAME: ${{ secrets.MAIL_USERNAME }}
            MAIL_PASSWORD: ${{ secrets.MAIL_PASSWORD }}
          run: |
            python3 scripts/alert_delivery.py deliver --kind "$ALERT_KIND" --directory "$RUNNER_TEMP/alert-inputs"
---

<!--
Shared news/Reddit safe-output boundary. An immutable upload before the agent
provides trusted candidates; the agent returns only references and evidence.
Only this job has SMTP and memory-write credentials. The memory Contents API
uses SHA-conditional updates; no agent memory patch is applied.
-->
