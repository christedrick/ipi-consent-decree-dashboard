# Security Setup — remaining manual steps

Two items from the security audit need Google Cloud console access (I can't
do them from the CLI on this machine). Both are ~10 minutes.

## 1. Split the service account (least privilege)

Today one service account with full edit rights serves both the pipeline
(needs write) and the dashboard (needs read + write to two tables). Give the
dashboard its own limited account:

1. console.cloud.google.com → project **ipi-consent-decree-dashboard** →
   IAM & Admin → Service Accounts → **Create service account**
   - Name: `dashboard-reader`
   - Grant role: **BigQuery Job User** (project level)
2. BigQuery → dataset `ipi_intelligence` → Sharing → Permissions →
   Add principal `dashboard-reader@...iam.gserviceaccount.com` with role
   **BigQuery Data Viewer** (dataset level).
3. For the two tables the app writes (`research_queue`,
   `stakeholders_staging`): open each table → Share → add the same
   principal with **BigQuery Data Editor** (table level).
4. Service account → Keys → Add key → JSON. Download it.
5. share.streamlit.io → your app → Settings → Secrets → replace the
   `[gcp_service_account]` block with the new key's contents.
6. After confirming the app works: delete the OLD key from the original
   service account (IAM → Service Accounts → keys tab) so the wide-permission
   key that lived in OneDrive is dead.

## 2. Rotate the pipeline key (it lived in a synced folder)

The original key file sat in OneDrive-synced storage until 2026-07-05 (it's
now at `~/.config/ipi-etl/service-account.json`, outside sync). Because
copies may exist in OneDrive version history:

1. IAM & Admin → Service Accounts → the original account → Keys →
   **Add key** (JSON) → save the download over
   `~/.config/ipi-etl/service-account.json`  (`chmod 600` it)
2. Delete the old key (created 2026-03-25) from the same screen.
3. Optionally: purge OneDrive version history for the old project folder.

## Already done in code (2026-07-05)

- All pipeline scripts + refresh.sh + daily_incidents.sh now read
  credentials from `~/.config/ipi-etl/` (outside OneDrive sync)
- The `.env` files left in the repo directories contain only the
  non-secret project ID
- All dashboard SQL that writes to BigQuery is parameterized
- HubSpot token (when you create it) goes in `~/.config/ipi-etl/.env` as
  `HUBSPOT_PRIVATE_APP_TOKEN=...` — scope it to contacts + contact-schema
  read/write only
