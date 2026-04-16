# Impact-CI-data-pipeline
Full documentation : https://wearetogeather-my.sharepoint.com/:w:/g/personal/peter_wearetogether_co_nz/Ed48HUg5uM5JqBsY5k5e9KIB6GAnhfeO63p7dG_MnPIVCg?e=52HFLh

---

## Terraform

### What is Terraform?

Terraform is an **Infrastructure as Code (IaC)** tool by HashiCorp. It lets you define cloud resources (servers, databases, networks, etc.) in configuration files, then automatically creates, updates, or deletes those resources to match your config.

### Core Concepts

**Providers** -- Plugins that connect Terraform to cloud platforms (GCP, AWS, Azure, etc.):

```hcl
provider "google" {
  project = "kiwibank-main"
  region  = "australia-southeast1"
}
```

**Resources** -- The actual infrastructure you want to create. Each resource block declares one piece of infrastructure:

```hcl
resource "google_bigquery_dataset" "facebook_raw" {
  dataset_id = "facebook_raw"
  project    = "kiwibank-main"
  location   = "australia-southeast1"
}
```

**State** -- Terraform keeps a state file (`terraform.tfstate`) that tracks what resources it has created. This is how it knows the difference between "create new" vs "update existing" vs "delete removed".

### How It Works (Workflow)

1. **`terraform init`** -- Downloads the provider plugins (e.g., Google Cloud provider). Creates the `.terraform/` directory.

2. **`terraform plan`** -- Compares your `.tf` files against the current state. Shows what will be created, changed, or destroyed -- without actually doing anything. This is your "dry run".

3. **`terraform apply`** -- Executes the plan. Creates/updates/deletes resources in the cloud to match your config. Updates the state file.

4. **`terraform destroy`** -- Tears down all resources managed by the config.

### Project Structure

```
terraform/clients/<client>/
├── main.tf              # Provider config + module references
├── backend.tf           # Where state is stored (GCS bucket)
├── BigQueryDataset/     # One .tf file per BigQuery dataset
│   ├── facebook-raw.tf
│   ├── facebook-transformed.tf
│   ├── dash-table-credit-card.tf
│   └── ...
└── IamServiceAccount/   # Service account definitions
```

Each `.tf` file in `BigQueryDataset/` declares one BigQuery dataset with:
- **Access controls** (who can read/write -- project owners, tahi service account, etc.)
- **Dataset ID** (the name in BigQuery)
- **Project** (e.g., `kiwibank-main`)
- **Location** (`australia-southeast1`)

When you run `terraform apply`, it creates all those datasets in the GCP project with the exact permissions specified.

### Key Principles

- **Declarative** -- You describe the desired end state, not the steps to get there. Terraform figures out the diff.
- **Idempotent** -- Running `apply` multiple times with the same config produces the same result. If a resource already exists and matches, nothing happens.
- **Version controlled** -- Since it's just text files, you track changes in git. You can review infrastructure changes in PRs just like code.
- **Plan before apply** -- Always review `terraform plan` output before applying to avoid accidental deletions.

### What NOT to Commit to Git

- `.terraform/` -- Downloaded provider binaries (large, platform-specific)
- `.terraform.lock.hcl` -- Provider version lock
- `terraform.tfstate` / `terraform.tfstate.backup` -- Contains sensitive data and should be stored remotely (e.g., GCS bucket as defined in `backend.tf`)
