# BERDL Notebook

[![codecov](https://codecov.io/gh/BERDataLakehouse/spark_notebook/branch/main/graph/badge.svg)](https://codecov.io/gh/BERDataLakehouse/spark_notebook)

* Set up the user's environment
* Installs custom dependencies

# Sample env
* The environment gets injected by the KubeSpawner
```
# Core Authentication
KBASE_AUTH_TOKEN=your_kbase_token_here
CDM_TASK_SERVICE_URL=https://your-cdm-service.example.com
USER=your_kbase_username

# MinIO Configuration
MINIO_ENDPOINT=https://your-minio.example.com
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key
MINIO_SECURE=true

# Spark Configuration
BERDL_POD_IP=10.0.0.1
SPARK_MASTER_URL=spark://spark-master:7077

# Hive Configuration
BERDL_HIVE_METASTORE_URI=thrift://hive-metastore:9083

# Optional Spark Tuning
MAX_EXECUTORS=10
EXECUTOR_CORES=2
EXECUTOR_MEMORY=4g

```



# Welcome to Your BERDL Environment

* nginx.ingress.kubernetes.io/proxy-body-size is set to 64m, so you cannot upload files bigger than that directly

## Shell Customization (`.custom_profile`)

Your command-line shell is configured by a file named `.bash_profile` in your home directory (`~/`). While you can look at this file, **you should not edit it directly**, as it may be reset by the system to ensure a consistent base environment.

For your personal customizations—such as aliases, custom functions, or modifying your `PATH`—you should use the `.custom_profile` file.

* **File Location:** `~/.custom_profile`
* **Purpose:** A safe place for all your personal shell settings.
* **Behavior:** This file is created for you if it doesn't exist. **It will never be overwritten by the system**, so any changes you make are permanent.

### Example: How to use `.custom_profile`

Open the file `~/.custom_profile` with a text editor and add your settings.

```bash
# Example content for ~/.custom_profile

# Add your favorite command-line aliases
alias ll='ls -alF'
alias glog="git log --graph --pretty=format:'%Cred%h%Creset -%C(yellow)%d%Creset %s %Cgreen(%cr) %C(bold blue)<%an>%Creset' --abbrev-commit"

# Add a custom location to your system's PATH
export PATH="$HOME/bin:$HOME/.local/bin:$PATH"

# Set your preferred text editor
export EDITOR='vim'