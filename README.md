# BERDL Notebook

* Set up the user's environment
* Installs custom dependencies


# Dockerfile.base Image
* Use the official JupyterHub base image
* Installs python dependencies
* This takes a long time to build, so it is pushed only on release.

# Dockerfile build
* Build the Docker image using the base image and the notebook image
* Copies the necessary files and configurations


# Required ENV Vars

The following env vars must be provided by the jupyterhub
* KBASE_ORIGIN
* SPARK_JOB_LOG_DIR_CATEGORY
* CDM_TASK_SERVICE_URL
* SPARK_CLUSTER_MANAGER_API_URL
* KBASE_AUTH_TOKEN
* SPARK_MASTER_URL
* SPARK_DRIVER_HOST as BERDL_POD_IP
 

# Contributing
See the [CONTRIBUTING.md](../CONTRIBUTING.md) file for guidelines on how to contribute to this project.



# Welcome to Your BERDL Environment
* nginx.ingress.kubernetes.io/proxy-body-size is set to 64m, so you cannot upload files bigger than that directly
* 


This environment has been automatically configured to provide a consistent and powerful experience. This document explains how the setup works and, most importantly, how you can customize it.

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