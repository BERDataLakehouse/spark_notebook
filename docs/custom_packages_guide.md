# Guide: Installing Custom Python Packages

The default BERDL notebook environment comes with many pre-installed packages. However, if you need additional Python packages for your analysis, you can create a custom virtual environment that persists across sessions.

## Creating a Custom Virtual Environment

### Step 1: Open a Terminal

In JupyterLab, open a terminal:
- Click **File** → **New** → **Terminal**

### Step 2: Create a Virtual Environment That Inherits System Packages

Create a new virtual environment in your home directory with access to all the default BERDL packages:

```bash
python -m venv --system-site-packages ~/my_venv
```

The `--system-site-packages` flag allows your custom environment to inherit all packages from the default BERDL environment (PySpark, Delta Lake, pandas, etc.) while still allowing you to install additional packages.

You can name the environment anything you like (e.g., `~/ml_env`, `~/viz_env`, etc.)

> **💡 Tip:** If you want a completely isolated environment instead (without inheriting BERDL packages), omit the `--system-site-packages` flag:
> ```bash
> python -m venv ~/my_isolated_env
> ```

### Step 3: Activate the Virtual Environment

```bash
source ~/my_venv/bin/activate
```

You should see `(my_venv)` appear in your terminal prompt.

### Step 4: Verify Inherited Packages (Optional)

You can verify that you have access to the default BERDL packages:

```bash
pip list | grep pyspark
pip list | grep delta-spark
pip list | grep pandas
pip list | grep berdl_notebook_utils
pip list | grep datalake-mcp-server-client
```

You should see all the pre-installed packages from the base environment.

### Step 5: Install Additional Packages

Install only the additional packages you need, including `ipykernel`. In this example, we'll install `plotly` for visualization:

```bash
pip install ipykernel plotly
```

> **💡 General Pattern:**
> ```bash
> pip install ipykernel <additional-packages>
> ```
> Replace `<additional-packages>` with the packages you need (e.g., `plotly`, `scikit-learn`, `tensorflow`, etc.)

You can install multiple packages at once:

```bash
pip install ipykernel plotly seaborn scikit-learn matplotlib
```

Since you're using `--system-site-packages`, you don't need to reinstall packages that are already in the base BERDL environment.

### Step 6: Register as Jupyter Kernel

Register your virtual environment as a Jupyter kernel so you can use it in notebooks:

```bash
python -m ipykernel install --user --name my_venv --display-name "Python (my_venv)"
```

- `--name my_venv`: Internal kernel name (must be unique)
- `--display-name "Python (my_venv)"`: Name shown in JupyterLab

### Step 7: Refresh Your Browser

Refresh your browser page to see the new kernel option.

### Step 8: Select Your Custom Kernel

**For a new notebook:**
1. Click **File** → **New** → **Notebook**
2. Select "Python (my_venv)" from the kernel list

**For an existing notebook:**
1. Click on the kernel name in the top-right corner of your notebook
2. Select "Python (my_venv)" from the dropdown menu

## Using Your Custom Environment

Once you've switched to your custom kernel, you can import and use the packages you installed:

```python
import plotly.graph_objects as go
import seaborn as sns
import matplotlib.pyplot as plt

# Create a simple Plotly visualization
fig = go.Figure(data=go.Scatter(x=[1, 2, 3, 4], y=[10, 15, 13, 17]))
fig.update_layout(title="My Custom Plot")
fig.show()
```

## Managing Multiple Virtual Environments

You can create multiple virtual environments for different projects or purposes:

```bash
# Machine learning environment (inherits BERDL packages + adds ML tools)
python -m venv --system-site-packages ~/ml_env
source ~/ml_env/bin/activate
pip install scikit-learn tensorflow ipykernel
python -m ipykernel install --user --name ml_env --display-name "Python (ML)"
deactivate

# Data visualization environment (inherits BERDL packages + adds viz tools)
python -m venv --system-site-packages ~/viz_env
source ~/viz_env/bin/activate
pip install plotly bokeh altair ipykernel
python -m ipykernel install --user --name viz_env --display-name "Python (Visualization)"
deactivate

# Bioinformatics environment (inherits BERDL packages + adds bio tools)
python -m venv --system-site-packages ~/bio_env
source ~/bio_env/bin/activate
pip install biopython ipykernel
python -m ipykernel install --user --name bio_env --display-name "Python (Bioinformatics)"
deactivate
```

Each kernel runs with:
- **All the default BERDL packages** (PySpark, Delta Lake, pandas, etc.)
- **Its own additional specialized packages**
- **Access to BERDL helper functions** (`get_spark_session()`, `display_df()`, etc.)

## Useful Commands

### List All Available Kernels

```bash
jupyter kernelspec list
```

This shows all registered kernels and their locations.

### Remove a Kernel

If you no longer need a custom kernel:

```bash
jupyter kernelspec uninstall my_venv
```

Note: This only removes the kernel registration, not the virtual environment itself.

### Remove a Virtual Environment

To completely delete a virtual environment and free up disk space:

```bash
rm -rf ~/my_venv
```

**Warning:** Make sure to uninstall the kernel first if you've registered it.

### Update Packages in a Virtual Environment

To update packages in your custom environment:

```bash
source ~/my_venv/bin/activate
pip install --upgrade plotly
```

## Important Notes

> **💡 Persistence:**
> - Virtual environments are stored in your home directory (`~/`) and persist across notebook sessions
> - Your files will remain available even after your JupyterHub server restarts
> - Each kernel maintains its own independent set of packages

> **💾 Disk Space:**
> - Virtual environments can consume significant disk space, especially with large packages like TensorFlow
> - Monitor your home directory usage and remove unused environments when no longer needed
> - Check disk usage with: `du -sh ~/my_venv`

> **🔄 Kernel Restart:**
> - If you install new packages in an active kernel, you may need to restart the kernel to use them
> - Click **Kernel** → **Restart Kernel** in JupyterLab

## Troubleshooting

### Kernel Doesn't Appear After Registration

1. Refresh your browser page
2. If still not visible, verify registration: `jupyter kernelspec list`
3. Check for errors in the terminal when you ran the install command

### Import Error After Installing Package

1. Verify you're using the correct kernel (check top-right corner of notebook)
2. Restart the kernel: **Kernel** → **Restart Kernel**
3. Verify package installation:
   ```python
   import sys
   print(sys.executable)
   !pip list | grep plotly
   ```

### Virtual Environment Activation Fails

Make sure you're using the correct path:
```bash
source ~/my_venv/bin/activate
```

If the environment doesn't exist, create it first with `python -m venv ~/my_venv`

## Best Practices

1. **Use descriptive names**: Name your environments based on their purpose (e.g., `ml_env`, `viz_env`)
2. **Document your packages**: Keep a `requirements.txt` file in your notebook directory:
   ```bash
   source ~/my_venv/bin/activate
   pip freeze > ~/requirements.txt
   ```
3. **Clean up unused environments**: Regularly remove environments you no longer use
4. **Request common packages**: If you find yourself installing the same packages repeatedly, contact the BERDL Platform team to have them added to the base image

## Getting Help

If you need packages added to the default BERDL environment or encounter issues with custom kernels, please contact the BERDL Platform team for assistance.
