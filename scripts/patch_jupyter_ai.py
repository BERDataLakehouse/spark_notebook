import os
import sys
import glob


def patch_jupyter_ai():
    # --- Backend Patching (Python) ---
    site_packages = [p for p in sys.path if "site-packages" in p]
    print(f"Searching in site-packages locations: {site_packages}")

    target_files = []

    packages_to_patch = ["jupyter_ai", "jupyter_ai_magics"]

    for sp in site_packages:
        for pkg_name in packages_to_patch:
            package_path = os.path.join(sp, pkg_name)
            if os.path.exists(package_path):
                print(f"Found package {pkg_name} at: {package_path}")
                target_files.extend(
                    glob.glob(os.path.join(package_path, "**", "*.py"), recursive=True)
                )

    # Python replacements - Name Only
    replacements = [
        ("Jupyternaut", "KBaseLakehouseAgent"),
    ]

    files_modified = 0
    for file_path in target_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            new_content = content
            modified = False
            for old, new in replacements:
                if old in new_content:
                    print(f"  Found '{old}' in {file_path}")
                    new_content = new_content.replace(old, new)
                    modified = True
            if modified:
                print(f"Patching backend file {file_path}...")
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(new_content)
                files_modified += 1
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    print(f"Backend patching complete. Modified {files_modified} files.")

    # --- Frontend Patching (JS/Extensions) - Name Only ---
    labextensions_path = "/opt/conda/share/jupyter/labextensions"
    js_files = glob.glob(os.path.join(labextensions_path, "**", "*.js"), recursive=True)
    print(f"Found {len(js_files)} JS files in labextensions to scan.")

    frontend_files_modified = 0

    for js_path in js_files:
        try:
            with open(js_path, "r", encoding="utf-8") as f:
                content = f.read()

            new_content = content
            modified = False

            # Patch "Jupyternaut" text in UI
            if "Jupyternaut" in new_content:
                print(f"  Found 'Jupyternaut' text in {js_path}")
                new_content = new_content.replace("Jupyternaut", "KBase Agent")
                modified = True

            # Patch "Jupyter AI Chat" title
            if "Jupyter AI Chat" in new_content:
                print(f"  Found 'Jupyter AI Chat' title in {js_path}")
                new_content = new_content.replace("Jupyter AI Chat", "KBase Agent")
                modified = True

            if modified:
                print(f"Patching frontend file {js_path}...")
                with open(js_path, "w", encoding="utf-8") as f:
                    f.write(new_content)
                frontend_files_modified += 1

        except Exception as e:
            print(f"Skipping {js_path} due to error: {e}")

    print(f"Frontend patching complete. Modified {frontend_files_modified} files.")


if __name__ == "__main__":
    patch_jupyter_ai()
