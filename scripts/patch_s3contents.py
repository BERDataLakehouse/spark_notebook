"""
Patch s3contents library to fix two issues:
1. Directory Visibility: Allow directories without .s3keep files to be listed by providing a fallback creation date.
   Target: genericmanager.py

2. Access Denied Crash: Prevent crash during initialization when user has prefix-restricted access
   (cannot create root bucket).
   Target: s3_fs.py
"""

import os
import sys
import re

GENERIC_MANAGER_FILE = "/opt/conda/lib/python3.13/site-packages/s3contents/genericmanager.py"
S3_FS_FILE = "/opt/conda/lib/python3.13/site-packages/s3contents/s3_fs.py"


def patch_genericmanager():
    """allows directories without .s3keep to be listed"""
    if not os.path.exists(GENERIC_MANAGER_FILE):
        print(f"Error: Target file {GENERIC_MANAGER_FILE} not found.")
        return False

    print(f"Patching {GENERIC_MANAGER_FILE}...")

    with open(GENERIC_MANAGER_FILE, "r") as f:
        content = f.read()

    search_pattern = """                except FileNotFoundError:
                    pass"""

    replace_pattern = """                except FileNotFoundError:
                    # PATCHED by BERDL: Allow directories without .s3keep to be listed
                    s3_detail["LastModified"] = DUMMY_CREATED_DATE"""

    if search_pattern in content:
        new_content = content.replace(search_pattern, replace_pattern)
        with open(GENERIC_MANAGER_FILE, "w") as f:
            f.write(new_content)
        print("Successfully patched genericmanager.py")
        return True
    elif 's3_detail["LastModified"] = DUMMY_CREATED_DATE' in content:
        print(f"{GENERIC_MANAGER_FILE} is already patched.")
        return True
    else:
        print(f"Error: Could not find search pattern in {GENERIC_MANAGER_FILE}. It might have changed.")
        return False


def patch_s3fs_init():
    """makes initialization lenient for prefix-restricted policies"""
    if not os.path.exists(S3_FS_FILE):
        print(f"Error: Target file {S3_FS_FILE} not found.")
        return False

    print(f"Patching {S3_FS_FILE} for prefix-restricted policies...")

    with open(S3_FS_FILE, "r") as f:
        content = f.read()

    if "# BERDL patch" in content:
        print(f"{S3_FS_FILE} is already patched.")
        return True

    if "def init(self):" in content and "self.mkdir" in content:
        # Find indentation of the self.mkdir("") call
        mkdir_match = re.search(r'([ \t]*)self\.mkdir\(""\)', content)

        if mkdir_match:
            original_line = mkdir_match.group(0)
            indent = mkdir_match.group(1)

            # Wrap in try/except with correct indentation
            new_block = f"""{indent}try:
{indent}    self.mkdir("")
{indent}except (ClientError, PermissionError) as ex:
{indent}    self.log.warning(f"Could not create .s3keep marker: {{ex}}")  # BERDL patch"""

            new_content = content.replace(original_line, new_block, 1)

            with open(S3_FS_FILE, "w") as f:
                f.write(new_content)
            print("Successfully patched s3_fs.py")
            return True
        else:
            print('Error: Could not find self.mkdir("") pattern to patch.')
            return False
    else:
        print("Error: init() method not found in expected format.")
        return False


if __name__ == "__main__":
    success_gm = patch_genericmanager()
    success_fs = patch_s3fs_init()

    if not success_gm or not success_fs:
        sys.exit(1)
