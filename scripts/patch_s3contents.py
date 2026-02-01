import os
import sys

# Define the file to patch
# In the Docker container, it is located at:
TARGET_FILE = "/opt/conda/lib/python3.13/site-packages/s3contents/genericmanager.py"


def patch_genericmanager():
    if not os.path.exists(TARGET_FILE):
        print(f"Error: Target file {TARGET_FILE} not found.")
        sys.exit(1)

    print(f"Patching {TARGET_FILE}...")

    with open(TARGET_FILE, "r") as f:
        content = f.read()

    # The code we want to patch looks like this:
    #                 try:
    #                     lstat = await self.fs.fs._info(dir_path)
    #                     s3_detail["LastModified"] = lstat["LastModified"]
    #                 except FileNotFoundError:
    #                     pass

    # We want to change the 'pass' to set a default date.

    search_pattern = """                except FileNotFoundError:
                    pass"""

    replace_pattern = """                except FileNotFoundError:
                    # PATCHED by BERDL: Allow directories without .s3keep to be listed
                    s3_detail["LastModified"] = DUMMY_CREATED_DATE"""

    if search_pattern in content:
        new_content = content.replace(search_pattern, replace_pattern)
        with open(TARGET_FILE, "w") as f:
            f.write(new_content)
        print("Successfully patched genericmanager.py")
    elif 's3_detail["LastModified"] = DUMMY_CREATED_DATE' in content:
        print("File is already patched.")
    else:
        print("Error: Could not find search pattern in file. It might have changed.")
        # Print a snippet to help debugging if needed
        # print("File content snippet around target area:")
        # print(content[content.find("get_content_s3_metadata"):content.find("get_content_s3_metadata")+1000])
        sys.exit(1)


if __name__ == "__main__":
    patch_genericmanager()
