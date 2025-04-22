import time
import logging
import argparse
import os
import re
import uuid 

from arkitekt_next import register, easy
from mikro_next.api.schema import (
    from_file_like,
    Dataset,
    ensure_dataset,
    File,
)
from pathlib import Path

@register
def stream_folder(
    folder: Path,
    pattern: str = ".*",
    run_once: bool = False, 
    parent_dataset: Dataset = None,
) -> File:
    """
    Stream files from a folder to Mikro.
    Args:
        folder (str): The folder to stream files from.
        pattern (str): The regex pattern to match files.
        run_once (bool): If True, the function will exit after one run.
        dataset_id (str): The ID of the dataset to upload files to.
        dataset_name (str): The name of the dataset to upload files to.
    """
    print("Start streaming files...")
    keep_running = True
    files_already_uploaded = []
    while keep_running:
        try:
            # Get files that match the pattern and haven't been uploaded
            # ToDo - Add a check for a correct pattern
            file_regex = re.compile(pattern)
            # ToDo - maybe check what files already exist (hash for comparison) 
            #        since we do not want to upload the same file twice
            files_to_upload = [
                f 
                for f in folder.iterdir() # ToDo - check if filewatch is less resource intensive
                if f.is_file() 
                and file_regex.match(str(f))
                and f not in files_already_uploaded
                ]
            print(f"Files to upload: {files_to_upload}")
            print(f"Files already uploaded: {files_already_uploaded}")
            print(f"Files in folder matching regex ({pattern}): {[f for f in folder.iterdir() if f.is_file()]}")

            if not files_to_upload:
                if run_once:
                    print("No new files to upload, exiting.")
                    keep_running = False
                else:
                    print("No new files to upload, waiting...")
                    time.sleep(5)
            else:
                for file in files_to_upload:
                    file_path = file.resolve()
                    try:
                        # Test if file is accessible (not being written to)
                        os.rename(file_path, file_path)
                    except OSError:
                        print(
                            f"Could not access {file.name}. Probably still in use. Will try again later."
                        )
                    
                    print(f"Uploading: {file_path}")
                    try:
                        from_file_like(
                            file=file_path,
                            file_name=file.name,
                            dataset=parent_dataset,
                        )
                        files_already_uploaded.append(file)
                        print(f"Successfully uploaded: {file_path} (ID: {"uploaded_file"})")
                    except Exception as e:
                        print(f"Failed to upload {file_path}: {str(e)}")
                
            time.sleep(1)
                
        except KeyboardInterrupt:
            print("Process interrupted by user")
            break
        except Exception as e:
            print(f"Error in file streaming: {str(e)}")
            if run_once:
                break
            time.sleep(5)  # Wait a bit before retrying in case of errors

if __name__ == "__main__":
    with easy("Stream Files", url="grogu.physbio.uni-frankfurt.de") as app:
        parent_dataset = ensure_dataset(name="Stream Files Test 1")
        folder_to_upload = Path("files")
        stream_folder(
            folder=folder_to_upload,
            pattern=".*",
            run_once=False,
            parent_dataset=parent_dataset,
        )
