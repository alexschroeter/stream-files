from mikro_next.api.schema import (
    from_file_like,
    ensure_dataset,
    Dataset,
)
from arkitekt_next import easy
from pathlib import Path

def recursive_upload(path: Path, parent_dataset: Dataset):
    """
    Recursively upload files from a directory to Mikro.
    Args:
        path (Path): The path to the directory to upload.
        parent_dataset (Dataset): The parent dataset to upload the files to.
    """
    for item in path.iterdir():
        if item.is_dir():
            # Create a new dataset for the directory
            dataset = ensure_dataset(name=item.name)
            print(f"Creating dataset: {item.name}")
            recursive_upload(item, dataset)
        elif item.is_file():
            # Upload the file
            if item.name.startswith("."):
                print(f"Skipping hidden file: {item.name}")
                continue
            try:
                from_file_like(
                    file=item.resolve(), 
                    file_name=str(item.name), 
                    dataset=parent_dataset,
                )
            except Exception as e:
                print(f"Error uploading {item.name}: {e}")
                raise e
            
if __name__ == "__main__":
    with easy("Stream Files", url="grogu.physbio.uni-frankfurt.de") as app:
        parent_dataset = ensure_dataset(name="Parent Dataset Test 1")
        folder_to_upload = Path("files")
        print(f"Uploading files from {folder_to_upload} to Mikro...")
        recursive_upload(folder_to_upload, parent_dataset)
