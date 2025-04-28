import os
import subprocess
from functools import reduce
from concurrent.futures import ThreadPoolExecutor
import polars as pl
        
    
def copy_from_bucket(file_path: str, bucket_id: str = None) -> None:
    """
    Copies a file from specified bucket and path into the enviroment workspace.
    
    Parameters:
    -----------
    file_path: str
        Path to the file to copy from bucket
    bucket_id:[Optional]
        The bucket id to copy the file from. Defaults to the environment variable 'WORKSPACE_BUCKET'.
    
    Returns:
    -------
    None returned. File from bucket is copied to the work environment.

    Example:
    --------
    copy_from_bucket('datasets/fitbit.csv')
    """
    
    if bucket_id == None:
        bucket_id = os.getenv('WORKSPACE_BUCKET')

    os.system(f"gsutil cp '{bucket_id}/{file_path}' .")
    print(f'[INFO] {file_path} is successfully downloaded into your working space')
        
def read_from_bucket(file_name: str = None, 
                    file_folder: str = None, 
                    bucket_id: str = None, 
                    lazy: bool = True, 
                    stack: bool = False) -> pl.DataFrame:
    """Copies and reads a csv file from bucket
    
    :param file_name: the name of the file that is in the file folder
    :param file_folder: the folder the file is in
    :param bucket_id: The bucket id to read the file from. Defaults to environment variable WORKSPACE_BUCKET.
    :param lazy: Either to read or scan csv file. Check polars documentation for this behaviour. Defaults to True.
    :returns: Polars Dataframe is returned. Read might be set to lazy to scan the csv instead of reading. Check polars documentation for this behaviour.

    Example:
    --------
    df = read_from_bucket(file_name = 'fitbit.csv', file_folder = 'datasets')
    """
    if file_name is None and file_folder is None:
        raise ValueError("Neither file_path nor file_folder is specified")

    if file_name is not None and file_name.split(".")[-1] != "csv":
            raise ValueError("The specified file is not csv format hence cannot be loaded")
    
    if bucket_id == None:
        bucket_id = os.getenv('WORKSPACE_BUCKET')

    if file_folder is not None and not os.path.isdir(f'bucket_io/{file_folder}'):
        os.makedirs(f'bucket_io/{file_folder}')
    
    if file_name is not None:
        if file_folder is None:
            os.system(f"gcloud storage cp '{bucket_id}/{file_name}' 'bucket_io'")
        else:
            os.system(f"gcloud storage cp '{bucket_id}/{file_folder}/{file_name}' 'bucket_io/{file_folder}'")
        if lazy:
            return pl.scan_csv(f'bucket_io/{file_folder}/{file_name}')
        else:
            return pl.read_csv(f'bucket_io/{file_folder}/{file_name}')
    else:
        file_targets = ls_bucket(target=file_folder, bucket_id=bucket_id, return_list=True)
        os.system(f"gcloud storage cp '{bucket_id}/{file_folder}/*.csv' 'bucket_io/{file_folder}'")
        
        dfs = []
        for f in file_targets:
            if lazy:
                dfs.append(pl.scan_csv(f.replace(bucket_id, "bucket_io")))
            else:
                dfs.append(pl.read_csv(f.replace(bucket_id, "bucket_io")))
        if stack and lazy:
            return reduce(lambda x,y: x.vstack(y), [df.collect() for df in dfs])
        elif stack:
            return reduce(lambda x,y: x.vstack(y), dfs)
        else:
            return dfs


def copy_to_bucket(file_name: str, target: str, bucket_id: str = None) -> None:
    """Copies a file from enviroment workspace to designated bucket folder
    
    Parameters:
    -----------
    file_name: str
        Path to file in the environment to copy to bucket
    target: str
        Path to copy the file
    bucket_id:[Optional]
        The bucket id to copy the file to. Defaults to environment variable WORKSPACE_BUCKET.
    
    Returns:
    -------
    None returned. File from the environment is copied to the given location.

    Example:
    --------
    copy_from_bucket('fitbit.csv', 'datasets/fitbit.csv')
    """
    
    if bucket_id == None:
       bucket_id = os.getenv('WORKSPACE_BUCKET')
    target_folder = target.split("/")
    if len(target_folder)>1 and not os.path.isdir("/".join(target_folder[:-1])):
        os.makedirs("/".join(target_folder[:-1]))

    os.system(f"gsutil cp {file_name} {bucket_id}/{target}")

def ls_bucket(target: str = None, bucket_id: str = None, return_list:bool=False) -> None:
    """List the files in the given directory in the given bucket
    
    Parameters:
    -----------
    target: str [Optional]
        Path to folder in the bucket to list the files. Defaults to workspace folder.
    bucket_id:[Optional]
        The bucket id to list the files from. Defaults to environment variable WORKSPACE_BUCKET.
    
    Returns:
    -------
    None returned. Files from the given directory is listed.

    Example:
    --------
    ls_bucket('datasets')
    """
    
    if bucket_id == None:
       bucket_id = os.getenv('WORKSPACE_BUCKET')
    
    if target == None:
        cmd = f"gsutil ls {bucket_id}"
    else:
        cmd = f"gsutil ls {bucket_id}/{target}"
    
    if return_list:
        return subprocess.check_output(cmd, shell=True).decode('utf-8').split("\n")[:-1]
    else:
        os.system(cmd)

def remove_from_bucket(file_path: str, bucket_id:str = None) -> None:
    """Removes the file from the bucket

    Parameters:
    -----------
    file_path: str
        Path to file to remove.
    bucket_id:[Optional]
        The bucket id to remove the file from. Defaults to environment variable WORKSPACE_BUCKET.
    
    Returns:
    -------
    None returned. File from bucket is removed.

    Example:
    --------
    remove_from_bucket('datasets/fitbit.csv')
    """
    
    if bucket_id == None:
       bucket_id = os.getenv('WORKSPACE_BUCKET')
    os.system(f"gsutil rm {bucket_id}/{file_path}")

def write_to_bucket(file: pl.DataFrame, target: str, bucket_id: str =  None) -> None:
    """Writes the given file to the given bucket location
    
    Parameters:
    -----------
    file: DataFrama
        Polars DataFrame to write to the bucket
    target: str
        Path to write the file
    bucket_id:[Optional]
        The bucket id to write the file. Defaults to environment variable WORKSPACE_BUCKET.
    
    Returns:
    -------
    None returned. DataFrame is written to the given bucket.

    Example:
    --------
    write_from_bucket(fitbit_dat, 'datasets/fitbit.csv')
    """
    
    file.write_csv(f'bucket_io/temp.csv')
    copy_to_bucket(file_name = 'bucket_io/temp.csv', target=target, bucket_id=bucket_id)