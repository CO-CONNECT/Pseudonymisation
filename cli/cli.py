import pandas as pd
import click
import os
import sys
import hashlib
from loguru import logger

@click.command(help="Command to pseudonymise csv files, given a salt, the name of the columns to pseudonymise and the input file.")
@click.option("-s","--salt",help="salt hash",required=True,type=str)
@click.option("columns","--column","-c","--id",help="name of the identifier columns",required=True,type=str,multiple=True)
@click.option("--output-folder","-o",help="path of the output folder",required=True,type=str)
@click.option("--chunksize",help="set the chunksize when loading data, useful for large files",type=int,default=None)
@click.argument("input",required=True)
def csv(input,output_folder,chunksize,salt,columns):
    columns = list(columns)
    logger.info(f"Working on file {input}, pseudonymising columns '{columns}' with salt '{salt}'")

    #create the dir
    os.makedirs(output_folder,exist_ok=True)
    f_out = f"{output_folder}{os.path.sep}{os.path.basename(input)}"

    logger.info(f"Saving new file to {f_out}")

    #load data
    data = pd.read_csv(input,chunksize=chunksize,dtype=str)
    i = 0 
    while True:
        for col in columns:
            data[col] =  data[col].apply(
                lambda x: hashlib.sha256((x+salt).encode("UTF-8")).hexdigest()
            )
            logger.debug(data[col])
        
        mode = 'w'
        header=True
        if i > 0 :
            mode = 'a'
            header=False
        
        data.to_csv(f_out,mode=mode,header=header,index=False)
        
        i+=1

        if isinstance(data,pd.DataFrame):
            break
        else:
            try:
                data.next()
            except StopIteration:
                break
    
    logger.info(f"Done with {f_out}!")

@click.group()
def pseudonymise():
    pass

pseudonymise.add_command(csv, "csv")

if __name__ == "__main__":
    pseudonymise()
