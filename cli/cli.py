import pandas as pd
import click
import os
import hashlib
import logging

@click.command(help="Command to help pseudonymise data.")
@click.option("-s","--salt",help="salt hash",required=True,type=str)
@click.option("--person-id","-i","--id",help="name of the person_id",required=True,type=str)
@click.option("--output-folder","-o",help="path of the output folder",required=True,type=str)
@click.option("--chunksize",help="set the chunksize when loading data",type=int,default=None)
@click.argument("input",required=True)
def csv(input,output_folder,chunksize,salt,person_id):
    logging.info(f"Working on file {input}, pseudonymising column '{person_id}' with salt '{salt}'")

    #create the dir
    os.makedirs(output_folder,exist_ok=True)
    f_out = f"{output_folder}{os.path.sep}{os.path.basename(input)}"

    logging.info(f"Saving new file to {f_out}")

    #load data
    data = pd.read_csv(input,chunksize=chunksize,dtype=str)
    i = 0 
    while True:
        data[person_id] =  data[person_id].apply(
            lambda x: hashlib.sha256(x.encode("UTF-8")).hexdigest()
        )
        logging.debug(data[person_id])
        
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
    
    logging.info(f"Done with {f_out}!")

@click.group()
def pseudonymise():
    logging.basicConfig(level=logging.INFO,datefmt='%m/%d/%Y %I:%M:%S %p',format='%(levelname)s  %(asctime)s %(message)s')
    pass

pseudonymise.add_command(csv, "csv")

if __name__ == "__main__":
    pseudonymise()
