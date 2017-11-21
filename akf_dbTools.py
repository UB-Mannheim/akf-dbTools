###################### INFORMATIONS #############################
#           This program can call different tools to edit the
#           the Aktienfuehrer-Database.
# Program:  **akf-dbTools**
# Info:     **Python 3.6**
# Author:   **Jan Kamlah**
# Date:     **14.11.2017**

######### IMPORT ############
import dblib
import configparser
import argparse
import os
####################### CMD-PARSER-SETTINGS ########################
def get_parser():
    parser = argparse.ArgumentParser(description="You can choose between different dbTools")
    parser.add_argument("--input", type=str,default="",help='Input db directory or type it into the config file.')
    parser.add_argument("--tool", type=str, choices=[0, 1], default=0,
                        help='Choose the tool(0:ref-Getter, 1:kennGetter), default: %(default)s')
    args = parser.parse_args()
    return args

################ START ################
if __name__ == "__main__":
    """
    Entrypoint: Searches for the files and parse them into the mainfunction (can be multiprocessed)
    """
    args = get_parser()
    dbPath = os.path.abspath(args.input)
    if dbPath == "":
        # The filespath are stored in the config.ini file.
        # And can be changed there.
        config = configparser.ConfigParser()
        config.sections()
        config.read('./akf-dbToolslib/config.ini')
        # For later use to iterate over all dir
        dbPath = config['DEFAULT']['DBPath']
    options = {
        1: dblib.akf_refgetterr,
        2: dblib.akf_kenngetter,
    }
    options[args.tool](dbPath)
    print("Finished!")
