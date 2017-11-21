###################### INFORMATIONS #############################
#           This program can call different tools to edit
#           the Aktienfuehrer-Database.
# Program:  **akf-dbTools**
# Info:     **Python 3.6**
# Author:   **Jan Kamlah**
# Date:     **14.11.2017**

######### IMPORT ############
import dblib.kennGetter as kennGetter
import dblib.refGetter as refGetter
import configparser
import argparse
import textwrap
import os
####################### CMD-PARSER-SETTINGS ########################
def get_parser():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,description=textwrap.dedent("You can choose between different dbTools:\n"
                                                 "refGetter: Get all Years of the same referenz and write it to 'Jahresspanne'.\n"
                                                 "kennGetter: Get all the WKN/ISIN of the same referenz and write it to 'Kennnummer'.\n"))
    parser.add_argument("--input", type=str,default="",help='Input db directory or type it into the config file.')
    parser.add_argument("--tool", type=str, choices=[0, 1], default=1,
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
    if args.input == "":
        # The filespath are stored in the config.ini file.
        # And can be changed there.
        config = configparser.ConfigParser()
        config.sections()
        config.read('./dblib/config.ini')
        # For later use to iterate over all dir
        dbPath = config['DEFAULT']['DBPath']
    tools = {
        0: refGetter.akf_refgetter,
        1: kennGetter.akf_kenngetter,
    }
    tools[args.tool](dbPath)
    print("Finished!")
