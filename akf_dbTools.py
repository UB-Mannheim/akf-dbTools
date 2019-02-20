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
import dblib.json2sqlite as json2sqlite
import configparser
import argparse
import textwrap
import os
####################### CMD-PARSER-SETTINGS ########################
def get_parser():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,description=textwrap.dedent("You can choose between different dbTools:\n\n"
                                                 "json2sqlite for books(0):\n"
                                                 "Parse the gained json from books.\n\n"
                                                 "json2sqlite for cds(1):\n"
                                                 "Parse the gained json from cds.\n"
                                                 "refGetter(2):\n"
                                                 "Get all Years of the same referenz and write it to 'Jahresspanne'.\n\n"
                                                 "kennGetter(3):\n"
                                                 "Get all the WKN/ISIN of the same referenz and write it to 'Kennnummer'.\n"))
    parser.add_argument("--db", type=str,default="",help='Input db directory or type it into the config file.')
    parser.add_argument("--files", type=str, default="", help='Input input file directory or type it into the config file.')
    parser.add_argument("--tool", type=str, choices=[0, 1, 2, 3], default=2,
                        help='Choose the tool(1:json2sqlite for books, 1:json2sqlite for cds,3:ref-Getter, 3:kennGetter), default: %(default)s')
    args = parser.parse_args()
    return args

################ START ################
if __name__ == "__main__":
    """
    Entrypoint: Searches for the files and parse them into the mainfunction (can be multiprocessed)
    """
    config = configparser.ConfigParser()
    config.sections()
    config.read('./dblib/config.ini')
    args = get_parser()
    if args.db != "":
        config['DEFAULT']['DBPath'] = args.db
    if args.files != "":
        config['DEFAULT']['DBPath'] = args.files
    tools = {
        0: json2sqlite.book,
        1: json2sqlite.cd,
        2: refGetter.akf_refgetter,
        3: kennGetter.akf_kenngetter,
    }
    tools[args.tool](config)
    print("Finished!")
