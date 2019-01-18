###################### INFORMATIONS #############################
#           It gets the Kennnummer values, reads all years which
#           are bind to it and pretty prints the data into the table.
# Program:  **akf-kennGetter**
# Info:     **Python 3.6**
# Author:   **Jan Kamlah**
# Date:     **20.11.2017**

######### IMPORT ############
from sqlalchemy import create_engine, MetaData, select
import configparser
################ START ################

def akf_kenngetter(config):
    """
    Main function of the akf-kennGetter!
    """
    print("Start kennGetter")
    #Connection to SQLite
    dbPath = config['DEFAULT']['DBPath']
    db_akf = dbPath
    engine = create_engine(db_akf)
    conn = engine.connect()

    # Create a MetaData instance
    metadata = MetaData(bind=engine, reflect=True)

    # Get all the referenz values
    s = select([metadata.tables['Main'].c.referenz])
    result = conn.execute(s)
    mainresults = result.fetchall()
    # Get all the years which are bind to the referenz
    for ref in mainresults:
        s = select([metadata.tables['MainRelation']]).where(metadata.tables['MainRelation'].c.referenz == ref[0])
        knn = ""
        try:
            result = conn.execute(s)
            mainrelationresults = result.fetchall()
            for uids in mainrelationresults:
                s = select([metadata.tables['WKN']]).where(
                    metadata.tables['WKN'].c.unternehmenId == uids[1])
                resultwkns = conn.execute(s).fetchall()
                for wkn in resultwkns:
                    if wkn[2] not in knn:
                        knn += wkn[2]+" "
                    if wkn[3] not in knn:
                        knn += wkn[3]+" "

            stmt = metadata.tables['Main'].update().values(Kennnummern=knn).where(
                metadata.tables['Main'].c.referenz == ref[0])
            conn.execute(stmt)
        except:
            continue
    conn.close()
    engine.dispose()
    return 0

if __name__ == "__main__":
    """
    Entrypoint: Searches for the files and parse them into the mainfunction (can be multiprocessed)
    """
    # The filespath are stored in the config.ini file.
    # And can be changed there.
    config = configparser.ConfigParser()
    config.sections()
    config.read('config.ini')
    akf_kenngetter(config)
    print("Finished!")
