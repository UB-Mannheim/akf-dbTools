###################### INFORMATIONS #############################
#           It gets the referenz values, reads all years which
#           are bind to it and pretty prints the data into the table.
# Program:  **akf-refGetter**
# Info:     **Python 3.6**
# Author:   **Jan Kamlah**
# Date:     **14.11.2017**

######### IMPORT ############
from sqlalchemy import create_engine, MetaData, select
import configparser
################ START ################

def get_pretty_dates(dates):
    """
    It takes alle the years
    and pretty printed it.
    E.g. 2001,2002,2003,2004 -> 2001-2004
    """
    pretty_date = str(dates[0])+"-"
    last = dates[0]
    for idx, date in enumerate(dates[1:]):
        if (date-1 == last):
            if idx == len(dates) - 2:
                pretty_date = pretty_date + str(date)
        else:
            if idx == len(dates) - 2:
                pretty_date = pretty_date+str(dates[idx])+", "+str(date)
            else:
                if int(pretty_date[-5:-1]) == dates[idx]:
                    pretty_date = pretty_date[:-1]+", " + str(date) + "-"
                else:
                    pretty_date = pretty_date + str(dates[idx]) + ", " + str(date) + "-"
        last = date
    return pretty_date

def akf_refgetter(config):
    """
    Main function of the AKF_RefGetter!
    """
    print("Start RefGetter")
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
    # And collect all years and prints them as year span to the "Jahresspanne" column in Main
    for ref in mainresults:
        s = select([metadata.tables['MainRelation']]).where(metadata.tables['MainRelation'].c.referenz == ref[0])
        dates = []
        try:
            result = conn.execute(s)
            mainrelationresults = result.fetchall()
            for date in mainrelationresults:
                dates.append(int(date[1][:4]))
            dates = sorted(list(set(dates)))
            if not dates: continue
            pretty_dates = dates[0]
            if len(dates)>2:
                pretty_dates = get_pretty_dates(dates)
            elif len(dates) == 2:
                pretty_dates = str(dates[0])+"-"+str(dates[1])
            print(pretty_dates)

            stmt = metadata.tables['Main'].update().values(Jahresspanne=pretty_dates).where(
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
    akf_refgetter(config)
    print("Finished!")
