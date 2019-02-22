###################### INFORMATION #############################
#           It talks to the SQLite-DB and inserts the data of JSON-Files
#           The main difference of json2sql books and cds is that books use an class to coordinate the parsing
# Program:  **AKF_SQL_DBTalk**
# Info:     **Python 3.6**
# Author:   **Jan Kamlah**
# Date:     **18.02.2019**

######### LIBRARIES ############
from sqlalchemy import create_engine, MetaData, select
import json
import configparser
import os
from functools import wraps
import time, timeit
import glob
import pandas as pd
import regex
import pathlib
from output_analysis import OutputAnalysis

######### CLASSES ############
class NoneRemover(json.JSONDecoder):
    """
    Removes all Null/None to an empty string
    it works but its not beautiful because
    it iterates twice over the values.
    I would love to change default values :) of the
    test library change for perfomance reasons...
    jsondecoder -> scanner.py -> Zeile 42
    """
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        for key, value in obj.items():
            if value == None:
                obj[key] = ""
        return obj

######### DECORATOR ############
def call_counter_and_time(func):
    """
    This decorator is for benchmarking the
    single functions in the program.
    It tracks the calls and the process time.
    """
    @wraps(func)
    def helper(*args, **kwds):
        helper.calls += 1
        startt = timeit.default_timer()
        result = func(*args, **kwds)
        print(timeit.default_timer()- startt)
        return result
    helper.calls = 0
    return helper

######### TABLE-FUNCTIONS ############
"""
The following functions get case-related called.
Every function represents a table in the sqlite db.
Sorry for breaking the convention with lowercase for 
functions, but they seem so classy.
"""

def Aktienkursetable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if 'Aktienkurse' not in new_data or len(new_data['Aktienkurse']) < 2: return 0
    addinfo_added = False
    overall_additional_info = ""
    maxlen = len(new_data['Aktienkurse'])-1

    if "additional_info" in new_data['Aktienkurse'][maxlen]["shares"].keys():
        addinfo_list = new_data['Aktienkurse'][maxlen]["shares"]["additional_info"]
        output_analysis.subtract_entry('Aktienkurse', addinfo_list)
        for item in addinfo_list:
            overall_additional_info += item + " "
        overall_additional_info = overall_additional_info.strip()

    for idx, share in new_data['Aktienkurse'][maxlen]["shares"]["Sharedata"].items():
        commentplus = ""

        if "Amount" in share:
            share["Amount"] = share["Amount"].replace(".",",").split(",")[0]

        addinfo_added = False
        if "additional_info" in share.keys():
            commentplus = new_data['Aktienkurse']["additional_info"]
            addinfo_added = True

        if overall_additional_info != "":
            overall_additional_info = "Gesamt: "+overall_additional_info

        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Jahr': share.get("Year", "").strip(),
             'Stichtag': share.get("ClosingDate", "").strip(),
             'Hoehe': share.get("Amount", ""),
             'Waehrung': share.get("Currency", "").strip(),
             'Einheit': share.get("Unit", ""),
             'Art': share.get("Kind", "").strip(),
             'Notiz':share.get("Notice", "").strip(),
             'Bemerkung': share.get("Comment", "").strip(),
             'BemerkungAbschnitt':commentplus.strip()+ overall_additional_info,
             'Abschnitt': "",
             'Rang': int(idx) + 1
             }])

        subtraction_elements = [share.get("Year",""),
                share.get("ClosingDate", ""),
                share.get("Amount", ""),
                share.get("Currency", ""),
                share.get("Unit", ""),
                share.get("Kind", ""),
                share.get("Notice", ""),
                share.get("Comment", "")]

        output_analysis.subtract_entry('Aktienkurse', subtraction_elements)
        if addinfo_added:
            output_analysis.subtract_entry('additional_info', commentplus)

    return 0


def Aktionaertable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if 'Großaktionär' in new_data:
        idx = 1
        for shareholder in new_data["Großaktionär"]:
            if "origpost" in shareholder:
                continue
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                     'Name': shareholder.get("shareholder",""),
                     'Ort': shareholder.get("location","").strip(".").strip(),
                     'Anteil': shareholder.get("share",""),
                     'Abschnitt': "",
                     'Bemerkung': shareholder.get("additional_information",""),
                     'BemerkungAbschnitt': "",
                     'Rang': idx + 1
                     }])
            idx += 1

            subtraction_elements = [
                    shareholder.get("shareholder", ""),
                    shareholder.get("location", ""),
                    shareholder.get("share", ""),
                    shareholder.get("additional_information", "")
                ]

            output_analysis.subtract_entry('Großaktionär', subtraction_elements)

    return 0


def Anleihentable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    # Deactivate Splitting must be reworked later!!!
    if 'Anleihen' not in new_data: return 0
    idx = 1
    for anleihen in new_data['Anleihen']:
        if "origpost" in anleihen:
            anleihen = [anleihen["origpost"]] #.split(".-")
            for anleihe in anleihen:
                if anleihe.replace(".", "").replace("-", "").strip() != "":
                    conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                    'Anleihen': anleihe,
                    'Rang': idx
                    }])

                    subtraction_elements = [
                        anleihe
                        ]
                    idx += 1
                    output_analysis.subtract_entry('Anleihen', subtraction_elements)

    if "Emissionsbetrag" in new_data:
        for anleihen in new_data["Emissionsbetrag"]:
            if "origpost" in anleihen:
                anleihen = anleihen["origpost"] #.split(".-")
                for anleihe in anleihen:
                    if anleihe.replace(".","").replace("-","").strip() != "":
                        conn.execute(table.insert(), [
                        {'unternehmenId': new_data['unternehmenId'],
                         'Anleihen': anleihe,
                         'Rang': idx
                         }])
                        idx+=1
                        output_analysis.subtract_entry('Emissionsbetrag', anleihe)

    return 0


def Aufsichtsrattable(conn, new_data, table, output_analysis):
    ckey = output_analysis.current_key
    print(table.columns.keys())
    if 'Aufsichtsrat' not in new_data and 'Arbeitnehmervertreter' not in new_data: return 0
    av = []
    if 'Arbeitnehmervertreter' in new_data:
        av = new_data["Arbeitnehmervertreter"]
    idx = 1
    comment = ""
    aufsictsrat_len = 0
    ar = []
    if 'Aufsichtsrat' in new_data:
        aufsictsrat_len = len(new_data['Aufsichtsrat'])
        ar = new_data['Aufsichtsrat']
    ctr_el = 0
    main_tag = 'Aufsichtsrat'
    for entry in ar+av:
        if ctr_el >= aufsictsrat_len:
            main_tag = 'Arbeitnehmervertreter'
        ctr_el += 1
        if "type" in entry.keys():
            if entry["type"] == "Arbeitnehmervertreter":
                comment = entry["type"]
            continue

        joined_rest = ""
        if "rest" in entry.keys():
            for text_r in entry['rest']:
                joined_rest += text_r + " "
                output_analysis.subtract_entry(main_tag, [text_r])

        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Name': entry.get("last_name","").strip(),
             'Vorname': entry.get("first_name","").strip(),
             'Titel': entry.get('title',"").strip(),
             'Ort': entry.get('city',"").strip(),
             'Funktion': entry.get('funct',comment).strip(),
             'Bemerkung': joined_rest,
             'Rang': 1
             }])
        idx+=1

        subtraction_elements = [entry.get("first_name", "").strip(),
                                entry.get("last_name", "").strip(),
                                entry.get('title', "").strip(),
                                entry.get('city', "").strip(),
                                entry.get('funct', comment)]

        output_analysis.subtract_entry(main_tag, subtraction_elements)

    return 0


def Beschaeftigtetable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    #TODO: Erst ab 1982 verfuegbar
    return 0


def Beteiligungentable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if 'Beteiligungen' not in new_data: return 0
    idx = 1
    comment = ""
    for entry in new_data['Beteiligungen']:
        subtraction_elements_headlines = []
        subtraction_elements_kapital = []
        subtraction_elements_dividenden = []

        if "type" in entry:
            continue
        location = ""

        shareholder = entry.get("text", "")
        if "," in shareholder:
            location = shareholder.rsplit(",", 1)[-1]
        if "kapital" in entry:
            share = entry.get("kapital", "").get("amount", "").strip()
            currency = entry.get("kapital", "").get("currency", "").strip()
            share_pc_bef = entry.get("kapital", "").get("add_info", "").strip()
            share_pc = share_pc_bef.replace("(", "").replace(")", "")

            subtraction_elements_kapital.append(share)
            subtraction_elements_kapital.append(currency)
            subtraction_elements_kapital.append(share_pc_bef)
            subtraction_elements_headlines.append("Kapital")
        else:
            share, currency, share_pc = "","",""
        if "dividenden" in entry.keys():
            if "percentages" in entry["dividenden"]:
                numbers = ",".join(entry["dividenden"]["percentages"])
                comment = f'Dividenden ({entry["dividenden"].get("year","")}: {numbers} %)'
                for num in entry["dividenden"]["percentages"]:
                    subtraction_elements_dividenden.append(num)
                subtraction_elements_dividenden.append(entry["dividenden"].get("year","").strip())
            elif "year" in entry["dividenden"]:
                comment = entry["dividenden"]["year"].strip()
                subtraction_elements_dividenden.append(comment)
                subtraction_elements_headlines.append("Kalenderjahr")

            subtraction_elements_headlines.append("Dividenden")

        if "Geschäftsjahr" in shareholder:
            continue
        if "Inland" in shareholder:
            shareholder = shareholder.replace("Inland","")
            shareholder = shareholder[shareholder.find(".",0,3)+1:]
            comment = "Inland" + comment
        elif "Ausland" in shareholder:
            shareholder = shareholder.replace("Ausland","")
            shareholder = shareholder[shareholder.find(".", 0, 3) + 1:]
            comment = "Ausland" + comment
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Firmenname': shareholder.strip(),
             'Ort': location.strip().strip("."),
             'Kapitalbetrag': share,
             'Waehrung': currency,
             'Anteil': share_pc,
             'Bemerkung': comment,
             'Rang': idx
             }])

        output_analysis.subtract_entry('Beteiligungen', [shareholder] + subtraction_elements_kapital + subtraction_elements_dividenden)
        output_analysis.subtract_entry('Beteiligungen', subtraction_elements_headlines)


        idx+=1
    return 0


def BilanzAktivatable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if 'AusDenBilanzen' not in new_data: return 0
    unit, currency = "",""
    idx = 1
    final_info_to_subtract = []
    ausdenbilanzen = new_data['AusDenBilanzen']
    maxlen= len(ausdenbilanzen)-1
    if "Aktiva" in ausdenbilanzen[maxlen]["balances"]:
        for col in ausdenbilanzen[maxlen]["balances"]["Aktiva"]:

            positions = ausdenbilanzen[maxlen]["balances"]["Aktiva"][col]
            year = positions["date"]
            if unit == "" and currency == "":
                currency, unit = get_currency_unit(positions["amount"])
            for (pos, amount) in positions.items():
                if pos in ["date", "amount"]:
                    continue
                match_parenthesis = regex.search(r"\(.+\)", pos)
                found_parenth = ""
                if match_parenthesis:
                    found_parenth = match_parenthesis.group().strip("() ")
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Konzernebene': "AG",
                     'Bilanzposition': pos,
                     'Jahr': year,
                     'Einheit': unit,
                     'Waehrung': currency,
                     'Betrag': amount,
                     'Bemerkung': found_parenth,
                     'BemerkungAbschnitt': "",
                     'Rang': idx
                     }])
                idx += 1
                final_info_to_subtract.extend([found_parenth, pos.strip(), year, positions["amount"].strip(), amount.strip()])

        output_analysis.subtract_entry('AusDenBilanzen', final_info_to_subtract)

    return 0


def BilanzPassivatable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if 'AusDenBilanzen' not in new_data: return 0
    unit, currency = "", ""
    maxlen = len(new_data['AusDenBilanzen'])-1
    idx = 1
    if "Passiva" in new_data['AusDenBilanzen'][maxlen]["balances"]:
        for col in new_data['AusDenBilanzen'][maxlen]["balances"]["Passiva"]:
            positions = new_data['AusDenBilanzen'][maxlen]["balances"]["Passiva"][col]
            year = positions["date"]
            if unit == "" and currency == "":
                currency, unit = get_currency_unit(positions["amount"])
            for pos, amount in positions.items():
                if "Bilanzsumme" in pos:
                    continue
                if pos in ["date","amount"]:
                    continue

                match_parenthesis = regex.search(r"\(.+\)", pos)
                found_parenth = ""
                if match_parenthesis:
                    found_parenth = match_parenthesis.group().strip("() ")

                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Konzernebene': "AG",
                     'Bilanzposition': pos,
                     'Jahr': year,
                     'Einheit': unit,
                     'Waehrung': currency,
                     'Betrag': amount,
                     'Bemerkung': found_parenth,
                     'BemerkungAbschnitt': "",
                     'Rang': idx
                     }])
                output_analysis.subtract_entry('AusDenBilanzen', [found_parenth, pos, year, unit, currency, amount])

                idx += 1

    return 0


def BilanzSummetable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if 'AusDenBilanzen' not in new_data: return 0
    unit, currency = "", ""
    maxlen = len(new_data['AusDenBilanzen'])-1
    idx = 1
    if "Passiva" in new_data['AusDenBilanzen'][maxlen]["balances"]:
        for col in new_data['AusDenBilanzen'][maxlen]["balances"]["Passiva"]:
            positions = new_data['AusDenBilanzen'][maxlen]["balances"]["Passiva"][col]
            year = positions["date"]
            del positions["date"]
            if unit == "" and currency == "":
                currency, unit = get_currency_unit(positions["amount"])
            del positions["amount"]
            for pos, amount in positions.items():
                if "Bilanzsumme" in pos:
                    conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Konzernebene': "AG",
                     'Bilanzposition': pos,
                     'Jahr': year,
                     'Einheit': unit,
                     'Waehrung': currency,
                     'Betrag': amount,
                     'Bemerkung': "",
                     'BemerkungAbschnitt': "",
                     'Rang': idx,
                     }])
                    output_analysis.subtract_entry('AusDenBilanzen', [pos, year, unit, currency, amount])
                    idx += 1

    return 0


def Boersennotiztable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if 'Börsennotiz' not in new_data: return 0
    idx = 1
    for entry in new_data['Börsennotiz']:
        additional_info = ""
        handelsplatz = ""

        if "additional_info" in entry.keys():
            additional_info = entry.get("additional_info", "").strip()
        if "location" in entry.keys():
            handelsplatz = entry.get("location", "").strip()
        if ":" in entry.get("location",""):
            entry["location"] = regex.sub(r"([^\s]+:)","",entry["location"])
        if additional_info != "" or handelsplatz != "":
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Handelsplatz': entry.get("location",""),
                 'Abschnitt': "",
                 'Bemerkung': "",
                 'Rang': idx
             }])

            output_analysis.subtract_entry('Börsennotiz', [additional_info, handelsplatz])

            idx+=1
    return 0


def Dependencetable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    for dependence in ["Niederlassung",
                       "Filialen",
                       "Niederlassungen",
                       "Zweigniederlassungen",
                       "Anlagen",
                       "Tochtergesellschaften",
                       "Werke/Betriebsstätten",
                       "Betriebsanlagen",
                       "KommanditeUndBank"]:



        if dependence in new_data.keys():
            for entry in new_data[dependence]:
                if "type" in entry.keys():
                    content = entry.get("origpost","").strip("., ")
                    if content != "":
                        conn.execute(table.insert(),[
                        {'unternehmenId': new_data['unternehmenId'],
                        'Dependence': content,
                        'Bezeichnung': dependence,
                        }])
                        output_analysis.subtract_entry(dependence, [entry.get("origpost","")])
                        break
    return 0


def Dividendentable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    idx = 1

    if 'Dividenden' in new_data:
        subtracted_information = []
        maxlen = len(new_data["Dividenden"]) - 1
        if maxlen == 0 and "origpost" in new_data["Dividenden"][0]:
            maxlen = 1
            new_data['Dividenden'].append({"dividende":{"Comment": new_data['Dividenden'][0]["origpost"]}})
        for entry in new_data['Dividenden'][maxlen]["dividende"].values():
            if isinstance(entry, str):
                entry = {"Comment":entry}
            if "origpost" in entry:
                continue
            if "insgesamt" in entry.get("comment",""):
                if "%" in entry.get("comment",""):
                    dividend = regex.search(r"(insgesamt:\s){e<2}(?P<dividend>[\d\s%]*)", entry.get("comment", ""))
                    if dividend:
                        entry["Dividend"] = dividend["dividend"]
                else:
                    dividend = regex.search(r"(insgesamt:\s){e<2}(?P<currency>[^\s\d]*)\s(?P<dividend>[^a-zA-Z]*)",entry.get("comment",""))
                    if dividend:
                        entry["Dividend"] = dividend["dividend"]
                        entry["Currency"] = dividend["currency"]
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Jahr': entry.get("Year",""),
                 'Aktienart': entry.get("Type",""),
                 'Nennwert': entry.get("Nomvalue",""),
                 'Dividende': entry.get("Dividend",""),
                 'Bonus': entry.get("Bonus",""),
                 'St_G': entry.get("St_G",""),
                 'Waehrung': entry.get("Currency",""),
                 'Div_Sch_Nr': entry.get("Div_Sch_Nr",""),
                 'Bemerkung': entry.get("comment",""),
                 'BemerkungAbschnitt': entry.get("Comment",""),
                 'Abschnitt': "",
                 'Rang': idx,
                 }])

            # comments can get quite long, so they are splitted
            comment1 = entry.get("comment", "")
            comment2 = entry.get("Comment", "")
            comment_split = regex.split(",|;|:|\s ", comment1)
            comment_split_2 = regex.split(",|;|:|\s", comment2)
            subtracted_information.extend([
                entry.get("Year", ""),
                entry.get("Type", ""),
                entry.get("Nomvalue", ""),
                entry.get("Dividend", ""),
                entry.get("Bonus", ""),
                entry.get("St_G", ""),
                entry.get("Currency", ""),
                entry.get("Div_Sch_Nr", ""),

            ])
            subtracted_information.extend(comment_split)
            subtracted_information.extend(comment_split_2)

            idx += 1
        output_analysis.subtract_entry('Dividenden', subtracted_information)

    if 'DividendenAufXYaktien' in new_data:
        maxlen = len(new_data["DividendenAufXYaktien"]) - 1
        if maxlen == 0 and "origpost" in new_data["DividendenAufXYaktien"][0]:
            maxlen = 1
            new_data['DividendenAufXYaktien'].append({"dividende":{"Comment":new_data['DividendenAufXYaktien'][0]["origpost"]}})
        subtracted_information = []
        for entry in new_data['DividendenAufXYaktien'][maxlen]["dividende"].values():
            my_div = new_data['DividendenAufXYaktien']

            if isinstance(entry,str):
                entry = {"Comment":entry}
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Jahr': entry.get("Year",""),
                 'Aktienart': "",
                 'Nennwert': "",
                 'Dividende': entry.get("Dividend",""),
                 'Bonus': entry.get("Bonus",""),
                 'St_G': entry.get("St_G",""),
                 'Waehrung': entry.get("Currency",""),
                 'Div_Sch_Nr': entry.get("Div_Sch_Nr",""),
                 'Bemerkung': entry.get("comment",""),
                 'BemerkungAbschnitt': entry.get("Comment",""),
                 'Abschnitt': "",
                 'Rang': idx,
                 }])

            subtracted_information.extend([
                entry.get("Year", ""),
                entry.get("Dividend", ""),
                entry.get("Bonus", ""),
                entry.get("St_G", ""),
                entry.get("Currency", ""),
                entry.get("Div_Sch_Nr", ""),
            ])

            comment1 = entry.get("comment", "")
            comment2 = entry.get("Comment", "")
            comment_split = regex.split(",|;|:|\s ", comment1)
            comment_split_2 = regex.split(",|;|:|\s", comment2)
            subtracted_information.extend(comment_split)
            subtracted_information.extend(comment_split_2)
            idx += 1

        output_analysis.subtract_entry('DividendenAufXYaktien', subtracted_information)

    return 0


def Geschaeftsjahrtable(conn, new_data, table, output_analysis):
    if 'Geschäftsjahr' not in new_data : return 0
    KJ, GJ, GJA, GJE = 1, "", "", ""
    entries_to_subtract = []
    KJ_Rest = ""

    for entry in new_data['Geschäftsjahr']:
        if "origpost" in entry.keys():
            continue

        if "gesch_jahr_start" in entry.keys():
            GJA = entry["gesch_jahr_start"].strip()

        if "gesch_jahr_stop" in entry.keys():
            GJE = entry["gesch_jahr_stop"].strip()
        if "year" in entry.keys():
            KJ = "".join(entry['year'])
            KJ_Rest += KJ.replace("Kalenderjahr", "").strip()+" "
            if "Periode" in KJ:
                GJA = KJ
            elif "-" in KJ:
                GJA = KJ.split("-")[0].strip()
                GJE = KJ.split("-")[1].strip()
            else:
                GJA = KJ
            if "Kalenderjahr" in KJ:
                KJ = 1
            else:
                KJ = 0
            for year_chunk in entry['year']:
                entries_to_subtract.append(year_chunk)

    if not "".join([GJ, GJA, GJE]) == "" or KJ != 0:
        if KJ == 1:
            GJ = "Kalenderjahr"
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Kalenderjahr': KJ,
             'Geschaeftsjahresanfang': GJA.strip().replace("Kalenderjahr",""),
             'Geschaeftsjahresende': GJE.strip(),
             'Bemerkung': GJ,
             'Abschnitt': "",
             }])

        entries_to_subtract.extend([
            GJA,
            GJE,
            GJ
        ])

        output_analysis.subtract_entry('Geschäftsjahr', entries_to_subtract)

    return 0


def Geschaeftsleitungtable(conn, new_data, table, output_analysis):
    #TODO: Überprüfen ob Gescheaftsleitung exisitert?
    print(table.columns.keys())
    if 'Geschäftsleitung' not in new_data: return 0
    idx = 1
    for entry in new_data['Geschäftsleitung']:
        if "type" in entry.keys():
            continue
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Name': entry.get('last_name', ""),
             'Vorname': entry.get('first_name', ""),
             'Titel': entry.get('title', ""),
             'Ort': entry.get("city", "").strip().strip("."),
             'Funktion': entry.get("func", ""),
             'Bemerkung': "",
             'Rang': idx
             }])
        idx += 1

        subtracted_information = [
            entry.get('last_name', ""),
            entry.get('first_name', ""),
            entry.get('title', ""),
            entry.get("city", ""),
            entry.get("funct", "")
        ]

        output_analysis.subtract_entry('Geschäftsleitung', subtracted_information)

    return 0


def Grundkapitaltable(conn, new_data, table, output_analysis):
    print(table.columns.keys())

    if 'Grundkapital' in new_data:
        idx = 1
        for entry in new_data["Grundkapital"]:
            to_subtract_additions = []
            if 'Grundkapital' not in entry.keys():
                continue
            my_grundkapital = entry['Grundkapital']
            add_info = ""
            if 'additional_info' in entry.keys():
                for addinfo_element in entry['additional_info']:
                    adinfo = addinfo_element.replace("↑", ":")
                    add_info += adinfo + " "
                    to_subtract_additions.append(adinfo.strip())

            # comment = "".join(entry.get("additional_info",[""])) # removed -> moved to kapitalentwicklung
            #output_analysis.subtract_entry("Grundkapital", [my_grundkapital.get('amount', ""), my_grundkapital.get('currency', "")])
            if isinstance(my_grundkapital,str):
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Hoehe': "",
                     'Waehrung': "",
                     'Bemerkung': my_grundkapital + " ," + add_info,  # removed 'comment' here
                     'Rang': idx,
                     }])
                idx+=1
                output_analysis.subtract_entry('Grundkapital', my_grundkapital)

            else:
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Hoehe': my_grundkapital.get('amount', "").replace(".","").replace("-","").strip(),
                     'Waehrung': my_grundkapital.get('currency', ""),
                     'Bemerkung': add_info,  # removed 'comment' here
                     'Rang': idx,
                     }])

                idx += 1

                output_analysis.subtract_entry('Grundkapital',
                                               [
                                                my_grundkapital.get('amount', ""),
                                                my_grundkapital.get('currency', "")
                                               ] + to_subtract_additions)

    elif 'Bezugsrechte' in new_data.keys():
        for entry in new_data['Bezugsrechte']:
            if 'origpost' in entry.keys():
                continue
            if 'bezugsrechte' not in entry.keys():
                continue
            my_entry = entry['bezugsrechte']

            my_key = None
            for key in my_entry.keys():
                if 'Grundkapital' in key:
                    my_key = key
            if my_key is not None:
                gk_value = my_entry[my_key]
                idx = 1

                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Hoehe': gk_value.get('amount', "").replace(".","").replace("-","").strip(),
                     'Waehrung': gk_value.get('currency', ""),
                     'Bemerkung': "",  # removed 'comment' here
                     'Rang': idx,
                     }])

                idx += 1

                output_analysis.subtract_entry('Grundkapital',
                                               [
                                                gk_value.get('amount', ""),
                                                gk_value.get('currency', "")
                                               ])

    return 0


def GuVtable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if 'AusGewinnVerlustrechnungen' not in new_data: return 0
    unit, currency = "",""
    maxlen = len(new_data['AusGewinnVerlustrechnungen'])-1
    idx = 1
    for kind in new_data['AusGewinnVerlustrechnungen'][maxlen]["income"]:
        if kind == "additional_info": continue
        for positions in new_data['AusGewinnVerlustrechnungen'][maxlen]["income"][kind].values():
            year = positions["date"]
            del positions["date"]
            if unit == "" and currency == "":
                currency, unit = get_currency_unit(positions["amount"])
            del positions["amount"]
            for pos, amount in positions.items():
                if "=" in pos:
                    continue
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Konzernebene': "AG",
                     'GuVPosition': pos,
                     'Jahr': year,
                     'Einheit': unit,
                     'Waehrung': currency,
                     'Betrag': amount,
                     'Bemerkung': "",
                     'BemerkungAbschnitt': "",
                     'Rang': idx
                     }])

                output_analysis.subtract_entry('AusGewinnVerlustrechnungen',
                                               [
                                                   pos,
                                                   year,
                                                   unit,
                                                   currency,
                                                   amount
                                               ])

                idx += 1
    return 0


def Kapitalarttable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    idx = 0
    if 'Grundkapital' in new_data:
        myset = new_data['Grundkapital']
        for entry in myset:
            key_found = None
            if "Autorisiertes Kapital" in entry.keys():
               key_found = "Autorisiertes Kapital"
            if "Ausgegebenes Kapital" in entry.keys():
               key_found = "Ausgegebenes Kapital"
            if key_found is not None:
                if isinstance(entry[key_found],list):
                    entry[key_found]= "".join(entry[key_found])
                match_dm = regex.search(r"^(?P<currency>\D{1,4})(?P<amount>[\d\.\-\s]*)", entry[key_found])
                if match_dm:
                   currency = match_dm.group('currency')
                   amount = match_dm.group('amount')
                   conn.execute(table.insert(), [
                       {'unternehmenId': new_data['unternehmenId'],
                        'Kapitalart': key_found,
                        'Hoehe': amount.replace(' ', "").replace(" ", ""),
                        'Waehrung': currency,
                        'Bemerkung': "",
                        'Rang': idx + 1
                        }])
                   idx += 1
                   output_analysis.subtract_entry("Grundkapital",
                                                  [key_found,
                                                   amount,currency])



    return 0


def Kapitalentwicklungtable(conn, new_data, table, output_analysis):
    #TODO: Rest nach Grundkapital wohl hier....
    print(table.columns.keys())
    idx = 1
    if 'Grundkapital' in new_data:
        regex_year = regex.compile(r"\d\d\d\d")
        for entry in new_data["Grundkapital"]: # todo 11.01 additional years here ?
            if "additional_info" not in entry.keys():
                continue
            value_ai = "".join(entry['additional_info']).strip()
            year = ""
            key_strip = "".join(entry['additional_info']).strip()
            match_year = regex_year.search(key_strip)
            if match_year:
                year = match_year.group()  # only match the first year found
            if value_ai == "":
                continue

            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Jahr': year,
                 'Text': value_ai,  # rplaced comment
                 'Bemerkung':"",
                 'Rang': idx,
                 }])

            output_analysis.subtract_entry("Grundkapital",
                                           [year,
                                            value_ai])

            idx += 1

        for entry in new_data["Grundkapital"]:
            if len(entry.keys()) >= 1:
                # check if it's a 'year' entry
                first_key = list(entry.keys())[0]
                first_entry = entry[first_key]

                chk = isinstance(first_entry, dict)
                if chk and "year" in first_entry.keys():
                    if isinstance(first_entry["text"], list):
                        first_entry["text"] = "".join(first_entry["text"])
                    conn.execute(table.insert(), [
                        {'unternehmenId': new_data['unternehmenId'],
                         'Jahr': first_entry['year'],
                         'Text': first_entry['text'],  # placed comment
                         'Bemerkung': "",
                         'Rang': idx,
                         }])
                    idx += 1

                    output_analysis.subtract_entry("Grundkapital",
                                                   [first_entry['year'],
                                                    first_entry['text']])

    if "Bezugsrechte" in new_data:
        regex_year = regex.compile(r"\d\d\d\d")
        if len(new_data['Bezugsrechte']) >= 2 and 'bezugsrechte' in new_data["Bezugsrechte"][1].keys():
            my_set = new_data["Bezugsrechte"][1]['bezugsrechte']
            for key in my_set:
                entry = my_set[key]
                key_strip = key.strip()
                year = ""
                if 'Grundkapital' in key:
                    continue
                if entry == "":
                    continue

                match_year = regex_year.search(key_strip)
                if match_year:
                    year = match_year.group()  # only match the first year found

                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Jahr': year,
                     'Text': entry,  # rplaced comment
                     'Bemerkung':"",
                     'Rang': idx,
                     }])
                output_analysis.subtract_entry("Bezugsrechte",
                                               [year,
                                                entry])

                idx += 1

        if len(new_data['Bezugsrechte']) >= 2 and 'bezugsrechtsabschlaege' in new_data["Bezugsrechte"][1].keys():

            my_set = new_data["Bezugsrechte"][1]['bezugsrechtsabschlaege']
            for key in my_set:
                entry = my_set[key]

                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Jahr': key,
                     'Text': entry,  # rplaced comment
                     'Bemerkung':"",
                     'Rang': idx,
                     }])
                output_analysis.subtract_entry("Bezugsrechte",
                                               [key,
                                                entry])

        if len(new_data['Bezugsrechte']) >= 2 and 'berichtigungsaktien' in new_data["Bezugsrechte"][1].keys():
            my_set = new_data["Bezugsrechte"][1]['berichtigungsaktien']
            for key in my_set:
                entry = my_set[key]

                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Jahr': key,
                     'Text': entry,  # rplaced comment
                     'Bemerkung':"",
                     'Rang': idx,
                     }])
                output_analysis.subtract_entry("Bezugsrechte",
                                               [key,
                                                entry])

    return 0


def Kennzahlentable(conn, new_data, table, output_analysis):
    """
    In this case we have different conditions.
    Is not a pretty solution.
    """
    #TODO: Gibt es in den Jahren nicht...
    return 0
    print(table.columns.keys())
    if 'boersenbewertung' not in new_data: return 0
    for boerse in new_data['boersenbewertung']:
        addid = []
        addid.append(0)
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Kennzahlenposition': pprtname+entryinfo,
             'Jahr': year,
             'Einheit': unit,
             'W\xe4hrung': currency,
             'Betrag': block[entry][name].replace(' ', "").replace(" ", ""),
             'Bemerkung': comment,
             'Rang': idxxx + 1
             }])

        output_analysis.subtract_entry("boersenbewertung",
                                       [year,
                                        unit,
                                        currency,
                                        block[entry][name],
                                        comment])

    return 0


def Maintable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if new_data['reference'] == new_data['unternehmenId']:
        new_data['id'] = get_lastid(table, conn)
        conn.execute(table.insert(), [
            {'name': new_data['additionals']["LABEL"],
             'referenz': new_data['reference'],
             'Erstnotierung': "",
             'Letztnotierung': "",
             'imAktienfuehrer': "",
             'Bemerkung': "",
             'Indexzugeh\xf6rigkeit': "",
             'id': new_data['id'],
             }])
    return 0


def MainRelationtable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if "additionals" in new_data:
        release = new_data["additionals"].get("Release","")
        firmname = new_data["additionals"].get("LABEL","")
    else:
        release = ""
        firmname = new_data["firmname"]
    conn.execute(table.insert(), [
        {'referenz': new_data["reference"],
         'weiteresAuftreten': new_data["unternehmenId"],
         'Unternehmen': firmname,
         'Erscheinungsjahr': release,
         'id': new_data['id'],
         }])
    return 0


def Organbezuegetable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    #TODO:Gibt es in den Jahren nicht
    return 0


def Stimmrechttable(conn, new_data, table, output_analysis):
    """
    Stimmrecht entry could be in the first layer or
    in the "ausgebenes Kapital" entryblock.
    """
    print(table.columns.keys())
    if "StimmrechtAktien" not in new_data: return 0
    """regex_stimmrecht = regex.compile(r"(?P<number>Je\s?nom)[^a-zA-Z]*"
                                     r"(?P<currency>[^0132465789]*)"
                                     r"(?P<amount>[^.=]*)" 
                                     r"[^\d]*(?P<voice>\d*\s?Stimme)")"""
    general_addinfo = ""
    del_idx = []
    for idx, entry in enumerate(new_data["StimmrechtAktien"]):
        if "origpost" in entry:
            del_idx.append(idx)
            continue
        if len(entry) == 1 and "additional_info" in entry:
            general_addinfo = entry["additional_info"]
            del_idx.append(idx)
    for idx in reversed(del_idx):
        del new_data["StimmrechtAktien"][idx]
    if len(new_data["StimmrechtAktien"]) == 0:
        new_data["StimmrechtAktien"] = [{"entry":{"additional_info":general_addinfo}}]
    idx = 1
    add_info_subtract = []
    for idxx, entry in enumerate(new_data["StimmrechtAktien"]):
        if idxx == len(new_data["StimmrechtAktien"])-1:
            entry["additional_info"] = entry.get("additional_info", "") + general_addinfo
            add_info_subtract.append(entry.get("additional_info", "").strip())
        if "additional_info" != list(entry.keys())[0]:
            entry = entry[list(entry.keys())[0]]
        else:
            entry = entry[list(entry.keys())[1]]
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
                 'Aktienart': entry.get("kind",""),
                 'Stueck': entry.get("amount",""),
                 'Stimmzahl': entry.get("vote",""),
                 'Nennwert': entry.get("value",""),
                 'Waehrung': entry.get("currency",""),
                 'Bemerkung': entry.get("additional_info",""),
                 'Rang': entry.get("rank", 0)
                 }])

        output_analysis.subtract_entry("StimmrechtAktien",
                                       [entry.get("kind","").strip(),
                                        entry.get("amount", "").strip(),
                                        entry.get("vote", "").strip(),
                                        entry.get("value", "").strip(),
                                        entry.get("currency", "").strip(),
                                        entry.get("additional_info", "").strip(),
                                        entry.get("rank", "")
                                        ])

        idx += 1

    output_analysis.subtract_entry("StimmrechtAktien", add_info_subtract)
    return 0


def Stueckelungtable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if "Stückelung" not in new_data: return 0
    offset = 0
    general_addinfo = ""
    del_idx = []

    # fetch general info
    for idx, entry in enumerate(new_data["Stückelung"]):
        if "origpost" in entry.keys():
            del_idx.append(idx)
            continue
        if len(entry) == 1 and "additional_info" in entry:
            general_addinfo = entry["additional_info"]
            del_idx.append(idx)


    #for idx in reversed(del_idx):
    #    del new_data["Stückelung"][idx]
    #if len(new_data["Stückelung"]) == 0:
    #    new_data["Stückelung"] = [{"entry": {"additional_info": general_addinfo}}]
    subtraction_entries = []
    general_additional_info = []

    for idx, entry in enumerate(new_data["Stückelung"]): # todo check stückelung with changes from 30.01 in hocr parser
        if "origpost" in entry.keys():
            continue
        #    offset += 1
        #    continue
        #if idx == len(new_data["Stückelung"]) - 1:
        #    entry["additional_info"] = entry.get("additional_info", "") + general_addinfo
        if 'additional_info' in entry.keys():
            general_additional_info.append(entry['additional_info'].strip())
            output_analysis.subtract_entry("Stückelung",
                                           [general_additional_info])
        for subkey in entry:
            if subkey == "additional_info":
                continue
            subentry = entry[subkey]
            #sub_additional_info = ""
            #if 'additional_info' in subentry.keys():
            #    sub_additional_info = entry['additional_info']
            addendum = ""
            if general_additional_info != "":
                if "".join(general_additional_info) != "":
                    addendum = "Zusatzinfo: " + "".join(general_additional_info)
            conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Aktienart': subentry.get("kind", ""),
                     'Anzahl': subentry.get("amount", ""),
                     'Nominalwert': subentry.get("value", ""),
                     'Waehrung': subentry.get("currency", ""),
                     'Bemerkung': subentry.get("additional_info", "") + addendum,
                     'Rang': subentry.get("rank", 0)
                     }])

            subtraction_entries = subtraction_entries + ([subentry.get("kind", ""),
                                    subentry.get("amount", ""),
                                    subentry.get("value", ""),
                                    subentry.get("currency", ""),
                                    subentry.get("additional_info", ""),
                                    subentry.get("rank", 0)
                                    ])


    output_analysis.subtract_entry("Stückelung", subtraction_entries)

    return 0


def Unternehmentable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    information_to_subtract_gruendung = []
    information_to_subtract_other = {}

    # if there is info to zur Geschäftslage fetch it and write it in Bemerkungen
    zur_geschaeftslage = ""
    if 'ZurGeschäftslage' in new_data.keys():
        zur_geschaeftslage  = ""
        for zg in new_data['ZurGeschäftslage']:
            if "origpost" in zg:
                zur_geschaeftslage += zg['origpost']
        information_to_subtract_other['ZurGeschäftslage'] = [zur_geschaeftslage]
    firmname = ""
    founding = ""
    release = ""
    rest_info ="" # rest of Gründung

    if "additionals" in new_data:
        entry = new_data["additionals"]
        if "LABEL" in entry:
            firmname = entry["LABEL"]
        if "Year" in entry.keys():
            release = entry["Year"]
    if "Gründung" in new_data.keys():
        for entry in new_data["Gründung"]:
            if "year" in entry.keys():
                founding = entry.get("year", "")
                information_to_subtract_gruendung.append(founding)
            if "rest_info" in entry.keys():
                rest_info += entry.get("rest_info", "") + ""
                information_to_subtract_gruendung.append(rest_info)

    base_data = ""
    for bdata in ["Sitz",
                    "Verwaltung",
                    "Telefon/Fernruf",
                    "Fernschreiber"]:
        current_keytype = bdata  # ['type']
        information_to_subtract_other[current_keytype] = [] # create storage for corresponding key
        if bdata in new_data.keys():
            for entry in new_data[bdata]:
                if "type" in entry.keys():
                    base_data += " "+ bdata +":"+entry.get("origpost", "")
                    information_to_subtract_other[current_keytype].append(entry.get("origpost", ""))
                    break
    activity_description = ""
    # Combine all activities and products
    for actarea in ["Tätigkeitsgebiet",
                       "Erzeugnisse",
                       "Haupterzeugnisse",
                       "Spezialitäten"]:
        current_keytype = actarea
        information_to_subtract_other[current_keytype] = []
        if actarea in new_data.keys():
            for entry in new_data[actarea]:
                if "type" in entry.keys():
                    information_to_subtract_other[current_keytype].append(entry.get("origpost", ""))
                    if actarea == "Tätigkeitsgebiet":
                        activity_description += entry.get("origpost","")
                    else:
                        activity_description += " " + actarea + ":" + entry.get("origpost", "")
                    break
    conn.execute(table.insert(), [
        {'unternehmenId': new_data['unternehmenId'],
         'Unternehmen': firmname,
         'Stammdaten': base_data.strip(),
         'Taetigkeitsgebiet': activity_description.strip(),
         'Gruendungsjahr': founding,
         'AGseit': "",
         'InvestorRelations': "",
         'PublicRelations': "",
         'Hauptversammlung': "",
         'WP': "",
         'Erscheinungsjahr': release,
         'Startseite': "",
         'Bemerkung': rest_info + " " + "ZurGeschäftslage: " + zur_geschaeftslage,
         }])

    output_analysis.subtract_entry("Gründung", information_to_subtract_gruendung)
    for key in information_to_subtract_other:
        info = information_to_subtract_other[key]
        if len(info) >= 1:
            output_analysis.subtract_entry(key, info)

    return 0


def Volumetable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    return 0
    #TODO: Nur einmal pro Jahrgang (auch manuell nachtragbar)
    if "additionals" not in new_data: return 0
    if not table["volume"]: return 0
    table["volume"] = False
    idgoobi = new_data["additionals"].get("ProfileId","")
    offset = ""
    idOAI = new_data["additionals"].get("OAI","")
    release = new_data["additionals"].get("release","")
    volume = new_data["unternehmenId"].split(".")[0]
    conn.execute(table.insert(), [
        {'Erscheinungsjahr': release,
         'idGoobi': idgoobi,
         'offsetSeitenzahlen': offset,
         'idOAI': idOAI,
         'Volume': volume,
         }])

    # todo js not used atm, eventually add up



    return 0


def Vorstandtable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if 'Vorstand' not in new_data: return 0
    idx = 1
    for entry in new_data['Vorstand']:


        if "type" in entry.keys():
            continue

        joined_rest = ""
        if "rest" in entry.keys():
            for text_r in entry['rest']:
                joined_rest += text_r + " "
                output_analysis.subtract_entry("Vorstand", [text_r])

        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Name': entry.get('last_name', "").strip(),
             'Vorname': entry.get('first_name', "").strip(),
             'Titel': entry.get('title', "").strip(),
             'Ort':  entry.get("city", "").strip(),
             'Funktion': entry.get("funct", "").strip(),
             'Bemerkung': joined_rest,
             'Rang': idx
             }])

        output_analysis.subtract_entry("Vorstand",
                                       [entry.get('last_name',"").strip(),
                                        entry.get('first_name', "").strip(),
                                        entry.get('title', "").strip(),
                                        entry.get("city", "").strip(),
                                        entry.get("funct", "").strip()
                                        ])

        idx += 1
    return 0


def WKNtable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if "additionals" in new_data:
        idx = 1
        for wkn in new_data["additionals"]["WKN"]:
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Unternehmen': new_data['additionals']["LABEL"],
                 'WKN': wkn[0],
                 'ISIN': "",
                 'Bemerkung': "",
                 'Aktienart': wkn[1],
                 'Rang': idx + 1
                 }])
            idx+=1
    return 0


def WeitereBemerkungentable(conn, new_data, table, output_analysis):
    print(table.columns.keys())
    if "Direktionskomitee" in new_data:
        section = "Direktionskomitee"
        for entry in new_data[section]:
            if "origpost" in entry.keys():
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Abschnitt': section,
                     'Bemerkung': entry["origpost"],
                     }])

                output_analysis.subtract_entry("Direktionskomitee",
                                               [section,
                                                entry["origpost"]
                                                ])

    if "Generaldirektion" in new_data:
        section = "Generaldirektion"
        for entry in new_data[section]:
            if "origpost" in entry.keys():
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                    'Abschnitt': section,
                    'Bemerkung': entry["origpost"],
                    }])

                output_analysis.subtract_entry("Generaldirektion",
                                               [section,
                                                entry["origpost"]
                                                ])

    if "Vizegeneraldirektion" in new_data:
        section = "Vizegeneraldirektion"
        for entry in new_data[section]:
            if "origpost" in entry.keys():
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                    'Abschnitt': section,
                    'Bemerkung': entry["origpost"],
                    }])

                output_analysis.subtract_entry("Vizegeneraldirektion",
                                               [section,
                                                entry["origpost"]
                                                ])
    if "Sekretäre" in new_data:
        section = "Sekretäre"
        for entry in new_data[section]:
            if "origpost" in entry.keys():
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                    'Abschnitt': section,
                    'Bemerkung': entry["origpost"],
                    }])

                output_analysis.subtract_entry("Sekretäre",
                                               [section,
                                                entry["origpost"]
                                                ])
    if "BeratendeMitglieder" in new_data:
        section = "BeratendeMitglieder"
        for entry in new_data[section]:
            if "origpost" in entry.keys():
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                    'Abschnitt': section,
                    'Bemerkung': entry["origpost"],
                    }])

                output_analysis.subtract_entry("BeratendeMitglieder",
                                               [section,
                                                entry["origpost"]
                                                ])
    if "Gesellschafter" in new_data:
        section = "Gesellschafter"
        for entry in new_data[section]:
            if "origpost" in entry.keys():
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                    'Abschnitt': section,
                    'Bemerkung': entry["origpost"],
                    }])

                output_analysis.subtract_entry("Gesellschafter",
                                               [section,
                                                entry["origpost"]
                                                ])
    if "Zahlstellen" in new_data:
        section = "Zahlstellen"
        for entry in new_data[section]:
            if "origpost" in entry.keys():
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                    'Abschnitt': section,
                    'Bemerkung': entry["origpost"],
                    }])

                output_analysis.subtract_entry("Zahlstellen",
                                               [section,
                                                entry["origpost"]
                                                ])
    if "Auslandsvertretung" in new_data:
        section = "Auslandsvertretung"
        for entry in new_data[section]:
            if "origpost" in entry.keys():
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Abschnitt': section,
                     'Bemerkung': entry["origpost"],
                     }])

                output_analysis.subtract_entry("Auslandsvertretung",
                                               [section,
                                                entry["origpost"]
                                                ])

    if "KursVonZuteilungsrechten" in new_data:
        section = "KursVonZuteilungsrechten"
        for entry in new_data[section]:
            if "origpost" in entry.keys():
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Abschnitt': section,
                     'Bemerkung': entry["origpost"],
                     }])

                output_analysis.subtract_entry("KursVonZuteilungsrechten",
                                               [entry["origpost"]])

    if "RechteVorzugsaktien" in new_data:
        section = "RechteVorzugsaktien"
        for entry in new_data[section]:
            if "origpost" in entry.keys():
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Abschnitt': section,
                     'Bemerkung': entry["origpost"],
                     }])

                output_analysis.subtract_entry("RechteVorzugsaktien",
                                               [entry["origpost"]])

    return 0


######### FUNCTIONS ############
def clean_currency(currency):
    """
    Cleans the currency strings from irrelevant informations
    :param currency:
    :return:
    """
    if "TDM" in currency:
        currency = "TDM"
    elif "Mio" in currency and "DM" in currency:
        currency = "Mio DM"
    elif "Mrd" in currency and "DM" in currency:
        currency = "Mrd DM"
    elif "DM" in currency:
        currency = "DM"
    elif "Thlf" in currency:
        currency = "Thlf"
    elif "hlf" in currency:
        currency = "hlf"
    elif "mögen" in currency:
        currency = "DM"
    return currency


def get_currency_unit(value):
    """
    Extracts the currency and the unit of the entrystring.
    It replaces:
    "Mio" with unit: 1_000_000
    "Mrd" with currency: 1_000_000_000
    TEUR with unit: 1_000 and currency: EUR
    TDM with unit: 1_000 and currency: DM
    """
    currency = ""
    unit = "1"
    if "TEUR" in value:
        currency = "EUR"
        unit = "1000"
    elif "TDM" in value:
        currency = "DM"
        unit = "1000"
    elif "Mio" in value:
            unit = "1000000"
            currency = value[value.find("Mio")+3:]
    elif "Mrd" in value:
            unit = "1000000000"
            currency = value[value.find("Mrd")+3:]
    else:
        numbers = []
        lcidx = 0
        for cidx, char in enumerate(value):
            if char.isdigit():
                numbers.append(char)
                lcidx = cidx
        unit = "".join(numbers)
        currency = value[lcidx + 1:].strip()
    currency = clean_currency(currency)
    return currency, unit


def create_dir(newdir):
    """
    Creates a new directory
    """
    if not os.path.isdir(newdir):
        try:
            os.mkdir(newdir)
            print(newdir)
        except IOError:
            print("cannot create %s directoy" % newdir)


def get_lastid(table, conn):
    """
    Get the last id available to generate a new one.
    New_ID = Old_ID + 1
    """
    s = select([table.c.id])
    result = conn.execute(s)
    allids = result.fetchall()
    idlist = []
    for idx in allids:
        if isinstance(idx.values()[0], int):
            idlist.append(idx.values()[0])
    if len(idlist) >=1:
        lastid = max(idlist)
    else:
        lastid = 1
    return lastid + 1


def get_files(filedir):
    """
    Get all file names!
    This function can be modified for specific concerns or
    the config could be extend.
    """
    #if os.path.isdir(filedir):
    #    os.chdir(filedir)
    #inputfiles = []
    inputfiles = sorted(glob.glob(filedir+"*.txt"))
    # files_to_check.extend(glob.glob('*.json')) # also add json endings # todo check and uncomment if necessary
    #for inputfile in sorted(files_to_check):
    #    inputfiles.append(os.getcwd() + os.path.normcase("/") + inputfile)
    #else:
    #    inputfiles = [filedir]
    return inputfiles


def get_refid(additionals,wkn,refs):
    """
    Find reference ids to the given wkn or isin
    :param additionals:
    :param wkn:
    :param refs:
    :return:
    """
    ids = additionals["WKN"]
    for id in ids:
        id = id[0]
        print(id)
        findings = wkn.loc[wkn["WKN"] == id]
        if not findings.empty:
            break
    else:
        if isinstance(additionals["Connected_WKN"], list):
            for id in additionals["Connected_WKN"]:
                findings = wkn.loc[wkn["WKN"] == id]
                if not findings.empty:
                    break
        else:
            return None
    for finding in findings["unternehmenId"]:
        ref = refs.loc[refs['weiteresAuftreten'] == finding]
        if not ref.empty:
            return ref.referenz.iloc[0]
    return None


def match_grundkapital_bezugsrechte(new_data):
    """
    Exchange informatin between the segements grundkapital and bezugsrechte
    :param new_data:
    :return:
    """
    if "Grundkapital:" in new_data:
        new_data["Grundkapital"] = new_data["Grundkapital:"]
    if "Grundkapital" in new_data:
        for entry in new_data["Grundkapital"]:
            if 'Grundkapital' not in entry.keys():
                continue
            my_grundkapital = entry['Grundkapital']
            if isinstance(my_grundkapital,list):
                entry['Grundkapital'] = "".join(my_grundkapital)
    if "Bezugsrechte" in new_data and "Grundkapital" not in new_data:
        for entry in new_data["Bezugsrechte"]:
            if "bezugsrechte" in entry:
                for key,val in entry["bezugsrechte"].items():
                    if "Grundkapital" in key:
                        new_data["Grundkapital"] = [{"Grundkapital":entry["bezugsrechte"][key]}]
                        del entry
                        break
        pass
    return


def akf_db_updater(file,dbPath, df, output_analysis):
    """
    Main function of the AKF_SQL_DBTalk!
    It updates the database for each file.
    Each table will be called independently.
    """
    #file = file.replace("\\", "/")
    print("Start SQLTalk")
    print(file)
    with open(file, 'r', encoding="utf-8") as f:
        new_data = json.load(f, cls=NoneRemover)

    output_analysis.change_file(new_data, file)

    # Generate unternehmenId
    new_data.update({'unternehmenId': str(file).rsplit("/")[-1].split("_")[1].replace("-",".") + "." + str(file).split("/")[-1][:4]})

    # Normalize konsol. to normal? Or need some comment??
    if "AusDenKonsolidiertenBilanzen" in new_data:
        new_data["AusDenBilanzen"] = new_data["AusDenKonsolidiertenBilanzen"]
    if "Konsolid.Gewinn-u.Verlustrechnungen" in new_data:
        new_data["AusGewinnVerlustrechnungen"] = new_data["Konsolid.Gewinn-u.Verlustrechnungen"]

    # Match content from Grundkapital and Bezugsrechte
    match_grundkapital_bezugsrechte(new_data)

    # Generate Year
    new_data.update({'year': str(file).split("/")[-2]})

    # Connect to db
    db_akf = dbPath
    engine = create_engine(db_akf)
    conn = engine.connect()
    # Drop it if it already exists (seems not necassary) this is legacy
    #conn.execute('''PRAGMA journal_mode = OFF''')

    # Create a MetaData instance
    metadata = MetaData(engine, reflect=True)

    # Read all tablenames
    tablenames = engine.table_names()

    # Check if a universal ID already exists
    if "overall_info" in new_data and "additionals" in new_data["overall_info"][0]:
        new_data.update({"additionals":list(new_data["overall_info"][0]["additionals"].values())[0]})
        new_data.update({'reference': get_refid(new_data["additionals"],df["WKN"],df["MainRelation"])})
        if not new_data['reference']:
            new_data['reference'] = new_data["unternehmenId"]
        else:
            new_data["id"] = str(df["MainRelation"].loc[df["MainRelation"]['weiteresAuftreten'] == new_data["reference"]]["id"].values[0])
    else:
        with open("./no_additionals.txt","a") as f:
            f.write(f"{file}\n")
        return

    # Start writing in the table
    print("TABLES")
    options = {
        'Aktienkurse': Aktienkursetable,
        'Aktionaer': Aktionaertable,
        'Anleihen': Anleihentable,
        'Aufsichtsrat': Aufsichtsrattable,
        'Beschaeftigte': Beschaeftigtetable,
        'Beteiligungen': Beteiligungentable,
        'BilanzAktiva': BilanzAktivatable,
        'BilanzPassiva': BilanzPassivatable,
        'BilanzSumme': BilanzSummetable,
        'Boersennotiz': Boersennotiztable,
        'Dependence': Dependencetable,
        'Dividenden': Dividendentable,
        'Geschaeftsjahr': Geschaeftsjahrtable,
        'Geschaeftsleitung': Geschaeftsleitungtable,
        'Grundkapital': Grundkapitaltable,
        'GuV': GuVtable,
        'Kapitalart': Kapitalarttable,
        'Kapitalentwicklung': Kapitalentwicklungtable,
        'Kennzahlen': Kennzahlentable,
        'Main': Maintable,
        'MainRelation': MainRelationtable,
        'Organbezuege': Organbezuegetable,
        'Stimmrecht': Stimmrechttable,
        'Stueckelung': Stueckelungtable,
        'Unternehmen': Unternehmentable,
        'Volume': Volumetable,
        'Vorstand': Vorstandtable,
        'WKN': WKNtable,
        'WeitereBemerkungen': WeitereBemerkungentable,
    }
    conn.close()
    for table in tablenames:
        #if table in ['MainRelation','Main']:
        #    continue
        print(table)
        conn = engine.connect()
        options[table](conn, new_data, metadata.tables[table], output_analysis)
        conn.close()
    engine.dispose()
    print("FINISHED!")
    return 0


def read_dataframe(dbPath,table):
    """
    Connects to sqlite, reads the table and finally closed the connection
    :param dbPath:
    :param table:
    :return:
    """
    engine = create_engine(dbPath)
    conn = engine.connect()
    df = pd.read_sql(table, conn,coerce_float=False)
    conn.close()
    engine.dispose()
    return df


def delete_all_table_contents(dbPath):
    """
    Cleanes the database.
    The flag can be set in the config.
    :param dbPath:
    :return:
    """
    db_akf = dbPath
    engine = create_engine(db_akf)
    conn = engine.connect()

    # Create a MetaData instance
    metadata = MetaData(engine, reflect=True)

    for name in metadata.tables:
        sql_command = "delete from " + name  # command for deleting every entry
        conn.execute(sql_command)


def main(config=None):
    """
    Reads the config file, search all folders and files and calls the db_updater function per file.
    The filespath are stored in the config.ini file and can be changed there.
    :return:
    """
    if not config:
        config = configparser.ConfigParser()
        config.sections()
        config.read('config.ini')

    output_analysis = OutputAnalysis(config)

    # For later use to iterate over all dir
    if config['DEFAULT']['SingleOn'] == "True":
        folders = [config['DEFAULT']['SinglePath']]
    else:
        my_path = config['DEFAULT']['AllPath'] + "/"
        # folders = glob.glob(my_path) # old way of obtaining all folders
        # define the path (with pathlib so absolute paths also work in unix)
        folders = sorted(glob.glob(my_path))


    dbPath = config['DEFAULT']['DBPath']
    t0all = time.time()

    # optional delete of content
    #if config['DEFAULT']['DeleteAllContentFirst'] != "False":
    #delete_all_table_contents(dbPath)

    accumulated_results = {}

    for folder in folders:
        #if "_1977_" in folder or "_1976_" in folder:
        #    continue
        """"" Read files """""
        if config['DEFAULT']['SingleOn'] == "False":
            files = get_files(folder)
        else:
            files = folders
        base_folder_name = os.path.basename(os.path.normpath(folder))  # for writing analysis

        #### Read dataframes ####
        df = {}
        df["MainRelation"] = read_dataframe(dbPath, "MainRelation")
        df["WKN"] = read_dataframe(dbPath, "WKN")
        df["volume"] = True
        output_analysis.change_folder()  # this deletes the current the current data block

        """"" Start Main """""
        for file in files:
            #if "0010_" not in file:
            #   continue
            akf_db_updater(file, dbPath, df, output_analysis)

        folder_results = output_analysis.output_result_folder(base_folder_name)
        accumulated_results = output_analysis.accumulate_final_results(folder_results, accumulated_results)
        print("The whole folder was finished in {}s".format(round(time.time() - t0all, 3)))

    output_analysis.log_final_results(accumulated_results)

################ START ################
if __name__ == "__main__":
    """
    Entrypoint: Searches for the files and parse them into the mainfunction (can be multiprocessed)
    """
    main()