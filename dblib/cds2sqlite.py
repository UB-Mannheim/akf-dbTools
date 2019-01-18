###################### INFORMATION #############################
#           It talks to the SQLite-DB and inserts the data of JSON-Files
# Program:  **AKF_SQL_DBTalk**
# Info:     **Python 3.6**
# Author:   **Jan Kamlah**
# Date:     **02.11.2017**

###################### GENERAL TO DO #############################
# TODO: - Anleihen, Dependence(Besitztümer), Volume existent?
# TODO: Notiz, Notizen, Bemerkung?! Handelsplatz...Boersennotiztable

######### IMPORT SQL ############
from sqlalchemy import create_engine, MetaData, select

######### IMPORT JSON ############
import json
import configparser
import os
from itertools import zip_longest
import string
import tempfile
from copy import deepcopy
import glob
from functools import wraps
import time, timeit
# Obsolete modules
# import random

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
def Aktienkursetable(conn, new_data, table):
    print(table.columns.keys())
    del_entry(new_data['compare'], [], ['year'])
    if 'boersenbewertung' not in new_data: return 0
    for boerse in new_data['boersenbewertung']:
        if 'kurse' not in new_data['boersenbewertung'][boerse]: continue
        for idxx, block in enumerate(new_data['boersenbewertung'][boerse]['kurse']):
            del_entry(new_data['compare'], ['boersenbewertung', boerse, 'kurse', idxx],
                      ['jahr', "hoechst", "tiefst", "ultimo", "kommentar"])
            del_entry(new_data['compare'], ['boersenbewertung', boerse, ], ['notiz_bereinigteKurse'])
            entry_check(block, ['jahr', "hoechst", "tiefst", "ultimo", "kommentar"])
            blockkey = list(block.keys())
            blockkey.remove('jahr')
            blockkey.remove('kommentar')
            currency, unit = get_currency_unit(
                {'waehrung': new_data['boersenbewertung'][boerse]['notiz_bereinigteKurse'].split("in")[-1].strip()})
            year = block['jahr'].replace("\xa0", " ")
            year = year.split(" ")[0]
            notes = new_data['boersenbewertung'][boerse]['notiz_bereinigteKurse']
            comment = replace_geminfo(block['jahr'], new_data['boersenbewertung'][boerse],'notiz_bereinigteKurse')
            for idx, entry in enumerate(blockkey):
                del_entry(new_data['compare'], [], ['year'])
                amount = block[entry]
                if "," in amount and amount.index(",") == 0:
                    amount = "0"+amount
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Jahr': year,
                     'Stichtag': block['kommentar'],
                     'Hoehe': amount,
                     'Waehrung': currency,
                     'Einheit': unit,
                     'Art': entry,
                     'Notiz': notes,
                     'Bemerkung': comment,
                     'BemerkungAbschnitt': "",
                     'Abschnitt': "",
                     'Rang': idx + 1,
                     }])
    return 0


def Aktionaertable(conn, new_data, table):
    print(table.columns.keys())
    if 'aktionaer' in new_data:
        if not 'gesellschafter' in new_data:
            new_data['gesellschafter'] = {'aktionaere':[]}
        new_data['gesellschafter']['aktionaere'] = [deepcopy(new_data['aktionaer'])][0]
        del new_data['aktionaer']
        del_entry(new_data,['compare'], ['aktionaer'])
    if 'gesellschafter' in new_data:
        for name in new_data['gesellschafter']:
            if name in ['aktionaere', 'anteilseigner', 'kommanditisten']:
                for idx, entry in enumerate(new_data['gesellschafter'][name]):
                    del_entry(new_data['compare'], ['gesellschafter', name, idx],
                              ['beteiliger', 'ort', 'anteil', 'bemerkung'])
                    entry_check(entry, ['beteiliger', 'ort', 'anteil', 'bemerkung', "bemerkungen"])
                    comment = entry['bemerkung']
                    if comment == "":
                        comment = " ".join(entry["bemerkungen"])
                    if 'name' in entry:
                        entry['beteiliger'] = entry['name']
                    pwords = ["u.", "%", "über", "ca.", "Kdt.", "Inc.", "dir."]
                    for word in pwords:
                        if word in entry['ort']:
                            comment = " Info: " + entry['ort'] + " " + comment
                            entry['ort'] = ""
                            break
                    aktionear = ""
                    if len(entry['beteiliger']) > 1:
                        if ":" == entry['beteiliger'].strip()[0]:
                            aktionear = entry['beteiliger'].replace(":","").strip()
                        else:
                            aktionear = entry['beteiliger'].strip()
                    conn.execute(table.insert(), [
                        {'unternehmenId': new_data['unternehmenId'],
                         'Name': aktionear,
                         'Ort': entry['ort'],
                         'Anteil': entry['anteil'],
                         'Abschnitt': "",
                         'Bemerkung': comment.strip(),
                         'BemerkungAbschnitt': "",
                         'Rang': idx + 1,
                         }])
    return 0


def Anleihentable(conn, new_data, table):
    print(table.columns.keys())
    return 0
    if 'anleihen' not in new_data: return 0
    for idx, entry in enumerate(new_data['all_wkn_entry']):
        entry_check(entry, ['name', 'ort', 'anteil', 'bemerkung'])
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Anleihen': new_data['name'],
             'Rang': idx + 1,
             }])
    return 0


def Aufsichtsrattable(conn, new_data, table):
    print(table.columns.keys())
    if 'aufsichtsrat' not in new_data: return 0
    for idx, entry in enumerate(new_data['aufsichtsrat']):
        del_entry(new_data['compare'], ['aufsichtsrat', idx], ['lastName', 'firstName', 'title', 'cityAcc', 'funct'])
        entry_check(entry, ['lastName', 'firstName', 'title', 'cityAcc', 'funct'])
        if membercheck(entry): continue
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Name': entry['lastName'],
             'Vorname': entry['firstName'],
             'Titel': entry['title'],
             'Ort': entry['cityAcc'],
             'Funktion': entry['funct'],
             'Bemerkung': "",
             'Rang': idx + 1,
             }])
    return 0


def Beschaeftigtetable(conn, new_data, table):
    print(table.columns.keys())
    if 'boersenbewertung' not in new_data: return 0
    for boerse in new_data['boersenbewertung']:
        if 'kennzahlen' not in new_data['boersenbewertung'][boerse]: continue
        if "Mitarbeiter" in new_data['boersenbewertung'][boerse]['kennzahlen']:
            new_data['boersenbewertung'][boerse]['kennzahlen']["Beschäftigte"] = new_data['boersenbewertung'][boerse]['kennzahlen'].pop("Mitarbeiter")
        if "Beschäftigte" not in new_data['boersenbewertung'][boerse]['kennzahlen']: continue
        for idx, block in enumerate(new_data['boersenbewertung'][boerse]['kennzahlen']["Beschäftigte"]):
            blockkeys = list(block.keys())
            entry_check(block, ['jahr'])
            year = block['jahr'].replace("\xa0", " ")
            year = year.split(" ")[0]
            if "*" in year:
                year = year.split("*")[0]
            blockkeys.remove("jahr")
            comment = replace_geminfo(block['jahr'], "", "")
            for key in blockkeys:
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Stichtag': year,
                     'Anzahl': block[key],
                     'Notiz': comment,
                     'Bemerkung': key,
                     'Rang': idx + 1,
                     }])
    return 0


def Beteiligungentable(conn, new_data, table):
    print(table.columns.keys())
    if 'beteiligungen' not in new_data: return 0
    if len(new_data['beteiligungen']) == 0: return 0
    idx = 0
    if "elemente" in new_data['beteiligungen']:
        new_data['beteiligungen'] = new_data['beteiligungen']['elemente']
    for ct, _ in enumerate(new_data['beteiligungen']):
        if 'zeilen' not in new_data['beteiligungen'][ct - idx]:
            if not isinstance(new_data['beteiligungen'][ct - idx], str):
                new_data['beteiligungen'][ct - idx]['zeilen'] = [deepcopy(new_data['beteiligungen'][ct - idx])]
                del_entry(new_data['compare'], ['beteiligungen'], [ct - idx])
            else:
                del new_data['beteiligungen'][ct - idx]
                new_data['beteiligungen'].append([])
                del_entry(new_data['compare'], ['beteiligungen'], [ct - idx])
                idx += 1
                continue
    if idx != 0:
        del new_data['beteiligungen'][len(new_data['beteiligungen']) - idx:]
    comment = ""
    count = 0
    for ix ,block in enumerate(new_data['beteiligungen']):
        for idx, entry in enumerate(block['zeilen']):
            addcomment = ""
            if isinstance(entry, str):
                count +=1
                continue
            del_entry(new_data['compare'], ['beteiligungen', ix], ['beteiliger', 'ort', 'anteil'])
            entry_check(entry, ['beteiliger', 'ort', 'anteil'])
            if entry['anteil'] != "":
                share, share_pc, currency = get_share(entry['anteil'])
            else:
                share, share_pc, currency = get_share(entry['beteiliger'])
            if entry['ort'] == "" and entry['anteil'] == "" and "%" not in entry['beteiliger']:
                comment = " Gruppe: "+entry['beteiliger'].replace(":","")
                count += 1
                continue
            #Added new feat only for 2001 and less
            if entry['ort'] == "" and len(entry['anteil'].split(".")) > 1 and int(new_data['year'].split("-")[0])<2002:
                entry['ort'] = entry['anteil'].split(".")[0]
                entry['anteil'] = entry['anteil'].replace(entry['ort']+".","").strip()
            pwords = ["u.","%","ca.","Kdt.","Inc.","dir."]
            if "über" in entry['ort']:
                share_pc = "über "+share_pc
                entry['ort'] = entry['ort'].replace('über',"")
            for word in pwords:
                if word in entry['ort']:
                    addcomment = " Info: "+entry['ort']
                    entry['ort'] = ""
                    break
            headline =""
            if not "ohne_titel" == block['ueberschrift']:
                headline = block['ueberschrift']+" "+comment
            entry_check(block, ['ueberschrift'])
            del_entry(new_data['compare'], ['beteiligungen'], ['ueberschrift'])
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Firmenname': entry['beteiliger'],
                 'Ort': entry['ort'],
                 'Kapitalbetrag': share,
                 'Waehrung': currency,
                 'Anteil': share_pc,
                 'Bemerkung': (headline+" "+comment+addcomment).strip(),
                 'Rang': idx + 1 - count,
                 }])
    return 0


def BilanzAktivatable(conn, new_data, table):
    print(table.columns.keys())
    if 'ausBilanzen' not in new_data: return 0
    uinfo = "The amount was considered to low to name it. "
    for ct, _ in enumerate(new_data['ausBilanzen']):
        for idx, block in enumerate(new_data['ausBilanzen'][ct]['ausBilanzen']):
            if 'aktiva' in block.lower():
                currency, unit = get_currency_unit(new_data['ausBilanzen'][ct])
                for entries in new_data['ausBilanzen'][ct]['ausBilanzen'][block]:
                    lvalidpos = ""
                    year = ""
                    if "jahr" in entries:
                        year = entries['jahr'].replace("\xa0", " ")
                        year = year.split(" ")[0]
                        if "*" in year:
                            year = year.split("*")[0]
                    companystage = "AG"
                    if 'columnId' in entries:
                        companystage = entries['columnId']
                    comment = replace_geminfo(entries['jahr'], new_data['ausBilanzen'][ct], 'notizen')
                    for idxx, entry in enumerate(entries):
                        if entry == "jahr" or entry == "columnId":continue
                        #entity_check(entity, ['beteiliger', 'ort', 'anteil'])
                        if entries[entry].upper() == "U":
                            entries[entry] = ""
                            comment = uinfo + comment
                        pos = entry
                        if entry[0].isalpha():
                            pos = entry.title()
                            lvalidpos = pos
                        elif entry[0] == ".":
                            pos = lvalidpos + "(" + entry + ")"
                        conn.execute(table.insert(), [
                            {'unternehmenId': new_data['unternehmenId'],
                             'Konzernebene': companystage,
                             'Bilanzposition': pos,
                             'Jahr': year,
                             'Einheit': unit,
                             'Waehrung': currency,
                             'Betrag': entries[entry].replace(' ', "").replace(" ", ""),
                             'Bemerkung': comment,
                             'BemerkungAbschnitt': "",
                             'Rang': idxx + 1,
                             }])
                break
            if "u =" in block.lower():
                uinfo = block
    return 0


def BilanzPassivatable(conn, new_data, table):
    print(table.columns.keys())
    if 'ausBilanzen' not in new_data: return 0
    uinfo = "The amount was considered to low to name it. "
    for ct, _ in enumerate(new_data['ausBilanzen']):
        for idx, block in enumerate(new_data['ausBilanzen'][ct]['ausBilanzen']):
            if 'passiva' in block.lower():
                currency, unit = get_currency_unit(new_data['ausBilanzen'][ct])
                for entries in new_data['ausBilanzen'][ct]['ausBilanzen'][block]:
                    lvalidpos = ""
                    year = ""
                    if "jahr" in entries:
                        year = entries['jahr'].replace("\xa0", " ")
                        year = year.split(" ")[0]
                        if "*" in year:
                            year = year.split("*")[0]
                    companystage = "AG"
                    if 'columnId' in entries:
                        companystage = entries['columnId']
                    comment = replace_geminfo(entries['jahr'], new_data['ausBilanzen'][ct], 'notizen')
                    for idxx, entry in enumerate(entries):
                        if entry == "jahr" or entry == "columnId": continue
                        # entity_check(entry, ['beteiliger', 'ort', 'anteil'])
                        if entries[entry].upper() == "U":
                            entries[entry] = ""
                            comment = uinfo + comment
                        pos = entry
                        if entry[0].isalpha():
                            pos = entry.title()
                            lvalidpos = pos
                        elif entry[0] == ".":
                            pos = lvalidpos + "(" + entry + ")"
                        conn.execute(table.insert(), [
                            {'unternehmenId': new_data['unternehmenId'],
                             'Konzernebene': companystage,
                             'Bilanzposition': pos,
                             'Jahr': year,
                             'Einheit': unit,
                             'Waehrung': currency,
                             'Betrag': entries[entry].replace(' ', "").replace(" ", ""),
                             'Bemerkung': comment,
                             'BemerkungAbschnitt': "",
                             'Rang': idxx + 1,
                             }])
                break
            if "u =" in block.lower():
                uinfo = block
    return 0


def BilanzSummetable(conn, new_data, table):
    print(table.columns.keys())
    if 'ausBilanzen' not in new_data: return 0
    uinfo = "The amount was considered to low to name it. "
    for ct, _ in enumerate(new_data['ausBilanzen']):
        for idx, block in enumerate(new_data['ausBilanzen'][ct]['ausBilanzen']):
            if 'bilanzsumme' in block.lower():
                currency, unit = get_currency_unit(new_data['ausBilanzen'][ct])
                for entries in new_data['ausBilanzen'][ct]['ausBilanzen'][block]:
                    lvalidpos = ""
                    year = ""
                    if "jahr" in entries:
                        year = entries['jahr'].replace("\xa0", " ")
                        year = year.split(" ")[0]
                        if "*" in year:
                            year = year.split("*")[0]
                    companystage = "AG"
                    if 'columnId' in entries:
                        companystage = entries['columnId']
                    comment = replace_geminfo(entries['jahr'], new_data['ausBilanzen'][ct], 'notizen')
                    for idxx, entry in enumerate(entries):
                        if entry == "jahr" or entry == "columnId": continue
                        # entry_check(entity, ['beteiliger', 'ort', 'anteil'])
                        if entries[entry].upper() == "U":
                            entries[entry] = ""
                            comment = uinfo + comment
                        pos = entry
                        if entry[0].isalpha():
                            pos = entry.title()
                            lvalidpos = pos
                        elif entry[0] == ".":
                            pos = lvalidpos + "(" + entry + ")"
                        conn.execute(table.insert(), [
                            {'unternehmenId': new_data['unternehmenId'],
                             'Konzernebene': companystage,
                             'Bilanzposition': pos,
                             'Jahr': year,
                             'Einheit': unit,
                             'Waehrung': currency,
                             'Betrag': entries[entry].replace(' ', "").replace(" ", ""),
                             'Bemerkung': comment,
                             'BemerkungAbschnitt': "",
                             'Rang': idxx + 1,
                             }])
                break
            if "u =" in block.lower():
                uinfo = block
    return 0


def Boersennotiztable(conn, new_data, table):
    print(table.columns.keys())
    if 'boersenbewertung' not in new_data: return 0
    for idx, block in enumerate(new_data['boersenbewertung']):
        del_entry(new_data['compare'], ['boersenbewertung', block], ['notizen_kennzahlen', 'notizen', 'marktbetreuer'])
        notes = ""
        comment= ""
        if 'notizen_kennzahlen' in new_data['boersenbewertung'][block]:
            notes += " ".join(new_data['boersenbewertung'][block]['notizen_kennzahlen'])
        # if 'notizen' in new_data['boersenbewertung'][block]:
        #     notes += " ".join(new_data['boersenbewertung'][block]['notizen'])
        notes= notes.replace("i) gemäß IAS", " ").replace("g) gemäß US-GAAP", " ").replace("Beschäftigte", " ").replace("_","").replace("  "," ").strip()
        if 'marktbetreuer' in new_data['boersenbewertung'][block]:
            new_data["Marktbetreuer"] = ", ".join(new_data['boersenbewertung'][block]['marktbetreuer'])
            #TODO-Hint: Obsolete? The information are in "WeitereBemerkungen"
            #comment = "Marktbetreuer: "+new_data["Marktbetreuer"]+", "
        if len(notes) > 1:
            notes = notes[0].upper() + notes[1:]
        maerktelist = []
        with open("./dblib/Maerkte","r", encoding="utf-8") as f:
            for line in f.readlines():
                if line.strip() in notes:
                    maerktelist.append(line.strip())
        comment = ""
        for idx, markt in enumerate(maerktelist):
            if idx == len(maerktelist)-1:
                comment = notes
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Handelsplatz': markt,
                 'Abschnitt': "",
                 'Bemerkung': comment,
                 'Rang': idx + 1,
                 }])
    return 0


def Dependencetable(conn, new_data, table):
    print(table.columns.keys())
    if 'dependence' not in new_data: return 0
    for idx, _ in enumerate(new_data['all_wkn_entry']):
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Dependence': new_data['name'],
             'Bezeichnung': new_data['all_wkn_entry'][idx]['wkn'],
             }])
    return 0


def Dividendentable(conn, new_data, table):
    print(table.columns.keys())
    if 'boersenbewertung' not in new_data: return 0
    for block in new_data['boersenbewertung']:
        if 'dividenden' not in new_data['boersenbewertung'][block]: continue
        del_entry(new_data['compare'], ['boersenbewertung', block],
                  ['dividenden', 'dividenden_notiz', "dividenden_bemerkungen", "wkns", "isins"])
        for idx, entries in enumerate(new_data['boersenbewertung'][block]['dividenden']):
            entry_check(entries, ['jahr', 'dividende', 'steuerguthaben', 'bonus'])
            entry_check(new_data['boersenbewertung'][block], ['dividenden_notiz','dividenden_bemerkungen'])
            year = entries['jahr'].replace("\xa0", " ")
            year = year.split(" ")[0]
            if ")" in year:
                year = year.strip().split(")")[0][:-1]
            comment = new_data['boersenbewertung'][block]['dividenden_notiz']
            if isinstance(comment,list): comment = " ".join(comment)
            extracomment = ""
            for entry in entries:
                if entry not in ['jahr', 'dividende', 'steuerguthaben', 'bonus']:
                    extracomment += entry+": "+entries[entry]+", "
            extracomment = replace_geminfo(entries['jahr'], new_data['boersenbewertung'][block], 'dividenden_bemerkungen')+" "+extracomment
            currency, div_bemerk= "", ""
            if new_data['boersenbewertung'][block]["dividenden_bemerkungen"] != "":
                div_bemerk = new_data['boersenbewertung'][block]["dividenden_bemerkungen"][0]
            Divnr, type  = "", ""
            if 'wkns' in new_data['boersenbewertung'][block]:
                if 'aktienart' in new_data['boersenbewertung'][block]["wkns"][0]:
                    type = new_data['boersenbewertung'][block]["wkns"][0]["aktienart"]
            elif 'isins' in new_data['boersenbewertung'][block]:
                if 'aktienart' in new_data['boersenbewertung'][block]["isins"][0]:
                    type = new_data['boersenbewertung'][block]["isins"][0]["aktienart"]
            if div_bemerk.find("Sch") != -1:
                Divnrsplit = div_bemerk.strip().split(" ")
                Divnr = Divnrsplit[-1] if len(Divnrsplit) > 1 else Divnrsplit[0]
            if new_data['boersenbewertung'][block]["dividenden_notiz"] != "":
                currency = new_data['boersenbewertung'][block]["dividenden_notiz"].split("in")[1]
                if "in " in new_data['boersenbewertung'][block]["dividenden_notiz"].strip()[:3]:
                    currency = new_data['boersenbewertung'][block]["dividenden_notiz"].replace("in ","").strip().split(" ")[0]
                else:
                    for cur in ["TEUR","EUR","USD","DM"]:
                        if cur in new_data['boersenbewertung'][block]["dividenden_notiz"].upper():
                            currency = cur
            elif "dividenden_bemerkungen" in new_data['boersenbewertung'][block]:
                for entry in new_data['boersenbewertung'][block]["dividenden_bemerkungen"]:
                    if "Bereinigte Kurse" in entry:
                        try:
                            currency = entry.split("in")[-1].replace(")", "")
                        except Exception:
                            pass
            dividende = entries["dividende"]
            if len(entries["dividende"].split(" ")) > 1:
                if "%" in entries["dividende"]:
                    dividende = entries["dividende"].split(" ")[0].strip()
                    currency = entries["dividende"].split(" ")[-1].strip()
                elif ")" in entries["dividende"]:
                    dividende = entries["dividende"].split(" ")[0].strip()
                    extracomment += "Zusatz: "+entries["dividende"].split(" ")[-1]+" "
                else:
                    dividende = entries["dividende"].split(" ")[-1].strip()
                    currency = entries["dividende"].split(" ")[0].upper()
            #Clean the data
            if len(currency) > 1:
                if ";" == currency[-1] or "," == currency[-1] or "/" == currency[-1] or ":" == currency[-1]:
                    currency = currency[:-1]
            stg = entries["steuerguthaben"]
            if len(entries["steuerguthaben"].split(" ")) > 1:
                if entries["steuerguthaben"].split(" ")[0].upper() == currency:
                    stg = entries["steuerguthaben"].split(" ")[1]
            extracomment += " Dividendbemerkungen: "+" ".join(new_data['boersenbewertung'][block]["dividenden_bemerkungen"])
            bonus = entries["bonus"].replace(currency,"").replace(currency.lower(),"")
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Jahr': year,
                 'Aktienart': type.strip(),
                 'Nennwert': "",
                 'Dividende': dividende,
                 'Bonus': bonus,
                 'St_G': stg,
                 'Waehrung': currency,
                 'Div_Sch_Nr': Divnr,
                 'Bemerkung': comment,
                 'BemerkungAbschnitt': extracomment.strip(),
                 'Abschnitt': "",
                 'Rang': idx + 1,
                 }])
    return 0


def Geschaeftsjahrtable(conn, new_data, table):
    print(table.columns.keys())
    if 'sonstigeAngaben' not in new_data: return 0
    del_entry(new_data['compare'], [], ['sonstigeAngaben'])
    KJ, GJ, GJA, GJE = "0","", "", ""
    for entry in new_data['sonstigeAngaben']:
        if entry[0].find('jahr') != -1:
            GJ = " ".join(entry[1:])
    if len(GJ.split("-")) > 1:
        GJA = GJ.split("-")[0]
        GJE = GJ.split("-")[1]
        GJ = ""
    if "Kalenderjahr" in GJA:
        KJ = "1"
        GJ += "Kalenderjahr"
        GJA = GJA.replace("Kalenderjahr", "")
    if not "".join([GJ,GJA,GJE]) == "":
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Kalenderjahr': KJ,
             'Geschaeftsjahresanfang': GJA,
             'Geschaeftsjahresende': GJE,
             'Bemerkung': GJ,
             'Abschnitt': "",
             }])
    return 0


def Geschaeftsleitungtable(conn, new_data, table):
    print(table.columns.keys())
    if 'geschleitung' not in new_data: return 0
    for idx, entry in enumerate(new_data['geschleitung']):
        del_entry(new_data['compare'], ['geschleitung', idx], ['lastName', 'firstName', 'title', 'cityAcc', 'funct'])
        entry_check(entry, ['lastName', 'firstName', 'title', 'cityAcc', 'funct'])
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Name': entry['lastName'],
             'Vorname': entry['firstName'],
             'Titel': entry['title'],
             'Ort': entry['cityAcc'],
             'Funktion': entry['funct'],
             'Bemerkung': "",
             'Rang': idx + 1,
             }])
    return 0


def Grundkapitaltable(conn, new_data, table):
    print(table.columns.keys())
    if 'shareinfo' not in new_data: return 0
    del_entry(new_data['compare'], [], ['grundkapital'])
    for idx, entry in enumerate(new_data["shareinfo"]):
        if (entry['amount']+entry['currency']+entry['info']).strip() == "":continue
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Hoehe': entry['amount'],
             'Waehrung': entry['currency'],
             'Bemerkung': entry['info'],
             'Rang': idx+1,
             }])
    return 0


def GuVtable(conn, new_data, table):
    print(table.columns.keys())
    if 'ausGewinnUndVerlust' not in new_data: return 0
    if 'ausGewinnUndVerlustRechnung' in new_data['ausGewinnUndVerlust']:
        del_keys = list(new_data['ausGewinnUndVerlust'].keys())
        new_data['ausGewinnUndVerlust'][0] = deepcopy(new_data['ausGewinnUndVerlust'])
        for _ in del_keys:
            del new_data['ausGewinnUndVerlust'][_]
    for ct, _ in enumerate(new_data['ausGewinnUndVerlust']):
        if 'ausGewinnUndVerlustRechnung' not in new_data['ausGewinnUndVerlust'][ct]: continue
        for block in new_data['ausGewinnUndVerlust'][ct]['ausGewinnUndVerlustRechnung']:
            del_entry(new_data['compare'], ['ausGewinnUndVerlust', ct],
                      ['ausGewinnUndVerlustRechnung', 'notizen', 'ausGewinnUndVerlust', 'waehrung'])
            currency, unit = get_currency_unit(new_data['ausGewinnUndVerlust'][ct])
            entry_check(block, ['columnId', 'jahr'])
            blockkey = list(block.keys())
            blockkey.remove('jahr')
            blockkey.remove('columnId')
            year = block['jahr'].replace("\xa0", " ")
            year = year.split(" ")[0]
            if "*" in year:
                year = year.split("*")[0]
            lvalidpos = ""
            offset = 0
            for idx, entry in enumerate(blockkey):
                comment = replace_geminfo(block['jahr'], new_data['ausGewinnUndVerlust'][ct], 'notizen')
                pos = ""
                if block[entry].upper() == "U":
                    block[entry] = ""
                    comment = "The amount was considered to low to name it. " + comment
                if entry[0].isalpha():
                    pos = entry.title()
                    lvalidpos = pos
                elif entry[0] == ".":
                    pos = lvalidpos + "(" + entry + ")"
                if pos != "":
                    conn.execute(table.insert(), [
                        {'unternehmenId': new_data['unternehmenId'],
                         'Konzernebene': block['columnId'],
                         'GuVPosition': pos,
                         'Jahr': year,
                         'Einheit': unit,
                         'Waehrung': currency,
                         'Betrag': block[entry].replace(' ', "").replace(" ", ""),
                         'Bemerkung': comment,
                         'BemerkungAbschnitt': "",
                         'Rang': idx + 1-offset,
                         }])
                else:
                    offset += 1
    return 0


def Kapitalarttable(conn, new_data, table):
    print(table.columns.keys())
    entries = []
    entry_names = []
    del_entry(new_data['compare'], [],
              ['genehmigtesKapital', 'bedingtesKapital', 'besBezugsrechte', 'ermächtigungAktienerwerb',
               'bedingtesKapital2'])
    if 'genehmigtesKapital' in new_data:
        if new_data['genehmigtesKapital']:
            entries.append(new_data['genehmigtesKapital']['genehmKapital'])
            entry_names.append('Genehmigtes Kapital')
    if 'genehmigtesGenusKapital' in new_data:
        if new_data['genehmigtesGenusKapital']:
            entries.append(new_data['genehmigtesGenusKapital']['genehmKapital'])
            entry_names.append('Genehmigtes Genusskapital')
    if 'derzeitigesGenusKapital' in new_data:
        if new_data['derzeitigesGenusKapital']:
            text = ""
            if "bemerkungen" in new_data['derzeitigesGenusKapital']:
                for entry in new_data['derzeitigesGenusKapital']["bemerkungen"]:
                    if isinstance(entry, list):
                        text += " ".join(entry)+" "
                    else:
                        text += entry+ " "
            entries.append({'betrag': new_data['derzeitigesGenusKapital']['betrag'], 'bemerkung': text})
            entry_names.append('Derzeitiges Genusskapital')
    if 'bedingtesKapital' in new_data:
        if new_data['bedingtesKapital']:
            amount = ""
            if new_data['bedingtesKapital']['bedingkapital']["betrag"] != "":
                amount = new_data['bedingtesKapital']['bedingkapital']["betrag"].strip()
            comment = ""
            for idx, entry in enumerate(new_data['bedingtesKapital']['bedingkapital']["eintraege"]):
                if entry["betrag_einzel"] != "":
                    comment = ""
                    if len(entry["betrag_einzel"].strip().split(" ")) > 1:
                        currency = entry["betrag_einzel"].strip().split(" ")[0]
                        amount = currency + " "+entry["betrag_einzel"].replace(currency, "").strip().split("Gem")[0]
                        comment += " ,"+entry["betrag_einzel"].replace(amount,"").replace(currency,"")
                    else:
                        amount = entry["betrag_einzel"].strip()
                    comment = entry["bemerkung"]+comment
                    entries.append({'betrag': amount, 'bemerkung': comment})
                    entry_names.append('Bedingtes Kapital')
                else:
                    comment += entry["bemerkung"]+" "
                    if idx == len(new_data['bedingtesKapital']['bedingkapital']["eintraege"])-1:
                        entries.append({'betrag':amount,'bemerkung':comment})
                        entry_names.append('Bedingtes Kapital')
    if 'bedingtesKapital2' in new_data:
        if new_data['bedingtesKapital']:
            amount = ""
            if new_data['bedingtesKapital']['bedingkapital']["betrag"] != None:
                amount = new_data['bedingtesKapital']['bedingkapital']["betrag"]
            comment = ""
            for idx, entry in enumerate(new_data['bedingtesKapital']['bedingkapital']["eintraege"]):
                if entry["betrag_einzel"] != None:
                    amount = entry["betrag_einzel"]
                    comment = entry["bemerkung"]
                    entries.append({'betrag': amount, 'bemerkung': comment})
                    entry_names.append('Bedingtes Kapital')
                else:
                    comment += entry["bemerkung"]+" "
                    if idx == len(new_data['bedingtesKapital']['bedingkapital']["eintraege"]):
                        entries.append({'betrag':amount,'bemerkung':comment})
                        entry_names.append('Bedingtes Kapital')
    if entries:
        for idx, entry in enumerate(entries):
            entry_check(entry, ['betrag', 'bemerkung'])
            currency, amount = "", ""
            if entry['betrag'] != "":
                currency = entry['betrag'].translate(str.maketrans('', '', string.punctuation + string.digits)).strip()
                amount = entry['betrag']
                for feat in currency.split():
                    if feat.strip() in ["Mio","Mrd","Tsd","Brd"]:
                        currency = currency.replace(feat, '')
                    else:
                        amount = amount.replace(feat, '')
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Kapitalart': entry_names[idx],
                 'Hoehe': amount.replace(' ', "").replace(" ", ""),
                 'Waehrung': currency,
                 'Bemerkung': entry['bemerkung'],
                 'Rang': idx + 1,
                 }])
    return 0


def Kapitalentwicklungtable(conn, new_data, table):
    print(table.columns.keys())
    if 'kapitalEntwicklung' not in new_data: return 0
    del_entry(new_data['compare'], [], ['kapitalEntwicklung'])
    for entries in new_data['kapitalEntwicklung']:
        if not 'eintraege' in entries: continue
        for idx, entry in enumerate(entries['eintraege']):
            entry_check(entry, ['jahr', 'art', 'text', 'betrag'])
            if isinstance(entry['text'],str):
                text = entry['text']
            else:
                text = " ".join(entry['text'])
            if (entry['art'] + entry['betrag'] + text).strip()  != "":
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Jahr': entries['jahr'],
                     'Text': "Art: " + entry['art'] + ", Kapital: " + entry['betrag'] + ", Info: " + text,
                     'Bemerkung': "Kapital",
                     'Rang': idx + 1,
                     }])
    if 'entwicklungDesGenusKapitals' not in new_data: return 0
    for idx, entry in enumerate(new_data['entwicklungDesGenusKapitals']):
            entry_check(entry, ['jahr', 'text'])
            if isinstance(entry['text'],str):
                text = entry['text']
            else:
                text = " ".join(entry['text'])
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Jahr': entry['jahr'],
                 'Text': text,
                 'Bemerkung': "Genußkapital",
                 'Rang': idx + 1,
                 }])
    return 0


def Kennzahlentable(conn, new_data, table):
    """
    In this case we have different conditions.
    Is not a pretty solution.
    """
    print(table.columns.keys())
    if 'boersenbewertung' not in new_data: return 0
    for boerse in new_data['boersenbewertung']:
        if 'kennzahlen' not in new_data['boersenbewertung'][boerse]: continue
        featkeys = list(new_data['boersenbewertung'][boerse]['kennzahlen'].keys())
        if "Beschäftigte" in featkeys: featkeys.remove("Beschäftigte")
        addid = []
        addid.append(0)
        for id, feat in enumerate(featkeys):
            for idx, block in enumerate(new_data['boersenbewertung'][boerse]['kennzahlen'][feat]):
                del_entry(new_data['compare'], ['boersenbewertung', boerse,'kennzahlen'], [feat])
                entry_check(block, ['jahr'])
                entry_check(new_data['boersenbewertung'][boerse], ['waehrungsinfo', 'notizen_kennzahlen'])
                del_entry(new_data['compare'], ['boersenbewertung', boerse], ['waehrungsinfo', 'notizen_kennzahlen'])
                waehrungsinfo = ""
                keys = list(block.keys())
                try:
                    keys.remove('jahr')
                except Exception:
                    pass
                unit, currency = "", ""
                comment = ""
                if isinstance(new_data['boersenbewertung'][boerse]['notizen_kennzahlen'], list):
                    if "in" in new_data['boersenbewertung'][boerse]['notizen_kennzahlen'][-1]:
                        currency = new_data['boersenbewertung'][boerse]['notizen_kennzahlen'][-1].split("in")[-1].replace(
                            ")", "").strip()
                for idxx, entry in enumerate(keys):
                    if isinstance(block[entry],str):
                        block[entry] = {entry: block[entry]}
                    for idxxx, name in enumerate(block[entry]):
                        if 'waehrungsinfo' in new_data['boersenbewertung'][boerse]:
                            for infolist in new_data['boersenbewertung'][boerse]['waehrungsinfo']:
                                if infolist['name'] == feat:
                                    for info in infolist['eintraege']:
                                        if info["name"] == name:
                                            waehrungsinfo = info["waehrung"]
                        if isinstance(waehrungsinfo,str):
                            cuinfo = get_currencyinfo(["("+waehrungsinfo+")"])
                        else:
                            cuinfo = get_currencyinfo(waehrungsinfo)
                        if cuinfo:
                            if len(keys) > 1 or len(block[entry]) > len(keys):
                                if len(cuinfo) == 1:
                                    unit = cuinfo[0]['unit']
                                    currency = cuinfo[0]['currency']
                                else:
                                    unit = cuinfo[idxx]['unit']
                                    currency = cuinfo[idxx]['currency']
                            else:
                                unit = cuinfo[idxx]['unit']
                                currency = cuinfo[idxx]['currency']
                        currency = currency.replace("in ","").strip()
                        year = block['jahr'].replace("\xa0", " ")
                        year = year.split(" ")[0]
                        if "*" in year:
                            year = year.split("*")[0]
                        comment = replace_geminfo(block['jahr'], new_data['boersenbewertung'][boerse],
                                                  'notizen')
                        entryinfo = ""
                        pprtname = name
                        if "(" in pprtname:
                            pprtname = pprtname.split("(")[0].strip()
                        if "gesamt" in name.lower():
                            entryinfo = " " + cuinfo[0]["text"]
                        conn.execute(table.insert(), [
                            {'unternehmenId': new_data['unternehmenId'],
                             'Kennzahlenposition': pprtname+entryinfo,
                             'Jahr': year,
                             'Einheit': unit,
                             'W\xe4hrung': currency,
                             'Betrag': block[entry][name].replace(' ', "").replace(" ", ""),
                             'Bemerkung': comment,
                             'Rang': idxxx + 1,
                             }])
    return 0


def Maintable(conn, new_data, table):
    print(table.columns.keys())
    if new_data['reference'] == new_data['unternehmenId']:
        nextid = get_lastid(table, conn)
        new_data['id'] = nextid
        conn.execute(table.insert(), [
            {'name': new_data['name'],
             'referenz': new_data['reference'],
             'Erstnotierung': new_data['year'],
             'Letztnotierung': "",
             'imAktienfuehrer': new_data['year'],
             'Bemerkung': "",
             'Indexzugeh\xf6rigkeit': "",
             'id': nextid,
             }])
    return 0


def MainRelationtable(conn, new_data, table):
    print(table.columns.keys())
    #if new_data['reference'] != new_data['unternehmenId']:
    conn.execute(table.insert(), [
        {'referenz': new_data["reference"],
         'weiteresAuftreten': new_data["unternehmenId"],
         'Unternehmen': new_data['name'],
         'Erscheinungsjahr': int(new_data['year'].split("-")[0]),
         'id': new_data['id'],
         }])
    return 0


def Organbezuegetable(conn, new_data, table):
    print(table.columns.keys())
    if 'organbezuege' not in new_data: return 0
    del_entry(new_data['compare'], [], ['organbezuege'])
    for idx, _ in enumerate(new_data['organbezuege']):
        for entry in new_data['organbezuege'][idx]:
            entry_check(entry, ['bezuege', 'organ'])
            bezuege = entry['bezuege']
            organ = entry['organ'].translate(str.maketrans('', '', string.punctuation + string.digits)).strip()
            if 'bemerkung' in entry and bezuege == "":
                bezuege = entry['bemerkung']
                if "ufsicht" in entry['bemerkung'] and organ == "":
                    organ = "Aufsichtsrat"
                elif "Vorstand" in entry['bemerkung'] and organ == "":
                    organ = "Vorstand"
            else:
                if "Aufsichtsrat" in entry['bezuege']:
                        bezuege = entry['bezuege'].split("Aufsichtsrat")[0].replace(", ","").strip()
                        new_data['organbezuege'].append([{'organ':"Aufsichtsrat",'bezuege':entry['bezuege'].split("Aufsichtsrat")[1].replace(", ","").strip()}])
                elif "Vorstand" in entry['bezuege']:
                        bezuege = entry['bezuege'].split("Vorstand")[0].replace(", ","").strip()
                        new_data['organbezuege'].append([{'organ':"Aufsichtsrat",'bezuege':entry['bezuege'].split("Vorstand")[1].replace(", ","").strip()}])
            if bezuege == "":continue
            if organ == "":
                organ = "Organbezuege"
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Organ': organ,
                 'Bez\xfcge': bezuege,
                 }])
    return 0


def Stimmrechttable(conn, new_data, table):
    """
    Stimmrecht entry could be in the first layer or
    in the "ausgebenes Kapital" entryblock.
    """
    print(table.columns.keys())
    if "shareinfo" not in new_data: return 0
    for idx, entry in enumerate(new_data["shareinfo"]):
        if entry["voice"] != "":
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Aktienart': entry["type"],
                 'Stueck': entry["number"].strip(),
                 'Stimmzahl': entry["voice"].strip(),
                 'Nennwert': entry["nw"],
                 'Waehrung': entry["currency"],
                 'Bemerkung': entry["info"],
                 'Rang': idx + 1,
                 }])
    return 0


def Stueckelungtable(conn, new_data, table):
    print(table.columns.keys())
    if "shareinfo" not in new_data: return 0
    for idx, entry in enumerate(new_data["shareinfo"]):
        if entry["number"] != "":
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Aktienart': entry["type"],
                 'Anzahl': entry["number"].strip(),
                 'Nominalwert': entry["nw"],
                 'Waehrung': entry["currency"],
                 'Bemerkung': entry["info"],
                 'Rang': idx+1,
                 }])
    return 0


def Unternehmentable(conn, new_data, table):
    print(table.columns.keys())
    del_entry(new_data['compare'], [],
              ['Sitz', 'investorRelations', 'publicRelations', 'established_year', 'activity_description', 'name'])
    WP, HV, GJ, SD, investorRelations, publicRelations = "", "", "", "", "", ""
    if 'sonstigeAngaben' in new_data:
        for entry in new_data['sonstigeAngaben']:
            if entry[0].find('irtschaft') != -1:
                WP = entry[1]
            if entry[0].find('ersammlun') != -1:
                HV = entry[1]
            if entry[0].find('jahr') != -1:
                GJ = entry[1]
        try:
            for block in new_data['Sitz']:
                entry_check(block, ['type'])
                if 'origpost' in block:
                    SD += block['type']+": "+block['origpost']+" "
                else:
                    for x, y in block.items():
                        if isinstance(y, list):
                            for yy in y:
                                SD += x + ": " + yy + ", "
                        else:
                            SD += x + ": " + y + ", "
        except:
            pass
    if 'investorRelations' in new_data:
        for entry in new_data['investorRelations']:
            investorRelations += get_infostring(entry)
        investorRelations = investorRelations.strip().strip(string.punctuation)
    if 'publicRelations' in new_data:
        for entry in new_data['publicRelations']:
            publicRelations += get_infostring(entry)
        publicRelations = publicRelations.strip().strip(string.punctuation)
    comment = ""
    if "unternehmensVertraege" in new_data:
        comment = "Unternehmnensverträge: "+" | ".join(new_data["unternehmensVertraege"])
    entry_check(new_data, ['established_year', 'activity_description'])
    conn.execute(table.insert(), [
        {'unternehmenId': new_data['unternehmenId'],
         'Unternehmen': new_data['name'],
         'Stammdaten': SD,
         'Taetigkeitsgebiet': new_data['activity_description'],
         'Gruendungsjahr': new_data['established_year'],
         'AGseit': "",
         'InvestorRelations': investorRelations,
         'PublicRelations': publicRelations,
         'Hauptversammlung': HV,
         'WP': WP,
         'Erscheinungsjahr': new_data['year'],
         'Startseite': "",
         'Bemerkung': comment,
         }])
    return 0


def Volumetable(conn, new_data, table):
    print(table.columns.keys())
    #TODO: For cds not necassary
    return 0
    for idx, entry in enumerate(new_data['all_wkn_entry']):
        conn.execute(table.insert(), [
            {'Erscheinungsjahr': new_data['year'],
             'idGoobi': "",
             'offsetSeitenzahlen': "",
             'idOAI': "",
             'Volume': "",
             }])
    return 0


def Vorstandtable(conn, new_data, table):
    print(table.columns.keys())
    if 'vorstand' not in new_data: return 0
    for idx, entry in enumerate(new_data['vorstand'][0]):
        del_entry(new_data['compare'], ['vorstand', 0, idx], ['lastName', 'firstName', 'title', 'cityAcc', 'funct'])
        entry_check(entry, ['lastName', 'firstName', 'title', 'cityAcc', 'funct'])
        if membercheck(entry): continue
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Name': entry['lastName'],
             'Vorname': entry['firstName'],
             'Titel': entry['title'],
             'Ort': entry['cityAcc'],
             'Funktion': entry['funct'],
             'Bemerkung': "",
             'Rang': idx + 1,
             }])
    return 0


def WKNtable(conn, new_data, table):
    print(table.columns.keys())
    if 'all_wkn_entry' not in new_data: return 0
    if "shareinfo" in new_data:
        for shareinfo in new_data["shareinfo"]:
            if shareinfo['wkn'] + shareinfo['isin'] != "":
                count = 0
                for idx, wkn_entry in enumerate(new_data['all_wkn_entry']):
                    if set(shareinfo.values()) & set(wkn_entry.values()):
                        for entries in ['type', 'wkn', 'isin', 'nw']:
                            if wkn_entry[entries] == "":
                                wkn_entry[entries] = deepcopy(shareinfo[entries])
                        count = 1
                        continue
                if count == 0:
                    new_data['all_wkn_entry'] += {len(new_data['all_wkn_entry']): []}
                    new_data['all_wkn_entry'][len(new_data['all_wkn_entry']) - 1] = dict(
                        zip_longest(new_data['all_wkn_entry'][0].keys(), [""] * len(new_data['all_wkn_entry'][0].keys())))
                    for entry in ['type', 'wkn', 'isin', 'nw']:
                        new_data['all_wkn_entry'][len(new_data['all_wkn_entry']) - 1][entry] = shareinfo[entry]
    del_entry(new_data['compare'], [], ['all_wkn_entry'])
    for idx, entry in enumerate(new_data['all_wkn_entry']):
        entry_check(entry, ['type', 'wkn', 'isin', 'nw'])
        comment = ""
        if entry['isin'] != "":
            comment = "ISIN: " + entry['isin']
        if entry['nw'] != "":
            comment = (comment + " Nennwert: " + entry['nw']).strip()
        if entry['wkn']+entry['isin'] != "":
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Unternehmen': new_data['name'],
                 'WKN': entry['wkn'],
                 'ISIN': entry['isin'],
                 'Bemerkung': comment,
                 'Aktienart': entry['type'],
                 'Rang': idx + 1,
                 }])
    return 0


def WeitereBemerkungentable(conn, new_data, table):
    print(table.columns.keys())
    if "leitung_kommunikation" in new_data:
        for key in new_data["leitung_kommunikation"]:
            comment = ""
            for entries in new_data["leitung_kommunikation"][key]:
                for entry in entries:
                    comment += entry.title()+ ": " + entries[entry]+" "
            comment += " ("+key.title()+")"
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Abschnitt': "Leitung/Kommunikation",
                 'Bemerkung': comment,
                 }])
    if "ermächtigungAktienerwerb" in new_data:
        for key in new_data["ermächtigungAktienerwerb"]:
            comment = " ".join(new_data["ermächtigungAktienerwerb"][key])
            conn.execute(table.insert(), [
                {'unternehmenId': new_data['unternehmenId'],
                 'Abschnitt': "Ermächtigung Aktienerwerb",
                 'Bemerkung': comment,
                 }])
    if "Marktbetreuer" in new_data:
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Abschnitt': "Marktbetreuer",
             'Bemerkung': new_data["Marktbetreuer"],
             }])
    if 'besBezugsrechte' in new_data:
        if new_data['besBezugsrechte']:
            for entry in new_data['besBezugsrechte']['besBezugsrechte']:
                text = ""
                if 'jahr' in entry:
                    text = "Jahr: "+entry['jahr']+" "
                for feat in entry["bemerkungen"]:
                    text += feat+" "
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Abschnitt': "Besondere Bezugsrechte",
                     'Bemerkung': text,
                     }])
    if "unternehmensVertraege" in new_data:
        if isinstance(new_data["unternehmensVertraege"],list): comment = " ".join(new_data["unternehmensVertraege"])
        else: comment = new_data["unternehmensVertraege"]
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Abschnitt': "Unternehmens Verträge",
             'Bemerkung': comment,
             }])
    return 0


######### FUNCTIONS ############
def membercheck(entry):
    """
    This function checks if there is an entry like "4 Mitglieder",
    which appears sometimes in the data.
    """
    if entry['funct'] == "Mitglieder" and (entry["firstName"].isdigit() or entry["lastName"].isdigit()):
        return 1
    return 0


def update_all_wkn(new_data):
    """
    It takes the wkn/isin information out of the table "boersenbwertung"
    and update the data in "all_wkn_entries".
    """
    if not 'boersenbewertung' in new_data: return 0
    shareinfolist = []
    for block in new_data['boersenbewertung']:
        shareinfo = {}
        blockkeys = new_data['boersenbewertung'][block].keys()&['wkns','isins']
        for entries in blockkeys:
            for entry in new_data['boersenbewertung'][block][entries]:
                if 'nummer' in entry.keys():
                    shareinfo[entries[:-1]] = entry['nummer']
                if 'aktienart' in entry.keys():
                    shareinfo['type'] = entry['aktienart']
        if shareinfo:
            shareinfolist.append(deepcopy(shareinfo))
        del shareinfo
    if not shareinfolist: return 0
    if ''.join(new_data['all_wkn_entry'][0].values()) == "":
        for idx, shareinfo in enumerate(shareinfolist):
            new_data['all_wkn_entry'] += {idx+1: []}
            new_data['all_wkn_entry'][idx+1] = deepcopy(new_data['all_wkn_entry'][0])
            for entry in shareinfo:
                new_data['all_wkn_entry'][idx + 1][entry] = shareinfo[entry]
        del new_data['all_wkn_entry'][0]
    else:
        for shareinfo in shareinfolist:
            count = 0
            for idx, wkn_entry in enumerate(new_data['all_wkn_entry']):
                if set(shareinfo.values())&set(wkn_entry.values()):
                    for entries in shareinfo:
                        entry_check(new_data['all_wkn_entry'][idx], ['type', 'wkn', 'isin', 'nw'])
                        if wkn_entry[entries] == "":
                                wkn_entry[entries] = deepcopy(shareinfo[entries])
                    count = 1
                    continue
            if count == 0:
                new_data['all_wkn_entry'] += {len(new_data['all_wkn_entry']): []}
                new_data['all_wkn_entry'][len(new_data['all_wkn_entry'])-1] = dict(zip_longest(new_data['all_wkn_entry'][0].keys(), [""]*len(new_data['all_wkn_entry'][0].keys())))
                for entry in shareinfo:
                    new_data['all_wkn_entry'][len(new_data['all_wkn_entry']) - 1][entry] = shareinfo[entry]
    return 0


def seperate_shareinfo(entry_split,entries,shareinfo):
    """
    This function takes a line of information, splits it
    and search for the amount and the currency in the string.
    """
    # Test if Stückelung wasn't recognized
    shareinfo['currency'] = entry_split
    shareinfo['amount'] = ""
    for idx, entry in enumerate(entries.split(" ")[1:]):
        if entry == "":
            continue
        if entry[0].isdigit():
            shareinfo['amount'] += entry
        elif "Mio" in entry:
            shareinfo['amount'] += " "+entry
        elif "Mrd" in entry:
            shareinfo['amount'] += " "+entry
        else:
            entries = " ".join(entries.split(" ")[idx + 1:])
            break
    if "WKN" in entries:
        shareinfo['wkn'] = entries.split("WKN")[-1].replace(".", "").replace(":", "")
        shareinfo["info"] = entries.split("WKN")[0]
    elif "Kenn-Nr" in entries:
        shareinfo['wkn'] = entries.split("Kenn-Nr")[-1].replace(".", "").replace(":", "")
        shareinfo["info"] = " ".join(entries.split("Kenn-Nr")[0].split(" ")[:-1])
    elif "ISIN" in entries or "Wertpapier-Kenn-Nr" in entries:
        shareinfo['isin'] = entries.split("ISIN")[-1].replace(".", "").replace(":", "")
        shareinfo["info"] = entries.split("ISIN")[0]
    return 0


def get_shareinfo(new_data):
    """
    This function loads all necessary share information in one list.
    The informations will be used in the following functions:
        - grundkapital
        - stimmrecht
        - stueckelung
    """
    shareinfolist = []
    if 'grundkapital' in new_data:
        max_entries = max([len(new_data['stückelung']),len(new_data['stimmrecht'])])
        if max_entries > 1:
            new_data['grundkapital']['bemerkungen'].append([new_data['grundkapital']['betrag'],"Grundkapital"])
        # TODO-Hint: Search here if something smells fishy!
        for idx, entries in enumerate(x for x in new_data['grundkapital']['bemerkungen'] if x and not (len(x) == 1 and x[0]== "")):
            shareinfo = {'wkn': "", 'isin': "", 'nw': "", 'type': "", 'number': "", 'voice': "", 'amount': "",
                         'currency': "", 'info': ""}
            if isinstance(entries, str): entries = [entries]
            if entries[0] == "": del entries[0]
            if not entries: continue
            entries[0] = entries[0].strip().replace("  "," ")
            entry_split = entries[0].split(" ")
            # The query was earlier in "seperate shareinfo"
            for idxx, entry in enumerate(entries):
                for feat in ["Stückelung", "Stück "]:
                    if feat in entry:
                        new_data["stückelung"].append(entry.split(feat)[-1])
                        entries[idxx] = entry.split(feat)[0]
                if " Stimme" in entry:
                    new_data["stimmrecht"].append(entry)
                    entries[idxx] = ""
            if entries[0] == "": continue
            if len(entry_split) <= 1: continue
            if len(entry_split[0]) > 1 and not entry_split[0][1].isupper() \
                and (len(entry_split) < 2 or not entry_split[1][0].isdigit()):
                    if idx == 0:
                        entries[0] = new_data['grundkapital']['betrag']+" "+entries[0]
                        entry_split = entries[0].split(" ")
                    else:
                        if len(shareinfolist) > 1:
                            shareinfolist[len(shareinfolist)-1]['info'] += entries[0]
                        del entries[0]
                        if not entries:
                            continue
                        entry_split = entries[0].split(" ")

            if len(entry_split[0]) > 1 and entry_split[0][1].isupper() and entry_split[1][0].isdigit():
                    seperate_shareinfo(entry_split[0], entries[0], shareinfo)
                    if len(entries) > 1:
                        shareinfo['info'] += " ".join(entries[1:])

            if len(new_data['stückelung'])-1 >= idx: shareinfo['number'] = new_data['stückelung'][idx]
            if len(new_data['stimmrecht'])-1 >= idx: shareinfo['voice'] = new_data['stimmrecht'][idx]
            sharewkn = shareinfo['wkn']+shareinfo['isin'].replace(" ","")
            if sharewkn != "":
                for entry in new_data["all_wkn_entry"]:
                    for entrykey in ["wkn","isin"]:
                        if entry[entrykey] != "":
                            if entry[entrykey] in entries[0].replace(" ",""):
                                for feat in ['wkn','isin','nw','type']:
                                    if entry[feat] != "":
                                        shareinfo[feat] = entry[feat]
            shareinfolist.append(deepcopy(shareinfo))
            del shareinfo
    if "ausgegebenesKapital" in new_data:
        shareinfo = {'wkn': "", 'isin': "", 'nw': "", 'type': "", 'number': "", 'voice': "", 'amount': "",
                     'currency': "", 'info': ""}
        entrydict = {'betrag': "amount", 'notiz': "info", 'stimmrecht': "voice", 'stueckelung': "number"}
        for entries in new_data["ausgegebenesKapital"]["eintraege"]:
            for entry in entries:
                if entry in entrydict:
                    if entry == "betrag":
                        entry_split = entries[entry].split()
                        if len(entry_split[0]) > 1 and entry_split[0][1].isupper() and entry_split[1][0].isdigit():
                            seperate_shareinfo(entry_split[0], entries[entry], shareinfo)
                        else:
                            shareinfo[entrydict[entry]] += entries[entry] + " "
                    else:
                        shareinfo[entrydict[entry]] += entries[entry]+" "
        shareinfolist.append(deepcopy(shareinfo))
        del shareinfo
    new_data["shareinfo"] = shareinfolist
    print(new_data["shareinfo"])
    return 0


def replace_geminfo(yearinfo, notizinfo, notizstr):
    """"
    It searches for gem ("gemäß") information in the year info (like "2000 i)")
    and return it if it find something!
    """
    comment = ""
    if "i" in yearinfo:
        comment = "gemäß IAS"
    elif "g" in yearinfo:
        comment = "gemäß US-GAAP"
    elif string.ascii_letters in yearinfo:
        if notizstr in notizinfo:
            comment = " ".join(notizinfo[notizstr]).replace("_", "")
    elif "**)" in yearinfo:
        if notizstr == "":
            comment = yearinfo.strip().split("*")[0]
        elif notizstr in notizinfo:
            comment = " ".join(notizinfo[notizstr]).replace("_", "").split("**)")[-1]
    elif "*" in yearinfo:
        if notizstr == "":
            comment = yearinfo.strip().split("*")[0]
        elif notizstr in notizinfo:
            comment = " "+" ".join(notizinfo[notizstr]).replace("_", "")
            comment = comment.split("*")[1][1:]
    elif ")" in yearinfo:
        if yearinfo.strip().split(" ")[-1][0] in string.digits:
            if notizstr == "":
                comment = yearinfo.strip().split(" ")[0]
            elif notizstr in notizinfo:
                for info in notizinfo[notizstr]:
                    if yearinfo.strip().split(" ")[-1] in info:
                        comment = info.replace("_", "").replace(yearinfo.strip().split(" ")[-1], "").strip()
    return comment


def del_entry(table, locations, entries):
    """
    Delets entries in the compare table!
    The files contains all data from the jsonfile,
    which didn't get transferred in the db.
    It get stored in a temp dir.
    """
    if table['debug'] == False: return 0
    if table:
        for location in locations:
            if location in table:
                table = table[location]
        for entry in entries:
            if entry in table:
                del table[entry]
    return 0


def get_infostring(table):
    """
    Joins the values of an dictonary to one string,
    joined by comma.
    """
    infostring = ""
    for entries in table:
        for entry in entries.values():
            infostring += entry+", "
    return infostring


def get_share(entry):
    """
    Extracts the share, share_pc (percentage)
    and the currency of the entrystring.
    """
    share, share_pc, currency = "", "", ""
    if entry != "":
        if ":" in entry:
            clean_entry = entry.split(":")[1].strip()
            if "(" in clean_entry:
                share_list = clean_entry.replace(")", "").split("(")
                share_pc = share_list[1]
                currency = share_list[0].split(" ")[0]
                share = " ".join(share_list[0].split(" ")[1:]).strip()
        else:
            if "(" in entry and "%" in entry:
                share_list = entry.replace(")", "").split("(")
                share_pc = share_list[1]
                for feat in share_list:
                    if "%" in feat:
                        share_pc = feat
            elif "%" in entry:
                share_pc = entry
                # Obsolete code
                # share =.replace("Kapital:", "").strip()
                # currency = share.split(" ")[0]
    return share, share_pc, currency


def get_currencyinfo(table):
    """
    Generates a list of cuinfo (Currency & Unit INFO) dictonaries.
    And converts units from words to numbers (e.g. "Mio"->"1000000")
    """
    cuinfo = []
    for item in table:
        if "Beschäft" in item:
            continue
        currency = ""
        unit = "1"
        if "TEUR" in item:
            currency = "EUR"
            unit = "1000"
        elif "TDM" in item:
            currency = "DM"
            unit = "1000"
        elif "%" in item:
            unit = "%"
        elif len(item.split("(")) > 1:
            currency = item.split("(")[-1].split(" ")[-1].replace(")", "").replace(",", "").strip()
            if len(item.split("(")[-1].split(" ")) > 1:
                unit = item.split("(")[-1].split(" ")[-2]
            if "Mio" in item:
                unit = "1000000"
            if "Mrd" in item:
                unit = "1000000000"
        else:
            currency = item
        cuinfo.append({'currency': currency, 'unit': unit,'text': item.split("(")[0]})
    return cuinfo


def get_currency_unit(table):
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
    if 'waehrung' in table.keys():
        if "TEUR" in table['waehrung']:
            currency = "EUR"
            unit = "1000"
        elif "TDM" in table['waehrung']:
            currency = "DM"
            unit = "1000"
        elif len(table['waehrung'].split(" ")) > 2:
            currency = table['waehrung'].split(" ")[-1]
            unit = table['waehrung'].split(" ")[-2]
            if unit == "Mio":
                unit = "1000000"
            if unit == "Mrd":
                unit = "1000000000"
        elif len(table['waehrung'].split(" ")) > 1:
            currency = table['waehrung'].split(" ")[-1]
        elif len(table['waehrung']) > 0:
            currency = table['waehrung']
    return currency, unit


def entry_check(entry, paramlist):
    """
    Checks if every key is in the entry otherwise an empty key will be added
    """
    for i in paramlist:
        if i not in entry:
            entry[i] = ""
        if entry[i] != "":
            if not isinstance(entry[i], int):
                if len(entry[i]) == 0:
                    del entry[i]
                    entry[i] = ""
    return 0


def empty_check(entry, paramlist):
    """
    Checks if every key is empty or
    if its an empty string.
    """
    # TODO: Not yet implemented. Is it necessary?!
    count = 0
    for i in paramlist:
        if i not in entry:
            count += 1
        if entry[i] == "" or entry[i] == " ":
            count += 1
    if count == len(paramlist): return True
    return False


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
    lastid = max(idlist)
    return lastid + 1


def get_files(filedir):
    """
    Get all file names!
    """
    inputfiles = sorted(glob.glob(filedir + "*.json"))
    return inputfiles


def get_uid(new_data, metadata, conn,entryname):
    """
    Get uid (unique ID) for the given WKN.
    """
    wkn = ""
    if new_data['all_wkn_entry'][0][entryname] != "":
        wkn = new_data['all_wkn_entry'][0][entryname]
    elif 'börsenbewertung' in new_data:
        if entryname+"s" in new_data['börsenbewertung']['börsenbewertung1']:
            if "nummer" in new_data['börsenbewertung']['börsenbewertung1'][entryname+"s"]:
                wkn = new_data['börsenbewertung']['börsenbewertung1'][entryname+"s"]["nummer"]
    elif len(new_data['all_wkn_entry']) > 1:
        if new_data['all_wkn_entry'][1][entryname] != "":
            wkn = new_data['all_wkn_entry'][1][entryname]
    else:
        new_data['reference'] = new_data['unternehmenId']
        new_data['id'] = ""
        return 0
    #TODO-HINT: Be aware its order is descendent by year to avoid apply new WKN's to old ones which get used in early years!
    if entryname == "wkn":
        s = select([metadata.tables["WKN"]]).where(metadata.tables["WKN"].c.WKN == wkn).order_by(
            metadata.tables["WKN"].c.unternehmenId.desc())
    else:
        s = select([metadata.tables["WKN"]]).where(metadata.tables["WKN"].c.ISIN == wkn).order_by(
            metadata.tables["WKN"].c.unternehmenId.desc())
    result = conn.execute(s)
    try:
        someid = result.fetchone()[0]
    except:
        new_data['reference'] = new_data['unternehmenId']
        new_data['id'] = ""
        return 0
    s = select([metadata.tables['MainRelation']]).where(
        metadata.tables['MainRelation'].c.referenz == someid)
    result = conn.execute(s)
    fone = result.fetchall()
    if len(fone) > 0:
        for row in fone:
            new_data['reference'] = row[0]
            new_data['id'] = row[4]
            return 0

    s = select([metadata.tables['MainRelation']]).where(
        metadata.tables['MainRelation'].c.weiteresAuftreten == someid)
    result = conn.execute(s)
    fone = result.fetchall()
    if len(fone) > 0:
        for row in fone:
            new_data['reference'] = row[0]
            new_data['id'] = row[4]
            return 0

    else:
        new_data['reference'] = new_data['unternehmenId']
        new_data['id'] = ""
        return 0


def akf_db_updater(file,dbPath):
    """
    Main function of the AKF_SQL_DBTalk!
    """
    file = file.replace("\\", "/")
    print("Start SQLTalk")
    print(file)
    with open(file, 'r', encoding="utf-8") as f:
        new_data = json.load(f, cls=NoneRemover)

    # Generate a compare object
    new_data['debug'] = False
    if new_data['debug']:
        new_data['compare'] = deepcopy(new_data)
        del_entry(new_data['compare'], [], ['_fulltext', 'debug'])
    else:
        new_data['compare'] = {"debug": False}

    # Generate unternehmenId
    new_data.update({'unternehmenId': file.split("/")[-2].replace("-",".") + "." + file.split("/")[-1][:4]})

    # Generate Year
    new_data.update({'year': file.split("/")[-2]})

    db_akf = dbPath
    engine = create_engine(db_akf)
    conn = engine.connect()

    # Create a MetaData instance
    metadata = MetaData(engine, reflect=True)

    # Check if entry already exists
    #s = select([metadata.tables['Unternehmen']]).where(
    #    metadata.tables['Unternehmen'].c.unternehmenId == new_data['unternehmenId'])
    #result = conn.execute(s)
    #if len(result.fetchall()) > 0: print("Entry already exists!");conn.close(); return 0;

    # Check if a universal ID already exists
    for entryname in ("wkn","isin"):
        get_uid(new_data, metadata, conn,entryname)
        if new_data['id'] != "":break

    # Update all_wkn_entry
    update_all_wkn(new_data)

    # Get shareinfo for later use
    get_shareinfo(new_data)

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
    for name in metadata.tables:
        if name in ['Dependence','Volume']: continue;
        print(name)
        options[name](conn, new_data, metadata.tables[name])
    conn.close()
    engine.dispose()
    if new_data['debug']:
        TEMP = tempfile.gettempdir()
        create_dir(TEMP + "/SQLDBTalk/")
        with open(TEMP + "/SQLDBTalk/" + os.path.basename(file), 'w', encoding="utf-8") as f:
            json.dump(new_data['compare'], f, indent=4)
            print("Wrote File: \n" + os.path.normcase(TEMP + "/SQLDBTalk/" + os.path.basename(file)))
    print("FINISHED!")
    return 0


def main(config):
    # The filespath are stored in the config.ini file.
    # For later use to iterate over all dir
    if config['DEFAULT']['SingleOn'] == "True":
        folders = [config['DEFAULT']['SinglePath']]
    else:
        my_path = config['DEFAULT']['AllPath'] + "/"
        # folders = glob.glob(my_path) # old way of obtaining all folders

        # define the path (with pathlib so absolute paths also work in unix)
        folders = sorted(glob.glob(my_path))
    dbPath = config['DEFAULT']['DBPath']
    #if int(config['DEFAULT']['SingleOn']) == 1:
    #    folders = [config['DEFAULT']['SinglePath']]
    t0all = time.time()
    for folder in folders:
        """"" Read files """""
        files = get_files(folder)
        """"" Start Main """""
        for file in files:
            akf_db_updater(file, dbPath)
        print("The whole folder was finished in {}s".format(round(time.time() - t0all, 3)))
