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
import re
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
        idx = 1
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
            for entry in blockkey:
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
                     'Rang': idx,
                     }])
                idx+=1
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
                    if entry.get("ort","") != "" and not entry["ort"].strip()[0].isupper():
                        comment = " Info: " + entry['ort'] + " " + comment
                        entry['ort'] = ""
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
    indx=1
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
                             'Rang': indx,
                             }])
                        indx+=1
                break
            if "u =" in block.lower():
                uinfo = block
    return 0


def BilanzPassivatable(conn, new_data, table):
    print(table.columns.keys())
    if 'ausBilanzen' not in new_data: return 0
    uinfo = "The amount was considered to low to name it. "
    indx=1
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
                             'Rang': indx,
                             }])
                        indx+=1
                break
            if "u =" in block.lower():
                uinfo = block
    return 0


def BilanzSummetable(conn, new_data, table):
    print(table.columns.keys())
    if 'ausBilanzen' not in new_data: return 0
    uinfo = "The amount was considered to low to name it. "
    indx = 1
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
                             'Rang': indx,
                             }])
                        indx+=1
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
        GJA = "".join([char for char in GJ.split("-")[0] if not char.isalpha()])
        GJE = "".join([char for char in GJ.split("-")[1] if not char.isalpha()])
        GJ = ""
    if "Kalenderjahr" in GJ or "Kalenderjahr" in GJA:
        KJ = "1"
        GJ = "Kalenderjahr"
        #GJA = GJA.replace("Kalenderjahr", "")
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
        if entry["amount"] != "" and entry["amount"].strip()[0].isalpha():
            entry["info"] = entry["amount"]+" "+entry["info"]
            entry["amount"] = ""
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
    idx = 1
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
            for entry in blockkey:
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
                         'Rang': idx,
                         }])
                    idx+=1
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
                amount = "".join([char for char in entry['betrag'] if char.isdigit() or char in [".-"]])
                #for feat in currency.split():
                #    if feat.strip() in ["Mio","Mrd","Tsd","Brd"]:
                #        currency = currency.replace(feat, '')
                #    else:
                #       amount = amount.replace(feat, '')
            #TODO: Experimental!!
            if currency+amount == "":
                scomment = entry["bemerkung"].split(" ")
                if len(scomment)> 2:
                    if scomment[1][0].isdigit():
                        currency = scomment[0]
                        amount = scomment[1]
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
    idx=1
    for entries in new_data['kapitalEntwicklung']:
        if not 'eintraege' in entries: continue
        for entry in entries['eintraege']:
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
                     'Rang': idx,
                     }])
                idx+=1
    if 'entwicklungDesGenusKapitals' not in new_data: return 0
    for entry in new_data['entwicklungDesGenusKapitals']:
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
                 'Rang': idx,
                 }])
            idx+=1
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
    return
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
    return
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
    idx = 1
    for si in new_data["shareinfo"]:
        if si["amount"] != "":
           info = f"Gesamtbetrag: {si['amount']},"+si["info"]
        else:
            info = si["info"]
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Aktienart': si["type"],
             'Stueck': si["stuck"],
             'Stimmzahl': si["voice"],
             'Nennwert': si["nw"],
             'Waehrung': si["currency"],
             'Bemerkung': info,
             'Rang': idx,
             }])
        idx += 1
    return
    """
        if entry["voice"] != "":
            entry["voice"] = entry["voice"].replace("Je","je").replace("jede","je").split(":")[-1]
            if len(entry["voice"].split("je")) > 1:
                for part in entry["voice"].split("je"):
                    if part.strip() == "": continue
                    subparts = part.split("=")
                    type = entry["type"]
                    if "nom." in subparts[0]:
                        type = "nom."
                    else:
                        type = get_type(subparts[0])
                    amountreg = re.compile(r'([\d\s\\/]*)(\D*)([\d,\-]*)')
                    #print(subparts[0]))
                    finding = amountreg.findall(subparts[0].strip())[0]
                    stuck = finding[0]
                    if stuck == "":
                        stuck = "1"
                    currency, nw = "",""
                    if finding[2] != "":
                        currency = finding[1].strip().split(" ")[-1]
                    nw = finding[2]
                    voicereg = re.compile(r'(\D*)([\d\\/]*)(\D*)')
                    if len(subparts) > 1:
                        finding = voicereg.findall(subparts[1])
                        if finding:
                            finding = finding[0]
                            voice = finding[1]
                            if voice == "":
                                voice = "1"
                        else:
                            voice = "SEE THAT"
                    else:
                        voice = "SEE THIS"
                    #print(part)
                    conn.execute(table.insert(), [
                        {'unternehmenId': new_data['unternehmenId'],
                         'Aktienart': type,
                         'Stueck': stuck,
                         'Stimmzahl': voice,
                         'Nennwert': nw,
                         'Waehrung': currency,
                         'Bemerkung': entry["info"],
                         'Rang': idx,
                         }])
                    idx +=1
    return 0
    """
def get_type(content):
    typereg = re.compile(r"\s?([\w\-]*aktie[n]?)\s")
    stckreg = re.compile(r"\s?(\w*tück\w*)\s")
    type = ""
    if typereg.search(content):
        type = typereg.findall(content)[0]
    elif stckreg.search(content):
        type = "Stückaktien"
    return type

def exract_stuecklung(content, VERBOSE = False):
    if VERBOSE:
        print(f"Input: {content}\n")
    results = []
    content = content.replace("Nennwert von", "zu").replace("(rechnerischer Wert","zu").replace("jeweils zu", "zu").replace("zu je","zu").replace(" je "," zu ")
    #nw = True
    #for wn in ["o.N.","o.N","nennwertlose", "nennwertlos", "ohne Nennwert"]:
    #    content = content.replace(wn, "")
        #nw = False
    re_delpunct = re.compile(r"(\d)(\.)(\d)")
    if re_delpunct.search(content):
        for finding in re_delpunct.finditer(content):
            content = content.replace(finding[2], " ")
    re_date = re.compile(r'(\d\d/\d\d)')
    if re_date.search(content):
        for finding in re_date.finditer(content):
            content = content.replace(finding[0],"")
    if "zu" in content:
        re_nmbgrps = re.compile(r'(\d[\d\s]*)(\D*)(zu){1,}(\D*)(\d{1,},{1,}[\d-]{1,})')
        for finding in re_nmbgrps.finditer(content):
            type = get_type(finding[0])
            nw = True
            wnreplace = ""
            number = finding[1]
            amount = finding[5].replace(",-", "")
            unitreg = re.compile(r'(Mio|Mrd|Brd)')
            if unitreg.search(content):
                number += f" {unitreg.search(content)[0]}"
            for wn in ["o.N.", "o.N", "nennwertlose", "nennwertlos", "ohne Nennwert"]:
                wnreplace = finding[2].replace(wn, "")
                nw = False
                break
            content = content.replace(finding[0],"")
            results.append({"Anzahl":    number,
                            "Aktienart": type,
                            "Waehrung":  finding[4].strip(),
                            "Betrag":    amount,
                            "nw":        nw
                           })
            if VERBOSE:
                print("Anzahl:   "+finding[1])
                print("Aktie:    "+finding[2].replace(wnreplace,"").replace("zum rechn. ","").replace("und","").strip(";, "))
                print("zu:       "+finding[3])
                print("Waehrung: "+finding[4].strip())
                print("Betrag:   "+finding[5].replace(",-","")+"\n")
    if content != "" and "ISIN" not in content:
        re_nmbgrps = re.compile(r'(\d[\d\s]*)(\D*)')
        for finding in re_nmbgrps.finditer(content):
            nw = True
            wnreplace = ""
            for wn in ["o.N.", "o.N", "nennwertlose", "nennwertlos", "ohne Nennwert"]:
                wnreplace = wn
                nw = False
                break
            content = content.replace(finding[0],"")
            type = get_type(finding[0])
            results.append({"Anzahl": finding[1],
                           "Aktienart": type,
                           "nw": nw
                           })
            if VERBOSE:
                print("Stueck:   "+finding[1])
                print("Aktie:    "+finding[2].replace(wnreplace,"").replace("zum rechn. ","").replace("und","").strip(";, ")+"\n")
    if VERBOSE:
        print(f"Rest: {content}\n\n")
    return results

def Stueckelungtable(conn, new_data, table):
    print(table.columns.keys())
    if "shareinfo" not in new_data: return 0
    idx = 1
    for si in new_data["shareinfo"]:
        if si["amount"] != "":
           info = f"Gesamtbetrag: {si['amount']},"+si["info"]
        else:
            info = si["info"]
        conn.execute(table.insert(), [
            {'unternehmenId': new_data['unternehmenId'],
             'Aktienart': si["type"],
             'Anzahl': si["number"],
             'Nominalwert': si["nomval"],
             'Waehrung': si["currency"],
             'Bemerkung': info,
             'Rang': idx,
             }])
        idx += 1
    return
    """
        if entry["number"] != "":
            results = exract_stuecklung(entry["number"])
            for result in results:
                entry["number"] = result.get("Anzahl","")
                #entry["type"] = result.get("Aktienart","")
                entry["currency"] = result.get("Waehrung", "")
                entry["nw"] = result.get("Betrag", "")
                conn.execute(table.insert(), [
                    {'unternehmenId': new_data['unternehmenId'],
                     'Aktienart': entry["type"],
                     'Anzahl': entry["number"].strip(),
                     'Nominalwert': entry["nw"].replace(entry["currency"],"").replace("(rechnerisch)","").strip(),
                     'Waehrung': entry["currency"],
                     'Bemerkung': entry["info"],
                     'Rang': idx,
                     }])
                idx+=1
    return 0
    """

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
         'Stammdaten': SD.replace("Sitz: ",""),
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
    get_kennnummer(entries,shareinfo)
    return 0

def get_kennnummer(entries,shareinfo):
    re_idnr = re.compile(r"(WKN|Kenn-Nr|ISIN)\S*\s(\S*)")
    for finding in re_idnr.finditer(entries):
        if finding[1] == "ISIN":
            shareinfo['isin'] = finding[2]
            shareinfo["info"] = entries.replace(finding[1],"").replace(finding[2],"")
        else:
            shareinfo['wkn'] = finding[2]
            shareinfo["info"] = entries.replace(finding[1],"").replace(finding[2],"")
    """
    if "WKN" in entries:
        shareinfo['wkn'] = entries.split("WKN")[-1].replace(".", "").replace(":", "")
        shareinfo["info"] = entries.split("WKN")[0]
    elif "Kenn-Nr" in entries:
        shareinfo['wkn'] = entries.split("Kenn-Nr")[-1].replace(".", "").replace(":", "")
        shareinfo["info"] = " ".join(entries.split("Kenn-Nr")[0].split(" ")[:-1])
    elif "ISIN" in entries or "Wertpapier-Kenn-Nr" in entries:
        shareinfo['isin'] = entries.split("ISIN")[-1].replace(".", "").replace(":", "")
        shareinfo["info"] = entries.split("ISIN")[0]
    """
    return



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
        # TODO: REWORK REWORK REWORK!!! AND STIMMRECHT-STÜCKELUNG!
        for idx, entries in enumerate(x for x in new_data['grundkapital']['bemerkungen'] if x and not (len(x) == 1 and x[0]== "")):
            shareinfo = {'wkn': "", 'isin': "", 'nw': "", 'type': "", 'number': "", 'voice': "", 'amount': "",
                         'currency': "", 'info': ""}
            stype = ""
            if isinstance(entries, str): entries = [entries]
            if entries[0] == "": del entries[0]
            if not entries: continue
            entries[0] = entries[0].strip().replace("  "," ")
            entry_split = entries[0].split(" ")
            # The query was earlier in "seperate shareinfo"
            for idxx, entry in enumerate(entries):
                entry_splitted = entry.split(" ")
                for feat in ["Stückelung", "Stück "]:
                    if feat in entry:
                        new_data["stückelung"].append(entry.split(feat)[-1])
                        entries[idxx] = entry.split(feat)[0]
                if " Stimme" in entry:
                    new_data["stimmrecht"].append(entry)
                    entries[idxx] = ""
                kennnummer = [wkn["wkn"] for wkn in new_data["all_wkn_entry"]]+[wkn["isin"] for wkn in new_data["all_wkn_entry"]]
                aktienarten = [wkn["type"] for wkn in new_data["all_wkn_entry"]] + [wkn["type"] for wkn in
                                                                                  new_data["all_wkn_entry"]]
                for idxes, es in enumerate(entry_splitted):
                    if "Kenn-Nr" in es:
                        if idxes+1 < len(entry_splitted):
                            if entry_splitted[idxes+1] not in kennnummer:
                                stype = ""
                                for idxcon, content in enumerate(entry_splitted):
                                    if "ktien" in content:
                                        stype = content[0]
                                        if "vinku" in entry_splitted[idxcon-1]:
                                            stype = "vinkuliert "+stype
                                        break
                                new_data["all_wkn_entry"].append({"type": stype,
                                                                  "isin": "",
                                                                  "wkn": entry_splitted[idxes + 1],
                                                                  "nw": ""})
                                break
                            else:
                                stype = aktienarten[kennnummer.index(entry_splitted[idxes+1] )]
                                #new_data["all_wkn_entry"][len(new_data["all_wkn_entry"])] = {"type":stype[0],
                                #                                                             "isin":"",
                                #                                                             "wkn":entry_splitted[idxes+1],
                                #                                                             "nw":""}
                    if "ISIN" in es:
                        if idxes + 1 < len(entry_splitted):
                            if entry_splitted[idxes + 1] not in kennnummer:
                                stype = ""
                                for idxcon, content in enumerate(entry_splitted):
                                    if "ktien" in content:
                                        stype = content[0]
                                        if "vinku" in entry_splitted[idxcon - 1]:
                                            stype = "vinkuliert " + stype
                                        break
                                new_data["all_wkn_entry"].append({"type": stype,
                                                                  "isin": entry_splitted[idxes + 1],
                                                                  "wkn": "",
                                                                  "nw": ""})
                                break
                            else:
                                stype = aktienarten[kennnummer.index(entry_splitted[idxes+1] )]

            #if entries[0] == "": continue
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
            re_amount = re.compile(r'(\D*\S\D)\s(\d[\d\s.,]*(Mio\.?|Mrd\.?|Brd\.?)?)')
            finding = re_amount.search(entries[0])
            if finding:
                shareinfo["currency"] = finding[1]
                shareinfo["amount"] = finding[2]
                shareinfo['info'] += " ".join(entries[1:])
            get_kennnummer(entries[-1], shareinfo)
            """
            if len(entry_split[0]) > 1 and entry_split[0][1].isupper() and entry_split[1][0].isdigit():
                
                    seperate_shareinfo(entry_split[0], entries[0], shareinfo)
                    if len(entries) > 1:
                        shareinfo['info'] += " ".join(entries[1:])
            """
            if len(new_data['stückelung'])-1 >= idx: shareinfo['number'] = new_data['stückelung'][idx]
            if len(new_data['stimmrecht'])-1 >= idx: shareinfo['voice'] = new_data['stimmrecht'][idx]
            if shareinfo["type"] == "" and stype != "":
                shareinfo["type"] = stype
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
                        re_amount = re.compile(r'(\D*\S\D)\s(\d[\d\s.,]*(Mio\.?|Mrd\.?|Brd\.?)?)')
                        finding = re_amount.search(entries[entry])
                        if finding:
                            shareinfo["currency"] = finding[1]
                            shareinfo["amount"] = finding[2]
                            shareinfo["info"] = entries[entry]
                            #seperate_shareinfo(entry_split[0], entries[entry], shareinfo)
                        else:
                            shareinfo[entrydict[entry]] += entries[entry] + " "
                    else:
                        shareinfo[entrydict[entry]] += entries[entry]+" "
                    get_kennnummer(entries[entry], shareinfo)
        shareinfolist.append(deepcopy(shareinfo))
        del shareinfo
    new_data["shareinfo"] = shareinfolist
    print(new_data["shareinfo"])
    return 0

def stck_stimmrecht(data):
    #data="\f\n\n\n\n\n\n\n\n Basler Aktiengesellschaft \n\nWertpapier Kenn.-Nr.: 510200 (Inhaber-Stammaktien) \n\n Sitz \n\nAn der Strusbek 30, 22926 Ahrensburg Telefon: (0 41 02) 4 63-0 Telefax: (0 41 02) 4 63-108 \n\n\n\n Tätigkeitsgebiet/Gründung \n\nEntwicklung, Herstellung und Vertrieb von Produkten der Meß-, Automatisierungs- und Rechnertechnik. \n\n \n\nGründung: 1988 \n\n Management \n\nAufsichtsrat:  Dipl.-Ing. Prof. Dr.-Ing. Walter Kunerth, Zeitlarn, Vors.; Dipl.-Kfm. Hans Henning Offen, Großhansdorf, stellv. Vors.; Dipl.-Betriebsw. Bernd Priske, Willich-Neersen \n\nVorstand:  Dipl.-Ing. Norbert Basler, Großhansdorf, Vors.; Bryan Hayes, Hamburg; Dr. Dietmar Ley, Ahrensburg; Dipl.-Wirtschafts-Ing. Thorsten Schmidtke, Hamburg \n\n\n\n Aktionäre \n\nDipl.-Ing. Norbert Basler, Großhansdorf (45,7%); IKB Beteiligungsgesellschaft mbH, Düseldorf (21,4%); Nicola-Irina Basler, Großhansdorf (4,3%); Dr. Dietmar Ley, Ahrensburg (4,3%); Bryan Hayes, Hamburg (4,3%); Streubesitz \n\n\n\n\n\n Wesentliche Beteiligungen \n\n\n\n  Basler Inc., Exton (USA), Kapital: US$ 0,03 Mio (100%)\n\n\n\n\n\n Kapitalentwicklung seit 1980 \n\n\n\n  1988GründungskapitalDM 0,05 Mio\n\n  der GmbH\n\n  1993ErhöhungDM 0,1 Mio\n\n  Gem. GV vom 3.6.\n\n  1994ErhöhungDM 0,25 Mio\n\n  Im April\n\n  1997ErhöhungDM 0,4 Mio\n\n  Im August\n\n  1998Kapital bei Umwandlung der GesellschaftDM 0,4 Mio\n\n  in AG gem. GV vom 13.10.\n\n  1999Umstellung auf EUREUR 204 516,75\n\n  Gem. HV vom 24.2\n\n  KapitalberichtigungEUR 3 Mio\n\n  Gem. HV vom 24.2. (ohne Ausgabe von Aktien).\n\n  BareinlageEUR 3,5 Mio\n\n  Gem. ao. HV vom 3.3.\n\n\n\n\n\n\n\n  Derzeitiges GrundkapitalEUR 3,5 Mio\n\n\n\nInhaber-Stammaktien, WP-Kenn-Nr. 510200 Stückelung: 3 500 000 Stückaktien Stimmrecht: je Stückaktie = 1 Stimme \n\n\n\n\n\n  Genehmigtes KapitalEUR 1,75 Mio\n\n\n\nGem. ao. HV vom 3.3.1999, befristet bis 3.3.2004 (ggf. unter Ausschluß des ges. Bezugsrechts). \n\n\n\n\n\n  Bedingtes KapitalEUR 0,3 Mio\n\n\n\nGem. ao. HV vom 3.3.1999 für Manager und Mitarbeiter der Gruppe (300 000 Stücke) \n\n\n\n Börsenbewertung \n\nWertpapier-Kenn-Nr.: 510200, Inhaber-Stammaktien \n\nNotiert: Neuer Markt in Frankfurt \n\nMarktbetreuer: Dresdner Bank AG; BHF-Bank AG \n\nNotiert seit 23.3.1999 Stückaktien o.N.; Emissionspreis: EUR 57,- \n\n\n\nBereinigte Kurse (Frankfurt in EUR) \n\n\n\n  \n\n  \n\n  \n\n  \n\n  \n\n  1999\n\n  \n\n  \n\n  \n\n  \n\n  \n\n  \n\n  bis 31.3.\n\n  \n\n  Höchst\n\n  \n\n  \n\n  \n\n  \n\n  95,00\n\n  \n\n  Tiefst\n\n  \n\n  \n\n  \n\n  \n\n  78,00\n\n  \n\n  Ultimo\n\n  \n\n  \n\n  \n\n  \n\n  82,00\n\n  \n\n\n\n\n\n Dividenden (in DM; Auschüttungen sind erst für das Jahr 2000 geplant) \n\n\n\n  \n\n  \n\n  \n\n  \n\n  1998\n\n  1999\n\n  \n\n  Dividende\n\n  \n\n  \n\n  \n\n  0\n\n  0\n\n  \n\n\n\n\n\n \n\n Sonstige Angaben \n\nWirtschaftsprüfer : ARTHUR ANDERSEN Wirtschaftsprüfungsgesellschaft Steuerberatungsgesellschaft mbH, Hamburg \n\nHauptversammlung: 3.3.1999 (ao. HV) \n\nGeschäftsjahr: Kalenderjahr \n\n\n\n"
    #data= "Inhaber-Stammaktien, WP-Kenn-Nr. 553700 voll an der Börse zugelassen und eingeführt Stückelung: 77 000 Stücke zu je DM 50; 102 000 zu DM 100; 25 000 zu DM 300; 100 450 zu DM 1 000 Stimmrecht: Das Stimmrecht jeder Aktie entspricht ihrem Nennbetrag."
    #data = "\n\n\n\n\n\n\n\n\n \n\n Didier-Werke Aktiengesellschaft \n\nWertpapier Kenn.-Nr.: 553700 (Inhaber-Stammaktien) \n\n Sitz \n\nAbraham-Lincoln-Straße 1, 65189 Wiesbaden Postfach 20 25, 65010 Wiesbaden Telefon: (06 11) 73 35-0 Telefax: (06 11) 73 35-4 75 \n\n\n\n Tätigkeitsgebiet/Gründung \n\nFeuerfest-Produkte: Fertigung, Vertrieb, Montage von hochtemperaturfester Spezialkeramik. Anlagentechnik: Konstruktion, Fertigung, Vertrieb, Montage von Spezialaggregaten der Energie-, der Hochtemperatur-, der Korrosionsschutz- und Umweltschutztechnik. \n\n \n\nGründung: 1834 \n\n Management \n\nAufsichtsrat:  Dr. Walter Ressler, Villach (Österreich), Vors.; Roland Platzer, Wien (Österreich), stellv. Vors.; Hubert Jacobs, Wiesbaden *); Dipl.-Ing. Dr. Günther Mörtl, Wien (Österreich); Jürgen Waligora, Duisburg *); Dr. Wilhelm Winterstein, München \n\n*) Arbeitnehmervertreter \n\nVorstand:  Dipl.-Kfm. Robert Binder, Wiesbaden; Dipl.-Ing. Ingo Gruber, St. Veit/Glan (Österreich); Dr. Andreas Meier, Niklasdorf (Österreich); Dipl.-Ing. Uwe Schatz, Urmitz/Rhein; Walther von Wietzlow (Gesamtkoordination, Finanzen, Verkauf/Marketing, Personal, Recht, Organisation) \n\nOrganbezüge : 1997: Vorstand DM 1,755 Mio; Aufsichtsrat DM 0,038 Mio \n\n Investor Relations \n\nMag. Peter Hofmann, Tel.: 0043-1-587767123, Fax: 0043-1-5873380 \n\n\n\n Aktionäre \n\nRHI AG, Wien (Österreich) (90,1%); Rest Streubesitz \n\n\n\n\n\n Wesentliche Beteiligungen \n\nI. Verbundene Unternehmen, die in den Konzernabschluß einbezogen sind (Inland) \n\n\n\n  Dinova GmbH, Königswinter, Kapital: DM 6 Mio (100%)\n\n  Didier-M&P Energietechnik GmbH, Wiesbaden, Kapital: DM 4,5 Mio (66,67%)\n\n  Teublitzer Ton GmbH, Teublitz, Kapital: DM 5 Mio (51%)\n\n\n\n\n\n  Rohstoffgesellschaft mbH Ponholz, Maxhütte-Haidhof, Kapital: DM 2 Mio (100%)\n\n\n\n\n\n \n\nII. Verbundene Unternehmen, die in den Konzernabschluß einbezogen sind (Ausland) \n\n\n\n  North American Refractories Co. (NARCO), Cleveland/Ohio (USA), Kapital: US-$ 20,158 Mio (85,12%)\n\n\n\n\n\n  Zircoa Inc., Solon/Ohio (USA), Kapital: US-$ 1 Mio (100%)\n\n  TRI-STAR Refractories Inc., Cincinnati/Ohio (USA), Kapital: US-$ 2,955 Mio (80%)\n\n\n\n\n\n  InterTec Company, Cincinnati/Ohio (USA), Kapital: US-$ 0,998 Mio (100%)\n\n\n\n\n\n  Didier, Corporation de Produits Réfractaires, Burlington (Kanada), Kapital: can$ 17 Mio (100%)\n\n\n\n\n\n  Narco Canada Inc., Burlington/Ontario (Kanada), Kapital: can$ 3,705 Mio (100%)\n\n\n\n\n\n  D.S.I.P.C. - Didier Société Industrielle de Production et de Constructions, Breuillet (Frankreich), Kapital: FF 33,713 Mio (99,88%)\n\n  Thor Ceramics Ltd., Clydebank (Großbritannien), Kapital: £ 1,375 Mio (100%)\n\n  Didier Refractarios S.A., Lugones (Spanien), Kapital: Ptas 200 Mio (100%)\n\n  REFEL S.p.A., San Vito al Tagliamento (Italien), Kapital: Lit 9 851 Mio (100%)\n\n  Didier Asia Sdn. Bhd., Petaling Jaya (Malaysia), Kapital: M-$ 7,5 Mio (60%)\n\n  Veitsch-Radex-Didier S.E.A. PTE. LTD., Singapur (Singapur), Kapital: S$ 0,3 Mio (100%)\n\n  Veitsch-Radex-Didier Andino C.A., Ciudad Guayana (Venezuela), Kapital: VEB 10 Mio (99,6%)\n\n  Veitsch-Radex-Didier México, S.A. de C.V., Garza Garcia (Mexiko), Kapital: mex$ 0,05 Mio (100%)\n\n  Veitsch-Radex-Didier Australia Pty. Ltd., Newcastle (Australien), Kapital: A$ 1,4 Mio (100%)\n\n\n\n\n\n \n\nIII. Verbundene Unternehmen, die nicht in den Konzernabschluß einbezogen sind (Inland) \n\n\n\n  Rheinischer Vulkan Chamotte- und Dinaswerke mbH, Königswinter, Kapital: DM 2 Mio (100%)\n\n  W. Strikfeld & Koch GmbH, Wiehl, Kapital: DM 1 Mio (100% über Striko-Westofen GmbH)\n\n\n\n\n\n \n\nIV. Verbundene Unternehmen, die nicht in den Konzernabschluß einbezogen sind (Ausland) \n\n\n\n  Shanghai Dinova Ltd., Shanghai (China), Kapital: DM 15 Mio (60% über Dinova GmbH)\n\n  Beijing Yanke Dinova Building Materials Co., Ltd., Peking (China), Kapital: DM 2,3 Mio (60% über Dinova GmbH)\n\n\n\n\n\n \n\nV. Beteiligungen an assoziierten Unternehmen \n\n\n\n  EKW-Eisenberger Klebsandwerke GmbH, Eisenberg/Pfalz, Kapital: DM 6 Mio (31,5%)\n\n\n\n\n\n  Société Francaise des Pises Siliceux S.A.R.L., Paris (Frankreich), Kapital: FF 1 Mio (97,5%)\n\n\n\n\n\n Kapitalentwicklung seit 1980 \n\n\n\n  1982BezugsrechtDM 92,4 Mio\n\n  Im Dezember aus gen. Kap. (HV 16.7.1981), i.V. 8:1 zu 130 %; rechn. Abschlag DM 6,- am 6.12.; div.-ber. ab 1.1.1983; Div.Sch.Nr. 42.\n\n  1989BezugsrechtDM 122 Mio\n\n  Im August aus gen. Kap. (HV 12.7.); für Aktionäre und Inhaber der Wandelanleihe von 1969 i.V. 7:2 zu 330 %; rechn. Abschlag DM 24,22 am 10.8.; div.-ber. ab 1.7.1989; Tal. bzw. Legitimationsschein A (für Inhaber der Wandelanleihe).\n\n\n\n\n\n\n\n  Besondere Bezugsrechte\n\n\n\n\n\n  1985Bezugsrecht auf Optionsanleihe i.V. 5:2 zu 100 %,  1. Bezugsrechtsnotiz DM 1,90 am 17.9., Div.Sch.Nr. 46\n\n\n\n\n\n\n\n  Derzeitiges GrundkapitalDM 122 Mio\n\n\n\nInhaber-Stammaktien, WP-Kenn-Nr. 553700 voll an der Börse zugelassen und eingeführt Stückelung: 77 000 Stücke zu je DM 50; 102 000 zu DM 100; 25 000 zu DM 300; 100 450 zu DM 1 000 Stimmrecht: Das Stimmrecht jeder Aktie entspricht ihrem Nennbetrag. \n\n\n\n\n\n  Genehmigtes KapitalDM 40 Mio\n\n\n\nGem. HV vom 18.7.1994, befristet bis 30.6.1999 \n\n\n\n Börsenbewertung \n\nWertpapier-Kenn-Nr.: 553700, Inhaber-Stammaktien \n\nNotiert: amtlich in Berlin, Düsseldorf, Frankfurt am Main und München sowie im Freiverkehr in Hamburg \n\nNotiz seit 1948 Seit 16.3.1973 Stücknotiz zu DM 50,-; seit 9.6.1969 Stücknotiz zu DM 100,-; vorher Prozentnotiz \n\n\n\nBereinigte Kurse (Frankfurt in EUR) \n\n\n\n  \n\n  1995\n\n  1996\n\n  1997\n\n  1998\n\n  1999\n\n  \n\n  \n\n  \n\n  \n\n  \n\n  \n\n  bis 31.3.\n\n  \n\n  Höchst\n\n  72,60\n\n  67,13\n\n  80,78\n\n  82,83\n\n  99,00\n\n  \n\n  Tiefst\n\n  53,69\n\n  50,11\n\n  58,29\n\n  66,47\n\n  70,00\n\n  \n\n  Ultimo\n\n  60,49\n\n  62,38\n\n  74,65\n\n  71,48\n\n  92,50\n\n  \n\n\n\n\n\n Dividenden (in DM pro Aktie) \n\n\n\n  \n\n  1993\n\n  1994\n\n  1995\n\n  1996\n\n  1997\n\n  \n\n  Dividende\n\n  0\n\n  0\n\n  0\n\n  2\n\n  3 1)\n\n  \n\n  Steuerguthaben\n\n  0\n\n  0\n\n  0\n\n  0,86\n\n  0\n\n  \n\n  Div.-Schein-Nr.\n\n  -\n\n  -\n\n  -\n\n  55\n\n  56\n\n  \n\n\n\n_____________________________ \n\n1) Freiwillige Zahlung des Mehrheitsaktionärs an außenstehende Aktionäre \n\nNr. des nächstfälligen Div.-Scheines: 57 \n\n\n\n \n\n Kennzahlen \n\n\n\n  Konzern\n\n  1993\n\n  1994\n\n  1995\n\n  1996\n\n  1997\n\n  \n\n  Investitionen (in TDM)\n\n  51 853,0\n\n  54 265,0\n\n  40 712,0\n\n  34 660,0\n\n  52 530,0\n\n  \n\n  Jahresüberschuß + Abschreibungen (in TDM)\n\n  9 057,0\n\n  45 885,0\n\n  59 420,0\n\n  58 013,0\n\n  52 584,0\n\n  \n\n  Bilanzkurs (%)\n\n  127,0\n\n  118,5\n\n  121,1\n\n  136,0\n\n  142,3\n\n  \n\n  Eigenkapitalquote (%)\n\n  16,8\n\n  16,0\n\n  17,1\n\n  19,9\n\n  20,6\n\n  \n\n\n\nBeschäftigte \n\n\n\n  Durchschnitt\n\n  6 953\n\n  6 516\n\n  5 753\n\n  5 293\n\n  4 681\n\n  \n\n  GJ-Ende\n\n  6 764\n\n  6 511\n\n  5 597\n\n  5 144\n\n  4 685\n\n  \n\n\n\n\n\n Aus den Bilanzen (in Mio DM) \n\n\n\n  \n\n  AG\n\n  Konzern\n\n\n\n  U = Posten unter 0,5 Mio DM  1996  1997  1996  1997\n\n\n\n    Aktiva          \n\n    Anlagevermögen  312  306  266  272  \n\n    ..(Sachanlagenzugang)  13  12  35  53  \n\n    ..(Beteiligungen)  250  247  28  21  \n\n    Umlaufvermögen  323  433  668  730  \n\n    ..(Flüssige Mittel)  27  12  49  28  \n\n    Rechnungsabgrenzung  U  -  8  8  \n\n    Steuerabgrenzung  -  -  4  4  \n\n\n\n\n\n    Passiva          \n\n    Eigenkapital  291  247  198  220  \n\n    ..(Gezeichnetes Kapital)  122  122  122  122  \n\n    ..(Bilanzergebnis)  5  -  5  11  \n\n    Sopo m. Rücklageant.  -  18  1  -  \n\n    Fremdkapital  344  474  744  794  \n\n    ..(Pensionsrückstell.)  148  148  186  191  \n\n    ..(And. Rückstellungen)  79  71  176  151  \n\n    ..(langfr. Verbindlichk.)  51  51  53  52  \n\n    ..(kurz- +mfr. Verbindlk.)  65  205  327  400  \n\n    Rechnungsabgrenzung  -  -  4  1  \n\n    Bilanzsumme  635  739  947  1 014  \n\n\n\n Aus den Gewinn- und Verlustrechnungen (in Mio DM) \n\n\n\n  \n\n  AG\n\n  Konzern\n\n\n\n  U = Posten unter 0,5 Mio DM  1996  1997  1996  1997\n\n\n\n    Umsatz  497  550  1 289  1 480  \n\n    Bestandsveränderung  - 9  1  2  15  \n\n    Akt. Eigenleistung  U  1  1  2  \n\n    sonst. betr. Erträge  33  54  43  72  \n\n    Materialaufwand  238  310  597  768  \n\n    Personalaufwand  145  136  395  432  \n\n    Abschreibungen  17  15  40  41  \n\n    sonst. betr. Aufwand  110  168  253  281  \n\n    Finanzergebnis  - 7  - 15  - 20  - 22  \n\n    Ergebnis d. gewöhnl. Geschäftstätigkeit  5  - 39  31  26  \n\n    Steuern  U  U  13  14  \n\n    ..(EE-Steuern)  - 2  - 1  5  6  \n\n    Jahresergebnis  5  - 39  18  11  \n\n\n\n Sonstige Angaben \n\nWirtschaftsprüfer : C & L Deutsche Revision Aktiengesellschaft Wirtschaftsprüfungsgesellschaft, Frankfurt/M. \n\nHauptversammlung: 15.5.1998 \n\nGeschäftsjahr: Kalenderjahr \n\n\n\n"

    split_by_line = [line for line in data.replace("\n\n","\n").replace("\nStimmrecht","Stimmrecht").split("\n") if line != "" and ("Stückelung:" in line or ("Stückelung" in line and "Stimmrecht" in line))]
    for line in split_by_line:
        print(line)
    reg_groups =re.compile(r"(Stückelung|Stimmrecht)")
    groups = []
    for line in split_by_line:
        group = {}
        datatype = "Aktien"
        line = line.replace("papier-Nr.","Kenn-Nr.").replace("WP-Nr.","Kenn-Nr.").replace(" bzw. ","").replace("(voll eingezahlt)","").replace("- ","-")
        sidx = 0
        max_parts = 1
        for finding in reg_groups.finditer(line):
            #if datatype == "Aktien":
            #    if re.compile(r"(Kenn-Nr\.|ISIN)"):
            #print(finding[0])
            group.update({datatype:line[sidx:finding.regs[0][0]].strip()})
            if datatype != "Aktien" and "stimmrechtslos" not in line[sidx:finding.regs[0][0]] and "Besondere" not in line[sidx:finding.regs[0][0]] and max_parts < len(line[sidx:finding.regs[0][0]].split(";")):
                max_parts = len(line[sidx:finding.regs[0][0]].split(";"))
            #offset = -1
            #if ":" not in finding[0]:
            #    offset = len(finding[0])
            datatype= finding[0][:]
            sidx = finding.regs[0][1]+1
        else:
            group.update({datatype: line[sidx:].strip()})
            if datatype != "Aktien" and "stimmrechtslos" not in line[sidx:]  and "Besondere" not in line[sidx:] and max_parts < len(line[sidx:].split(";")):
                max_parts = len(line[sidx:].split(";"))
        groups.append((max_parts,group))

    shareinfoarr = []
    regs = {"Aktien":re.compile(r"(?P<currency>\D*)(?P<amount>[\d\s,-]*(Mio\s|Mrd\s)?)(?P<type>(vinkulierte\s)?[^0-9,\s]*)(?P<rest>.*)"),
            "Stückelung":re.compile(r"(?P<number>[\d\s]*)(?P<addinfo>[\D]*)(?P<nomval>[\d\s,]*)"),
            "Stimmrecht":re.compile(r"((Je)(?P<stuck>([\d\s]*))\D*(?P<nw>[\d\s,-]*)[^=]*=(?P<voice>[\d\s]*))")}
    dellist = []
    for (max_parts, group)in groups:
        shareinfo = []
        for idx in range(0,max_parts):
            shareinfo.append({'wkn': "", 'isin': "", 'nw': "",'nomval':"", 'type': "", 'number': "", "stuck": "1", 'voice': "stimmrechtlos", 'amount': "",
                     'currency': "", 'info': ""})
        for key,content in group.items():
            if key == "Aktien" and " " not in content[:5]:
                regs["Aktien"] = re.compile(r"(?P<type>(vinkulierte\s|kumulative\s)?[^0-9,\s]*)(?P<rest>.*)")
            if key == "Aktien" and ("%" in content or "davon" in content[:6]):
                shareinfo[0]["info"] += content
                knr = re.compile(r"(Kenn-Nr\.)(?P<wkn>([\s\d]*))|(ISIN\s)(?P<isin>([\S]*)).*").search(content)
                if knr:
                    if knr["wkn"]:
                        shareinfo[0]["wkn"] = knr["wkn"].strip()
                    if knr["isin"]:
                        shareinfo[0]["isin"] = knr["isin"].strip()
                continue
            if key != "Aktien" and "stimmrechtslos" not in content and "Besondere" not in content:
                contentparts = content.split(";")
            else:
                contentparts = [content]
            for idx,part in enumerate(contentparts):
                finding = regs[key].search(part)
                if finding:
                    for grpkey, grpval in finding.groupdict().items():
                        if grpkey == "rest":
                            knr = re.compile(r"(Kenn-Nr\.)(?P<wkn>([\s\d]*))|(ISIN\s)(?P<isin>([\S]*)).*").search(grpval)
                            if knr:
                                if knr["wkn"]:
                                    shareinfo[idx]["wkn"] = knr["wkn"].strip()
                                if knr["isin"]:
                                    shareinfo[idx]["isin"] = knr["isin"].strip()
                        elif grpval and grpkey not in ["addinfo"]:
                            if grpval.strip() != "":
                                if part == "Aktien" and grpkey == "currency" and ("ktien" in grpval or "tück" in grpval or len(grpval) > 5):
                                    shareinfo[idx]["type"] = grpval
                                    shareinfo[idx]["currency"] = ""
                                else:
                                    if grpkey != "currency" or len(grpval) < 8:
                                        shareinfo[idx][grpkey] = grpval.strip(" :,=")
                    if key == "Stückelung" and finding["addinfo"]:
                        stype  = finding["addinfo"].replace("zu", "je").replace("(","").split("je")[0].replace("o.N.", "").replace("ohne Nennwert", "").strip(" ,")
                        if shareinfo[idx]["type"] == "" and ("ktien" in stype or "tück" in stype):
                            shareinfo[idx]["type"] = stype.strip(" :,=")
                        if shareinfo[idx]["currency"] == "":
                            currency = finding["addinfo"].strip().split(" ")[-1].replace("je","").replace(":","").replace("o.N.", "").replace("ohne Nennwert", "")
                            if "ktien" not in currency and "tück" not in currency and "wer" not in currency and len(currency) < 8:
                                shareinfo[idx]["currency"] = currency.strip(" :,=")
                else:
                    shareinfo[idx]["info"] += content
                if key == "Stückelung" and "Stimmrecht" not in group.keys() and shareinfo[idx]["number"]+shareinfo[idx]["nomval"] == "":
                    dellist.append(idx)
        for delitem in sorted(dellist,reverse=True):
            del shareinfo[delitem]
        shareinfoarr+=shareinfo
    return shareinfoarr
    #for shareinfo in shareinfoarr:
    #    print(shareinfo)


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
    inputfiles = sorted(glob.glob(os.path.normcase(filedir+"/")+"*.json"))
    return inputfiles


def get_uid(new_data, metadata, conn):
    """
    Get uid (unique ID) for the given WKN.
    """
    for awe in new_data["all_wkn_entry"]:
        for key in ["wkn","isin"]:
            if awe[key] == "":continue
            if key == "wkn":
                s = select([metadata.tables["WKN"]]).where(metadata.tables["WKN"].c.WKN == awe[key]).order_by(
                    metadata.tables["WKN"].c.unternehmenId.desc())
            else:
                s = select([metadata.tables["WKN"]]).where(metadata.tables["WKN"].c.ISIN == awe[key]).order_by(
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
    new_data['reference'] = new_data['unternehmenId']
    new_data['id'] = ""
    return 0
    #TODO-HINT: Be aware its order is descendent by year to avoid apply new WKN's to old ones which get used in early years!



def akf_db_updater(file,dbPath):
    """
    Main function of the AKF_SQL_DBTalk!
    """
    file = file.replace("\\", "/")
    #Condition
    #if "0704" not in file: return


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

    new_data["shareinfo"] = stck_stimmrecht(new_data["_fulltext"])
    #for shareinfo in new_data["shareinfo"]:
    #    print(shareinfo)

    for si in new_data["shareinfo"]:
        if si["wkn"]+si["isin"] != "":
            for awe in new_data["all_wkn_entry"]:
                if len(awe.keys())<4:
                    for key in ["type","wkn","isin","nw"]:
                        if not awe.get(key,False):
                            awe[key] = ""
                if si["wkn"] == awe["wkn"] and si["wkn"] != "":
                    break
                if si["isin"] == awe["isin"] and si["isin"] != "":
                    break
            else:
                new_data["all_wkn_entry"].append(
                    {"type":si.get("type",""),
                    "wkn":si.get("wkn",""),
                    "isin":si.get("isin",""),
                    "nw":""}
                )
    #return

    # Check if a universal ID already exists
    get_uid(new_data, metadata, conn)

    # Update all_wkn_entry
    #update_all_wkn(new_data)

    # Get shareinfo for later use
    #get_shareinfo(new_data)

    """
    with open("stimmrecht.txt","a") as stfile:
        for entry in new_data["shareinfo"]:
            stfile.write(entry["voice"]+"\n")
    with open("stuckelung.txt","a") as stfile:
        for entry in new_data["shareinfo"]:
            stfile.write(entry["number"]+"\n")
    return
    """
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
    if config['CDS']['SingleOn'] == "True":
        folders = [os.path.normpath(config['DEFAULT']['SinglePath'])]
    else:
        my_path = config['CDS']['AllPath']
        my_path = os.path.normpath(my_path)
        # folders = glob.glob(my_path) # old way of obtaining all folders
        # define the path (with pathlib so absolute paths also work in unix)
        folders = sorted(glob.glob(os.path.normpath(my_path)))
    dbPath = config['DEFAULT']['DBPath']
    t0all = time.time()
    for folder in folders:
        """"" Read files """""
        files = get_files(folder)
        """"" Start Main """""
        for file in files:
            #if "0062" not in file:continue
            akf_db_updater(file, dbPath)
        print("The whole folder was finished in {}s".format(round(time.time() - t0all, 3)))