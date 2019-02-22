from copy import deepcopy
import io
import os
import os.path as path
import shutil
import ntpath
import regex


class OutputAnalysis(object):

    def __init__(self, config):
        self.config_default = config["BOOKS"]
        self.analysis_enabled = False
        self.output_path = None

        if self.config_default['OutputLogging'] is False:
            print("OutputAnalysis: won't do anything OutputAnalysis not enabled in config")
        else:
            print("OutputAnalysis: analysis is enabled")
            self.analysis_enabled = True
            self.output_path = self.config_default['OutputRootPath']
            self.delete_directory_tree(self.output_path)  # clear the output path to prevent rest content there
        # add data holder
        self.data = {}
        self.current_key = None # key for the current file in the data holder

    def change_file(self, data, file):
        if not self.analysis_enabled: return

        # fetch the basic info like path and name from the file
        # ( this should contain the origposts which will be subtracted)
        self.data[file] = deepcopy(data)
        self.current_key = file     # use the full file path as current key

        # create copy of origdata
        for key in self.data[file]:
            if "overall_info" in key:
                continue
            entry = self.data[file][key]
            origpost = entry[0]['origpost']
            origpost_rest = deepcopy(origpost) # create another copy of origpost where the data is substracted from
            self.data[file][key][0]['origpost_rest'] = origpost_rest
            self.data[file][key][0]['origpost_rest_wo_sc'] = self.remove_sc_from_text(origpost_rest)


    def remove_sc_from_text(self,text):
        # rest without special characters
        my_text_wo_sc = regex.sub("[^\w]+", "", text)
        return my_text_wo_sc


    def subtract_entry(self, tag, texts_to_subtract, subtag1= None, subtag2=None ):
        if not self.analysis_enabled: return
        # current_data = self.data[self.current_key]
        # my_subtract = None

        if tag not in self.data[self.current_key].keys():
            print("subtract_entry-> Tag:", tag, "not in ", self.current_key)
            return
        my_text = self.data[self.current_key][tag][0]['origpost_rest']
        my_orig_text_wo_sc = self.remove_sc_from_text(my_text)
        len_bef = len(my_text)

        #todo order by length ? texts to subtract
        # cast necessary texts
        texts_to_sort = []
        for text in texts_to_subtract:
            if not isinstance(text, str):
                text = str(text)

            texts_to_sort.append(text)


        # sort necessary texts
        texts_to_subtract_sorted = sorted(texts_to_sort, key=len, reverse=True)

        # do the actual subtraction
        for text in texts_to_subtract_sorted:

            my_text = my_text.replace(text, "", 1)
            len_post = len(my_text)

            #if len_bef != len_post:
             #   print("asd")

        self.data[self.current_key][tag][0]['origpost_rest'] = my_text

            ## ['origpost_rest'].replace(text, "")

        # rest without special characters
        #my_text_wo_sc = self.remove_sc_from_text(my_text) # old version
        my_text_wo_sc = my_orig_text_wo_sc
        for text in texts_to_subtract_sorted:
            text_wo_sc = self.remove_sc_from_text(text)
            my_text_wo_sc = my_text_wo_sc.replace(text_wo_sc,"",1)


        #self.data[self.current_key][tag][0]['origpost_rest_wo_sc'] = my_text_wo_sc
        self.data[self.current_key][tag][0]['origpost_rest_wo_sc'] = my_text_wo_sc



        # subtract entry('origpost_rest') by tag and subtag

        # mind charset and stuff here

    def output_result_folder(self, base_folder_name):
        if not self.analysis_enabled:
            return

        acc__statements = {}  # overall length results for each file in the folder
        output_folder = path.join(self.output_path, base_folder_name)
        output_folder = output_folder.replace(".db", "") + "/"
        for filekey in self.data:
            data_to_print = self.data[filekey]
            table_name = ntpath.basename(filekey)
            file_statements = {}
            for key in data_to_print:
                if key == "overall_info":
                    continue
                entry = data_to_print[key]
                if not isinstance(entry, list) or len(entry) < 1:
                    continue
                # output_folder_final = output_folder
                if "/" in key:
                    key = key.replace("/", "_")  # replace all slashes with underscores to prevent misinterpretation as folder
                output_path = path.join(output_folder, key + ".txt")
                final_lines, origpost_len, origpost_rest_len, origpost_rest_wo_sc_len = \
                    self.create_data_for_file(entry[0], self.current_key, table_name)

                statement = {
                    "table_name":               table_name,
                    "origpost_len":             origpost_len,
                    "origpost_rest_len":        origpost_rest_len,
                    "origpost_rest_wo_sc_len":  origpost_rest_wo_sc_len
                }

                file_statements[key] = statement
                self.write_array_to_root_simple(output_folder, output_path, final_lines, append_mode=True)

                # accumulate all statements for one file
                acc__statements = self.accumulate_file_statements(file_statements, acc__statements)


        # write statement data to current folder
        output_path_folder_res = path.join(output_folder, "aaa_folder_results.txt")
       
        if bool(acc__statements):
            folder_report = self.create_report_for_folder(acc__statements, output_path)
            self.write_array_to_root_simple(output_folder, output_path_folder_res, folder_report, append_mode=True)

        # pass statement data to overall processing
        return acc__statements

    def log_final_results(self, results):
        path = self.output_path
        final_report = self.create_report_for_folder(results, path)

        self.write_array_to_root_simple(path, path+"final_report.txt", final_report, append_mode=True)

    def accumulate_final_results(self, folder_statements, acc_statements):
        for key in folder_statements:
            entry = folder_statements[key]
            #table_name = entry['table_name']
            origpost_len = entry['acc_orig_len']
            origpost_rest_len = entry['acc_rest_len']
            origpost_rest_wo_sc_len = entry['acc_rest_wo_sc_len']
            if key not in acc_statements.keys():
                acc_statements[key] = {
                    "acc_orig_len": origpost_len,
                    "acc_rest_len": origpost_rest_len,
                    "acc_rest_wo_sc_len": origpost_rest_wo_sc_len
                }
            else:
                acc_statements[key]["acc_orig_len"] += origpost_len
                acc_statements[key]["acc_rest_len"] += origpost_rest_len
                acc_statements[key]["acc_rest_wo_sc_len"] += origpost_rest_wo_sc_len

        return acc_statements

    def accumulate_file_statements(self, file_statements, acc_statements):
        for key in file_statements:
            entry = file_statements[key]
            #table_name = entry['table_name']
            origpost_len = entry['origpost_len']
            origpost_rest_len = entry['origpost_rest_len']
            origpost_rest_wo_sc_len = entry['origpost_rest_wo_sc_len']
            if key not in acc_statements.keys():
                acc_statements[key] = {
                    "acc_orig_len": origpost_len,
                    "acc_rest_len": origpost_rest_len,
                    "acc_rest_wo_sc_len": origpost_rest_wo_sc_len
                }
            else:
                acc_statements[key]["acc_orig_len"] += origpost_len
                acc_statements[key]["acc_rest_len"] += origpost_rest_len
                acc_statements[key]["acc_rest_wo_sc_len"] += origpost_rest_wo_sc_len

        return acc_statements

    def create_report_for_folder(self, folder_statements, output_path):
        final_report = []
        separators = '%-70s%-30s%-30s%-30s%-30s'
        final_report.append("Folder:"+ output_path)
        headline_to_add = separators % ("category_name", "original_text_length", "rest_text_length", "rest_text_wo_sc_length", "subtracted_chars")
        final_report.append(headline_to_add)
        final_report.append("----------------------------------------------------------------")

        for key in folder_statements:
            statement = folder_statements[key]
            subtacted_chars = int(statement['acc_orig_len']) - int(statement['acc_rest_len'])
            text_to_add = separators % (key,
                                        statement['acc_orig_len'],
                                        statement['acc_rest_len'],
                                        statement['acc_rest_wo_sc_len'],
                                        subtacted_chars
                                        )

            final_report.append(text_to_add)

        return final_report

    def create_data_for_file(self, data , source_file, table_name):
        origpost = data['origpost']
        origpost_rest = data['origpost_rest']
        origpost_rest_wo_sc = data['origpost_rest_wo_sc']

        origpost_len = len(origpost)
        origpost_rest_len = len(origpost_rest)
        origpost_rest_wo_sc_len = len(origpost_rest_wo_sc)

        final_lines = []

        separators = '%-30s%-30s'
        final_lines.append(table_name + "------------------------")
        final_lines.append(
            separators % ("origpost: ", origpost))
        final_lines.append(
            separators % ("origpost_rest: ", origpost_rest))
        final_lines.append(
            separators % ("origpost_rest_wo_sc: ", origpost_rest_wo_sc))


        final_lines.append("") # empty lines for overview
        final_lines.append("")

        return final_lines, origpost_len, origpost_rest_len, origpost_rest_wo_sc_len

    # create a file for each tag in output



    # log file info and compared output

    # (optional) per file create stats and accumulate them

    def write_array_to_root_simple(self, full_dir, full_path, text_lines, append_mode=False):

        self.create_directory_tree(full_dir)
        # write append or normal
        if append_mode is True:
            my_file = io.open(full_path, 'a', encoding='utf8')
        else:
            my_file = io.open(full_path, 'w', encoding='utf8')

        for text_line in text_lines:
            my_file.write(text_line + "\n")

        my_file.close()

    def create_directory_tree(self, path):
        # abspath = os.path.abspath(path)
        path_exists = os.path.exists(path)

        if not path_exists:
            os.makedirs(path)

    def delete_directory_tree(self, path):
        if os.path.exists(path):
            shutil.rmtree(path)


    def change_folder(self):
        self.data = {}