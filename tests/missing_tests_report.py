#!/usr/bin/python3
"""
    This script print a report pointing the missing test files, and for the existing files the missing test methods
"""
from os import walk
import re

source_folder="./hydra_base/lib"
test_folder="./tests"

def check_correct_file(fileName):
    # Extracts files that are ".py" and not starting with _
    match = re.match( r'[^_].*\.py$', fileName, re.I)
    if match is not None:
        return True
    else:
        return False


def extract_file_names_from_dir(folder):
    # Extracts files names from a directory
    for (dirpath, dirnames, filenames) in walk(folder):
        break
    files = list(filter(check_correct_file, filenames ))
    files.sort()
    return files


source_files = extract_file_names_from_dir(source_folder)
test_files = extract_file_names_from_dir(test_folder)

source_methods = dict()
test_methods = {}

def extract_methods_from_file(folder_name, file_name):
    # Returns all the methods int he file that are not private to the file
    file_methods = []
    with open("{}/{}".format(folder_name,file_name)) as f:
        content = f.readlines()
        content = [x.strip() for x in content]
        for line in content:
            match = re.match( r'^def (OLD_)?([^_].*)\(', line)
            if match is not None:
                file_methods.append(match.group(2))
            else:
                pass

    file_methods.sort()
    return file_methods

# Getting all methods for all source files
for source_filename in source_files:
    source_methods[source_filename] = extract_methods_from_file(source_folder, source_filename)

# Getting all methods for all test files
for test_filename in test_files:
    test_methods[test_filename] = extract_methods_from_file(test_folder, test_filename)

# Comparing the existence of test methods for each source file
print
for source_file in source_methods:
    test_file_name = "test_{}".format(source_file)
    if not test_file_name in test_methods:
        # The test filename does not exists
        print ("Filename:{}, Missing: 'ALL'".format(test_file_name))
    else:
        test_file_methods = test_methods[test_file_name]
        for source_method in source_methods[source_file]:
            test_method = "test_{}".format(source_method)
            if not test_method in test_file_methods:
                print ("File:{}, Missing: {}".format(test_file_name, test_method))
                #print("The test called '{}' in the test filename {} does not exists!".format(test_method, test_file_name))
