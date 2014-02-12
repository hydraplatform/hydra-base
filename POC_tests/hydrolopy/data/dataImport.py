#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" MODULE : dataImport, part of hydrology.data

"""

__all__ = ["importODS", "importCSV"]


from zipfile import ZipFile
from lxml import etree


def importODS(fileName, sheet='@all'):
    """Import data from an OpenDocument spreadsheet file. The imported
       data is stored in a nested dictionary with one entry for each
       sheet on the top level and one entry for each column at the
       second level. The first row of each column is used as key for the
       data saved in the column.
    """

    unz = ZipFile(fileName)
    odsdata = unz.read('content.xml')
    odsdata = etree.XML(odsdata)

    # Debugging
    #etree.dump(odsdata,pretty_print=True)

    SpreadsheetNames = _getODSsheetNames(odsdata)

    SheetContent = dict()

    if sheet == '@all':
        for sname in SpreadsheetNames:
            SheetContent.update({sname: _getODSsheetContent(odsdata, sname)})
    else:
        SheetContent.update({sname: _getODSsheetContent(odsdata, sname)})

    return SheetContent


def importCSV(filename,  separator=',', importrange=None):
    """Import a comma separated value (CSV) file."""
    file = open(filename)
    csvdata = list()

    line = file.readline().rstrip('\r\n')
    while line:
        if line[0] != '#':
            tmpdata = line.split(separator)
            for i in range(len(tmpdata)):
                try:
                    if not len(tmpdata[i]):
                        tmpdata[i] = 'nan'
                    tmpdata[i] = float(tmpdata[i])
                except:
                    pass
            csvdata.append(tmpdata)
        line = file.readline().rstrip('\n\r')

    return csvdata


## Private functions:
def _getODSsheetNames(odsdata):
    """Get the names of all the sheets in a given ODS table."""

    sheetNames = []
    for s in odsdata.xpath('.//table:table', namespaces=odsdata.nsmap):
        sheetNames.append(s.attrib['{' + odsdata.nsmap['table'] \
                                       + '}name'])

    return sheetNames


def _getODSsheetContent(odsdata, sheetName):
    """Get the contents of a ODS sheet with name <sheetName>."""

    sheetData = dict()
    header = []
    nrow = 0
    repeat = 1
    tableName = '{' + odsdata.nsmap['table'] + '}'
    officeName = '{' + odsdata.nsmap['office'] + '}'
    rowTag = tableName + 'table-row'
    repTag = tableName + 'number-columns-repeated'

    for s in odsdata.xpath('.//table:table', namespaces=odsdata.nsmap):
        currentSheetName = s.attrib[tableName + 'name']
        if currentSheetName == sheetName:

            for row in s.iterchildren():

                if row.tag == rowTag:
                    nrow += 1
                    ncell = 0
                    if nrow == 1:
                        # Import the headers of the data
                        for cell in row.iterchildren():
                            if officeName + 'value-type' in cell.attrib:
                                celltype = cell.attrib[officeName + \
                                           'value-type']
                            else:
                                celltype = None

                            if celltype == 'string':
                                cellValue = cell.getchildren()[0].text
                                header.append(cellValue)
                            elif celltype == 'float':
                                cellValue = float(cell.attrib[officeName + \
                                                  'value'])
                                #cellValue = float(cell.getchildren()[0].text)
                                header.append(cellValue)

                    else:
                        # import the data
                        for cell in row.iterchildren():
                            if officeName + 'value-type' in cell.attrib:
                                celltype = cell.attrib[officeName + \
                                           'value-type']
                            else:
                                celltype = None

                            if celltype == 'string':
                                cellValue = cell.getchildren()[0].text
                                # Check if the cell is repeated
                                if repTag in cell.attrib:
                                    repeat = int(cell.attrib[repTag])
                            elif celltype == 'float':
                                cellValue = float(cell.attrib[officeName + \
                                                  'value'])
                                #cellValue = float(cell.getchildren()[0].text)
                                # Check if the cell is repeated
                                if repTag in  cell.attrib:
                                    repeat = int(cell.attrib[repTag])
                            elif celltype == None:
                                repeat = 0

                            for rep in range(repeat):
                                tablecontent = sheetData.get(header[ncell], [])
                                tablecontent.append(cellValue)
                                sheetData.update({header[ncell]: tablecontent})
                                ncell += 1
                            repeat = 1

    return sheetData


if __name__ == '__main__':
    """This is the place, where automated tests can be designed. They will run
    if this file is called directly from console."""
    pass
