"""
File name: adb2re.py
Author: Lester Manalastas
Email: lmanalastas@rocketsoftware.com
Date created: 05/08/2020
Python Version: 3.7

Wrapper to call the ADB2RE stored procedure for generating DDL and statistics
Reference: https://www.ibm.com/support/knowledgecenter/SSAUZ9_12.1.0/topics/adbu_rev-eng_adb2re.html

Variable and functions will be used to build and execute the following call:
CALL tsoid.ADB2RE (parameter_list, request_list, __sql_output_list,  __rpt_output_list, DEBUG_MODE, <rc var>)

    parameter_list : list of GEN options such as GENDB, GRANTVW, ACCEPT_FL, etc
    request_list : list of objects to GEN, differentiated by their type, qualifier, and name
    __sql_output_list : specifies where to put the generated DDL
    __rpt_output_list : specifies where to put the generated report where any warnings, messages, etc go
    DEBUG_MODE : specifies whether to run GEN in debug mode or not
    <rc var> : not used in this python class, but represents the GEN return code
"""

from libs.aoc.utility import *
from taf.db2 import db2
import os
from pathlib import Path
import jaydebeapi


class Adb2re:
    # Initialize variables from configuration file
    DB2REL_CONFIG = DB2_VERSION
    PORT = LPAR_SSID[LPAR][DB2_SUBSYSTEM_ID]

    # In case config is still allowing two characters, default the remaining two to make 4
    if len(DB2REL_CONFIG) == 2:
        DB2REL_CONFIG += '15'

    # General Db2 options
    DB2SYS = DB2_SUBSYSTEM_ID
    DB2ALOC = ''
    DB2SERV = LPAR + DB2_SUBSYSTEM_ID
    DB2AUTH = TSO_USER_ID
    DB2REL = DB2REL_CONFIG

    # Options to generate DDL statements
    GENSG = GENDB = GENTS = GENTABLE = GENVIEW = GENINDEX = GENSYN = GENALIAS = GENUDT = GENUDF = GENSTP = GENSEQ = \
        GENVAR = GENLABEL = GENCOMM = GENRELS = GENTRIG = GENTRUST = GENROLE = GENMASK = GENPERM = GENSEQAL = 'N'

    # Options to generate GRANT statements
    GRANTSG = GRANTDB = GRANTTS = GRANTTAB = GRANTVW = GRANTSCH = GRANTUDT = GRANTUDF = GRANTSTP = GRANTSEQ = GRANTVAR = 'N'

    # Additional options to generate statements
    ACCEPT_FL = ACCEPT_FL
    ACTVCNTL = 'N'
    CATALOGSTATISTICS = 'N'
    TCATQUAL = ''
    TGTFL = ACCEPT_FL

    # STATISTICS Options
    SYSCOLDIST = SYSCOLDISTSTATS = SYSCOLSTATS = SYSCOLUMNS = SYSINDEX = SYSIXPART = SYSIXSTATS = SYSLOBSTATS = \
        SYSTBPART = SYSTABLES = SYSTABLESPACE = SYSIXPSTATS = SYSTSPSTATS = SYSROUTINES = SYSKEYTGTDIST = \
        SYSKEYTGTDISTSTATS = SYSKEYTARGETSTATS = 'Y'

    # Options to change DDL during generation
    NEWSQLID = NEWGRANTOR = NEWDB = NEWTSSG = NEWIXSG = ''

    # Additional options to customize the DDL
    PENDCHGS = 'Y'
    SPCALLOC = 'DEFINED'
    TGTDB2 = DB2REL_CONFIG
    DEFAULTS = 'K'
    COMMITFR = 'A'
    RUNSQLID = ''
    SQLCMTS = 'N'

    # sql-output options
    SQL_OUTFLAG = 'RS'
    SQL_DSNAME = SQL_MEMBER = SQL_UNIT = SQL_VOLSER = ''
    __sql_output_list = ''

    # report-output options
    RPT_OUTFLAG = 'BO'
    RPT_DSNAME = TSO_USER_ID + ".REPORT.TEMP"
    RPT_MEMBER = RPT_UNIT = RPT_VOLSER = ''
    __rpt_output_list = ''

    DEBUG_MODE = False

    # list of options
    parameter_list = ""
    # list of db2 objects
    request_list = ""

    # DDL output control variables
    __ddl_as_array = []
    __ddl_as_string = ""
    __executed_at_least_once = False
    __saved_report = ''

    def add_stogroup(self, sgname):
        """
        Append a stogroup object to the request_list
        """
        self.request_list += "TYPE='SG',NAME='"+sgname+"';"

    def add_database(self, dbname):
        """
        Append a database object to the request_list
        """
        self.request_list += "TYPE='DB',NAME='"+dbname+"';"

    def __add_object(self, type, qual, name):
        """
        Generic private method to add an object to the request_list
        """
        self.request_list += "TYPE='"+type+"',QUAL='"+qual+"',NAME='"+name+"';"

    def add_tablespace(self, dbname, tsname):
        """
        Append a table space object to the request_list
        """
        self.__add_object("TS", dbname, tsname)

    def add_table(self, qual, name):
        """
        Append a view object to the request_list
        """
        self.__add_object("TB", qual, name)

    def add_view(self, qual, name):
        """
        Append a view object to the request_list
        """
        self.__add_object("VW", qual, name)

    def add_alias(self, qual, name):
        """
        Append an alias object to the request_list
        """
        self.__add_object("AL", qual, name)

    def add_index(self, qual, name):
        """
        Append an index object to the request_list
        """
        self.__add_object("IX", qual, name)

    def add_user_defined_type(self, qual, name):
        """
        Append a user defined type object to the request_list
        """
        self.__add_object("DT", qual, name)

    def add_user_defined_function(self, qual, name):
        """
        Append a user defined function object to the request_list
        """
        self.__add_object("FU", qual, name)

    def add_stored_procedure(self, qual, name):
        """
        Append a stored procedure object to the request_list
        """
        self.__add_object("SP", qual, name)

    def add_sequence(self, qual, name):
        """
        Append a sequence object to the request_list
        """
        self.__add_object("SQ", qual, name)

    def add_sequence_alias(self, qual, name):
        """
        Append a sequence alias object to the request_list
        """
        self.request_list += "SCH='"+qual+"'," + "SEQ='"+name+"';"

    def add_schema(self, name):
        """
        Append a generic schema to the request_list
        """
        self.request_list += "SCH='"+name+"';"

    def add_trigger(self, qual, name):
        """
        Append a trigger object to the request_list
        """
        self.__add_object("TG", qual, name)

    def add_synonym(self, qual, name):
        """
        Append a synonym object to the request_list
        """
        self.__add_object("SY", qual, name)

    def __build_parameter_list(self):
        """
        Build the parameter_list based on global variables for GEN options
        """
        # always add these two as they are required
        self.parameter_list = "DB2SYS='" + self.DB2SYS + "',DB2REL='"+self.DB2REL+"'"

        # For remaining variables, only add if they differ from the default

        # General Db2 options
        if self.DB2ALOC is not '':
            self.parameter_list += ",DB2ALOC='"+self.DB2ALOC+"'"
        if self.DB2SERV is not LPAR + DB2_SUBSYSTEM_ID:
            self.parameter_list += ",DB2SERV='"+self.DB2SERV+"'"
        if self.DB2AUTH is not TSO_USER_ID:
            self.parameter_list += ",DB2AUTH='"+self.DB2AUTH+"'"

        # Options to generate DDL statements
        if self.GENSG is not 'N':
            self.parameter_list += ",GENSG='"+self.GENSG+"'"
        if self.GENDB is not 'N':
            self.parameter_list += ",GENDB='"+self.GENDB+"'"
        if self.GENTS is not 'N':
            self.parameter_list += ",GENTS='"+self.GENTS+"'"
        if self.GENTABLE is not 'N':
            self.parameter_list += ",GENTABLE='"+self.GENTABLE+"'"
        if self.GENVIEW is not 'N':
            self.parameter_list += ",GENVIEW='"+self.GENVIEW+"'"
        if self.GENINDEX is not 'N':
            self.parameter_list += ",GENINDEX='"+self.GENINDEX+"'"
        if self.GENSYN is not 'N':
            self.parameter_list += ",GENSYN='"+self.GENSYN+"'"
        if self.GENALIAS is not 'N':
            self.parameter_list += ",GENALIAS='"+self.GENALIAS+"'"
        if self.GENUDT is not 'N':
            self.parameter_list += ",GENUDT='"+self.GENUDT+"'"
        if self.GENUDF is not 'N':
            self.parameter_list += ",GENUDF='"+self.GENUDF+"'"
        if self.GENSTP is not 'N':
            self.parameter_list += ",GENSTP='"+self.GENSTP+"'"
        if self.GENSEQ is not 'N':
            self.parameter_list += ",GENSEQ='"+self.GENSEQ+"'"
        if self.GENVAR is not 'N':
            self.parameter_list += ",GENVAR='"+self.GENVAR+"'"
        if self.GENLABEL is not 'N':
            self.parameter_list += ",GENLABEL='"+self.GENLABEL+"'"
        if self.GENCOMM is not 'N':
            self.parameter_list += ",GENCOMM='"+self.GENCOMM+"'"
        if self.GENRELS is not 'N':
            self.parameter_list += ",GENRELS='"+self.GENRELS+"'"
        if self.GENTRIG is not 'N':
            self.parameter_list += ",GENTRIG='"+self.GENTRIG+"'"
        if self.GENTRUST is not 'N':
            self.parameter_list += ",GENTRUST='"+self.GENTRUST+"'"
        if self.GENROLE is not 'N':
            self.parameter_list += ",GENROLE='"+self.GENROLE+"'"
        if self.GENMASK is not 'N':
            self.parameter_list += ",GENMASK='"+self.GENMASK+"'"
        if self.GENPERM is not 'N':
            self.parameter_list += ",GENPERM='" + self.GENPERM + "'"
        if self.GENSEQAL is not 'N':
            self.parameter_list += ",GENSEQAL='" + self.GENSEQAL + "'"

        # Options to generate GRANT statements
        if self.GRANTSG is not 'N':
            self.parameter_list += ",GRANTSG='" + self.GRANTSG + "'"
        if self.GRANTDB is not 'N':
            self.parameter_list += ",GRANTDB='" + self.GRANTDB + "'"
        if self.GRANTTS is not 'N':
            self.parameter_list += ",GRANTTS='" + self.GRANTTS + "'"
        if self.GRANTTAB is not 'N':
            self.parameter_list += ",GRANTTAB='" + self.GRANTTAB + "'"
        if self.GRANTVW is not 'N':
            self.parameter_list += ",GRANTVW='" + self.GRANTVW + "'"
        if self.GRANTSCH is not 'N':
            self.parameter_list += ",GRANTSCH='" + self.GRANTSCH + "'"
        if self.GRANTUDT is not 'N':
            self.parameter_list += ",GRANTUDT='" + self.GRANTUDT + "'"
        if self.GRANTUDF is not 'N':
            self.parameter_list += ",GRANTUDF='" + self.GRANTUDF + "'"
        if self.GRANTSTP is not 'N':
            self.parameter_list += ",GRANTSTP='" + self.GRANTSTP + "'"
        if self.GRANTSEQ is not 'N':
            self.parameter_list += ",GRANTSEQ='" + self.GRANTSEQ + "'"
        if self.GRANTVAR is not 'N':
            self.parameter_list += ",GRANTVAR='" + self.GRANTVAR + "'"

        # Additional options to generate statements
        if self.ACCEPT_FL is not ACCEPT_FL:
            self.parameter_list += ",ACCEPT_FL='" + self.ACCEPT_FL + "'"
        if self.ACTVCNTL is not 'N':
            self.parameter_list += ",ACTVCNTL='" + self.ACTVCNTL + "'"
        if self.CATALOGSTATISTICS is not 'N':
            self.parameter_list += ",CATALOGSTATISTICS='" + self.CATALOGSTATISTICS + "'"
        if self.TCATQUAL is not '':
            self.parameter_list += ",TCATQUAL='" + self.TCATQUAL + "'"
        if self.TGTFL is not ACCEPT_FL:
            self.parameter_list += ",TGTFL='" + self.TGTFL + "'"

        # STATISTICS Options
        if self.SYSCOLDIST is not 'Y':
            self.parameter_list += ",GENSTATS.SYSCOLDIST='" + self.SYSCOLDIST + "'"

        if self.SYSCOLDISTSTATS is not 'Y':
            self.parameter_list += ",GENSTATS.SYSCOLDISTSTATS='" + self.SYSCOLDISTSTATS + "'"

        if self.SYSCOLSTATS is not 'Y':
            self.parameter_list += ",GENSTATS.SYSCOLSTATS='" + self.SYSCOLSTATS + "'"

        if self.SYSCOLUMNS is not 'Y':
            self.parameter_list += ",GENSTATS.SYSCOLUMNS='" + self.SYSCOLUMNS + "'"

        if self.SYSINDEX is not 'Y':
            self.parameter_list += ",GENSTATS.SYSINDEXES='" + self.SYSINDEX + "'"

        if self.SYSIXPART is not 'Y':
            self.parameter_list += ",GENSTATS.SYSINDEXPART='" + self.SYSIXPART + "'"

        if self.SYSIXSTATS is not 'Y':
            self.parameter_list += ",GENSTATS.SYSINDEXSTATS='" + self.SYSIXSTATS + "'"

        if self.SYSLOBSTATS is not 'Y':
            self.parameter_list += ",GENSTATS.SYSLOBSTATS='" + self.SYSLOBSTATS + "'"

        if self.SYSTBPART is not 'Y':
            self.parameter_list += ",GENSTATS.SYSTABLEPART='" + self.SYSTBPART + "'"

        if self.SYSTABLES is not 'Y':
            self.parameter_list += ",GENSTATS.SYSTABLES='" + self.SYSTABLES + "'"

        if self.SYSTABLESPACE is not 'Y':
            self.parameter_list += ",GENSTATS.SYSTABLESPACE='" + self.SYSTABLESPACE + "'"

        if self.SYSIXPSTATS is not 'Y':
            self.parameter_list += ",GENSTATS.SYSINDEXSPACESTATS='" + self.SYSIXPSTATS + "'"

        if self.SYSTSPSTATS is not 'Y':
            self.parameter_list += ",GENSTATS.SYSTABLESPACESTATS='" + self.SYSTSPSTATS + "'"

        if self.SYSKEYTGTDISTSTATS is not 'Y':
            self.parameter_list += ",GENSTATS.SYSKEYTGTDISTSTATS='" + self.SYSKEYTGTDISTSTATS + "'"

        if self.SYSKEYTARGETSTATS is not 'Y':
            self.parameter_list += ",GENSTATS.SYSKEYTARGETSTATS='" + self.SYSKEYTARGETSTATS + "'"

        if self.SYSKEYTGTDIST is not 'Y':
            self.parameter_list += ",GENSTATS.SYSKEYTGTDIST='" + self.SYSKEYTGTDIST + "'"

        if self.SYSROUTINES is not 'Y':
            self.parameter_list += ",GENSTATS.SYSROUTINES='" + self.SYSROUTINES + "'"

        # Options to change DDL during generation
        if self.NEWGRANTOR is not '':
            self.parameter_list += ",NEWGRANTOR='" + self.NEWGRANTOR + "'"
        if self.NEWDB is not '':
            self.parameter_list += ",NEWDB='" + self.NEWDB + "'"
        if self.NEWTSSG is not '':
            self.parameter_list += ",NEWTSSG='" + self.NEWTSSG + "'"
        if self.NEWIXSG is not '':
            self.parameter_list += ",NEWIXSG='" + self.NEWIXSG + "'"
        if self.NEWSQLID is not '':
            self.parameter_list += ",NEWSQLID='" + self.NEWSQLID + "'"

        # Additional options to customize the DDL
        if self.PENDCHGS is not 'Y':
            self.parameter_list += ",PENDCHGS='" + self.PENDCHGS + "'"
        if self.SPCALLOC is not 'DEFINED':
            self.parameter_list += ",SPCALLOC='" + self.SPCALLOC + "'"
        if self.TGTDB2 is not self.DB2REL_CONFIG:
            self.parameter_list += ",TGTDB2='" + self.TGTDB2 + "'"
        if self.DEFAULTS is not 'K':
            self.parameter_list += ",DEFAULTS='" + self.DEFAULTS + "'"
        if self.COMMITFR is not 'A':
            self.parameter_list += ",COMMITFR='" + self.COMMITFR + "'"
        if self.RUNSQLID is not '':
            self.parameter_list += ",RUNSQLID='" + self.RUNSQLID + "'"
        if self.SQLCMTS is not '':
            self.parameter_list += ",SQLCMTS='" + self.SQLCMTS + "'"

        # add semicolon
        self.parameter_list += ";"

    def __build_output_lists(self):
        """
        Build the sql_output_list and rpt_output_list based on SQL_ and RPT_ global variables
        """
        # sql-output options; outflag is always required
        self.__sql_output_list = "SQL_OUTFLAG='" + self.SQL_OUTFLAG + "'"

        # add remaining if provided
        if self.SQL_DSNAME is not '':
            self.__sql_output_list += ",SQL_DSNAME='" + self.SQL_DSNAME + "'"
        if self.SQL_MEMBER is not '':
            self.__sql_output_list += ",SQL_MEMBER='" + self.SQL_MEMBER + "'"
        if self.SQL_UNIT is not '':
            self.__sql_output_list += ",SQL_UNIT='" + self.SQL_UNIT + "'"
        if self.SQL_VOLSER is not '':
            self.__sql_output_list += ",SQL_VOLSER='" + self.SQL_VOLSER + "'"
        self.__sql_output_list += ";"

        # report-output options; outflag is always required
        self.__rpt_output_list = "RPT_OUTFLAG='" + self.RPT_OUTFLAG + "'"
        self.__rpt_output_list += ",RPT_DSNAME='" + self.RPT_DSNAME + "'"

        # add remaining if provided
        if self.RPT_MEMBER is not '':
            self.__rpt_output_list += ",RPT_MEMBER='" + self.RPT_MEMBER + "'"
        if self.RPT_UNIT is not '':
            self.__rpt_output_list += ",RPT_UNIT='" + self.RPT_UNIT + "'"
        if self.RPT_VOLSER is not '':
            self.__rpt_output_list += ",RPT_VOLSER='" + self.RPT_VOLSER + "'"
        self.__rpt_output_list += ";"

    def execute(self, ssid=DB2_SUBSYSTEM_ID):
        """
        Once the test has specified applicable options, execute() is used to
        1) build the stored strings to be passed to the stored procedure
        2) connect to Db2
        3) build the stored procedure call
        4) call the ADB2RE stored procedure
        5) save the DDL
        Note: To save either the DDL or the report into a data set, reference the knowledge center link
        (top of this file) to specify the appropriate options
        """
        if len(self.request_list) < 1:
            raise Exception("adb2re.py: request_list cannot be empty; use functions like add_table to build the list")

        # update SSID if given
        self.PORT = LPAR_SSID[LPAR][ssid]
        self.DB2SYS = ssid
        self.DB2ALOC = ''
        self.DB2SERV = LPAR + ssid

        # Build parameter list
        self.__build_parameter_list()

        # build sql and rpt options
        self.__build_output_lists()

        # prior to call, logger.info out lists for visual purposes
        logger.debug("adb2re.py variables:" +
                    "\nparameter-list\n" + self.parameter_list +
                    "\nrequest-list\n" + self.request_list +
                    "\nsql-output-list\n" + self.__sql_output_list +
                    "\nrpt-output-list\n" + self.__rpt_output_list +
                    "\nADB2RE DEBUG set? " + ("True" if self.DEBUG_MODE else "False") + "\n")

        db2_file = Path(os.path.realpath(db2.__file__)).parent
        db2jcc4 = Path(os.path.join(db2_file, 'data', 'db2jcc4.jar'))
        db2jcc = Path(os.path.join(db2_file, 'data', 'db2jcc_license_cisuz.jar'))
        logger.debug(db2_file)
        logger.debug(db2jcc4)
        logger.debug(db2jcc)

        logger.debug("adb2re.py attempt connection")
        jdb = jaydebeapi.connect(
            'com.ibm.db2.jcc.DB2Driver',
            'jdbc:db2://' + LPAR + '.rocketsoftware.com:'+self.PORT+'/' + self.DB2SERV,
            [TSO_USER_ID, PASSWORD],
            [str(db2jcc4), str(db2jcc)]
        )
        logger.debug("adb2re.py connection complete")

        logger.debug("adb2re.py attempt to call stored procedure")
        cursor = jdb.cursor()
        cursor.execute("Call "+TSO_USER_ID+".ADB2RE(?, ?, ?, ?, ?, ?)", [
            self.parameter_list, self.request_list, self.__sql_output_list, self.__rpt_output_list,
            "DEBUG" if self.DEBUG_MODE else "", ""
        ])
        logger.debug("adb2re.py stored procedure call complete")

        if self.RPT_DSNAME is not '':
            self.__saved_report = \
                zOSMFConnector(host=TARGET, user=TSO_USER_ID, password=PASSWORD).read_ds(self.RPT_DSNAME)
            logger.debug("adb2re.py report stored in memory")
            # validate report
            self.assert_text_in_report(["ADB2GEN - Create DDL from catalog info"])
            # automatically check for -805
            self.assert_text_not_in_report(["SQLCODE=-805", "SQLCODE = -805"])

        logger.debug("adb2re.py attempt to fetch ddl")
        self.__ddl_as_array = []
        self.__ddl_as_string = ""
        for row in cursor.fetchall():
            self.__ddl_as_array.append(row[1])

        for row in self.__ddl_as_array:
            self.__ddl_as_string += row + '\n'

        logger.debug("adb2re.py ddl stored in memory")



        self.__executed_at_least_once = True
        cursor.close()

    def get_ddl_as_array(self):
        """
        :return the DDL as an array, one line per element
        """
        if not self.__executed_at_least_once:
            raise Exception("adb2re.py: Call execute() before retrieving DDL")
        return self.__ddl_as_array

    def get_ddl_as_string(self):
        """
        :return the DDL as one string, lines broken up by the newline character
        """
        if not self.__executed_at_least_once:
            raise Exception("adb2re.py: Call execute() before retrieving DDL")
        return self.__ddl_as_string

    def get_report_as_string(self):
        """
        :return the DDL as one string, lines broken up by the newline character
        """
        if not self.__executed_at_least_once:
            raise Exception("adb2re.py: Call execute() before retrieving report")
        return self.__saved_report

    def assert_text_between_terminators(self, checklist, term=';'):
        """
        :param checklist: is a 2d array of strings to check. example:
            [
                ["CREATE TABLE MYDB01","PROPERTY1"],
                ["CREATE TABLE MYTS01","PROPERTY1"],
                ["CREATE TABLE MYTB01","PROPERTY1"],
            ]

        :param term is an optional character used as the terminator between statements

        Using the example, if a string of many Db2 objects is split by terminator, one separated objects must contain
        every item in one of the arrays within checklist
        """
        if not self.__executed_at_least_once:
            raise Exception("adb2re.py: Call execute() before retrieving DDL")
        if self.get_ddl_as_string().find(term) < 0:
            raise Exception("adb2re.py: Could not find terminator '" + term + "' in DDL:\n" + self.get_ddl_as_string())
        split_ddl = self.get_ddl_as_string().split(term)

        # for every sublist within the checklist...
        for sublist in checklist:
            # check each terminator-separated statement in the ddl
            for stmt in split_ddl:
                # split took off the terminator. add it back in case expected text needs it.
                stmt += term
                # keep track if we've found the first item in the sublist
                found_first_one = False
                # for each item in the sublist...
                count = 1
                for item in sublist:
                    # only start checking once the first element is found (like CREATE TABLE MYTABLE1)
                    if stmt.count(item):
                        found_first_one = True
                    # this stmt contained at least the first item, but not one later in the list, so fail
                    elif found_first_one:
                        raise Exception("adb2re.py: Failed on sub-checklist:\n" + "\n".join(sublist) + "\nCould not " +
                                        "find item#: " + str(count) + ", string value '" + item +
                                        "' in this statement:\n" +
                                        stmt + term + "\nFull DDL:\n" + self.get_ddl_as_string())
                    # didn't find the first one? move on to next stmt (prevent false positives on later sublist items)
                    else:
                        break
                    count = count + 1
                # if found_first_one and didn't fail, move on
                if found_first_one:
                    break
            # iterated entire DDL but didn't get a single hit on the current sublist
            if not found_first_one:
                raise Exception("adb2re.py: Could not find any items in DDL from sub-checklist:\n" + "\n".join(sublist))

    def assert_text_in_report(self, text, positive_test=True):
        """
        :param text, an array of strings
        :param positive_test, controls if we are expecting or not expecting the text
        """
        if self.RPT_DSNAME is '':
            raise Exception("adb2re.py: RPT_DSNAME must be set before calling execute()")

        for line in text:
            if positive_test and not self.__saved_report.count(line):
                raise Exception("adb2re.py Failed to find line '"+line+"' in report. Full report:\n\n" +
                                self.__saved_report)
            elif not positive_test and self.__saved_report.count(line):
                raise Exception("adb2re.py Unexpectedly found line '" + line + "' in report. Full report:\n\n" +
                                self.__saved_report)

    def assert_text_not_in_report(self, text):
        """
        :param text, an array of strings
        """
        self.assert_text_in_report(text, False)

    def set_all_gen_options(self, value='Y'):
        """
        :param value, set all GEN options to value
        """
        # Options to generate DDL statements
        self.GENSG = self.GENDB = self.GENTS = self.GENTABLE = self.GENVIEW = self.GENINDEX = self.GENSYN = \
            self.GENALIAS = self.GENUDT = self.GENUDF = self.GENSTP = self.GENSEQ = self.GENVAR = self.GENLABEL = \
            self.GENCOMM = self.GENRELS = self.GENTRIG = self.GENTRUST = self.GENROLE = self.GENMASK = self.GENPERM = \
            self.GENSEQAL = value

    def set_all_grant_options(self, value='Y'):
        """
        :param value, set all GRANT options to value
        """
        # Options to generate GRANT statements
        self.GRANTSG = self.GRANTDB = self.GRANTTS = self.GRANTTAB = self.GRANTVW = self.GRANTSCH = self.GRANTUDT = \
            self.GRANTUDF = self.GRANTSTP = self.GRANTSEQ = self.GRANTVAR = value
