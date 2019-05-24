#!/usr/bin/env python

"""
A new beam diagnostics loader for the PALFA survey.

Patrick Lazarus, Dec. 20th, 2010
"""
import re
import sys
import glob
import os.path
import tarfile
import optparse
import types
import binascii
import time
import gzip

import numpy as np

import debug
import database
import upload
import pipeline_utils
from formats import accelcands
from formats import ffacands
from singlepulse.spio import split_parameters
import config.basic


class Diagnostic(upload.Uploadable):
    """An abstract class to represent PALFA diagnostics.
    """
    # Define some class attributes
    description = None
    name = None
    
    def __init__(self, obs_name, beam_id, obstype, version_number, directory, sp_directory):
        self.obs_name = obs_name
        self.beam_id = beam_id
        self.obstype = obstype.lower()
        self.version_number = version_number
        self.directory = directory
        self.sp_directory = sp_directory
        # Store a few configurations so the upload can be checked
        self.pipeline = config.basic.pipeline
        self.institution = config.basic.institution

    def get_diagnostic(self):
        raise NotImplementedError("Method 'get_diagnostic(...)' " \
                                    "is not implemented for %s." % self.__class__.__name__)

    def get_upload_sproc_call(self):
        raise NotImplementedError("Method 'get_upload_sproc_call(...)' " \
                                    "is not implemented for %s." % self.__class__.__name__)

    def upload(self, dbname='default', *args, **kwargs):
        """An extension to the inherited 'upload' method.

            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
        """
        if debug.UPLOAD: 
            starttime = time.time()
        super(Diagnostic, self).upload(dbname=dbname, *args, **kwargs)
        self.compare_with_db(dbname=dbname)
        
        if debug.UPLOAD:
            upload.upload_timing_summary['diagnostics'] = \
                upload.upload_timing_summary.setdefault('diagnostics', 0) + \
                (time.time()-starttime)

class FloatDiagnostic(Diagnostic):
    """An abstract class to represent float-valued PALFA diagnostics.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'obs_name': '%s', \
              'beam_id': '%d', \
              'institution': '%s', \
              'pipeline': '%s', \
              'version_number': '%s', \
              'name': '%s', \
              'description': '%s', \
              'value': '%.12g', \
              'obstype': '%s'}
    
    def __init__(self, *args, **kwargs):
        super(FloatDiagnostic, self).__init__(*args, **kwargs)
        self.value = None # The diagnostic value to upload
        self.get_diagnostic()

    def get_upload_sproc_call(self):
        if self.value == "NULL":
            self.value = 0.0
        sprocstr = "EXEC spDiagnosticAdder " \
            "@obs_name='%s', " % self.obs_name + \
            "@beam_id=%d, " % self.beam_id + \
            "@instit='%s', " % config.basic.institution + \
            "@pipeline='%s', " % config.basic.pipeline + \
            "@version_number='%s', " % self.version_number + \
            "@diagnostic_type_name='%s', " % self.name + \
            "@diagnostic_type_description='%s', " % self.description + \
            "@diagnostic=%.12g, " % self.value + \
            "@obsType='%s'" % self.obstype.lower()
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding diagnostic from DB and compare values.
            Raise a DiagnosticError if any mismatch is found.
            
            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Outputs:
                None
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT obs.obs_name, " \
                        "h.beam_id, " \
                        "v.institution, " \
                        "v.pipeline, " \
                        "v.version_number, " \
                        "dtype.diagnostic_type_name AS name, " \
                        "dtype.diagnostic_type_description AS description, " \
                        "d.diagnostic_value AS value, " \
                        "h.obsType AS obstype " \
                   "FROM diagnostics AS d " \
                   "LEFT JOIN diagnostic_types AS dtype " \
                        "ON dtype.diagnostic_type_id=d.diagnostic_type_id " \
                   "LEFT JOIN headers AS h ON h.header_id=d.header_id " \
                   "LEFT JOIN observations AS obs ON obs.obs_id=h.obs_id " \
                   "LEFT JOIN versions AS v ON v.version_id=d.version_id " \
                   "WHERE obs.obs_name='%s' AND h.beam_id=%d " \
                        "AND v.institution='%s' AND v.version_number='%s' " \
                        "AND v.pipeline='%s' AND h.obsType='%s' " \
                        "AND dtype.diagnostic_type_name='%s' " \
                        "AND dtype.diagnostic_type_description='%s' " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            self.version_number, config.basic.pipeline, \
                            self.obstype.lower(), self.name, self.description))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(obs_name: %s,\n" \
                                " beam_id: %d,\n" \
                                " insitution: %s,\n" \
                                " pipeline: %s,\n" \
                                " version_number: %s,\n" \
                                " diagnostic_type_name: %s,\n" \
                                " diagnostic_type_description: %s,\n" \
                                " obsType: %s) " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            config.basic.pipeline, self.version_number, \
                            self.name, self.description, self.obstype.lower()))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(obs_name: %s,\n" \
                                " beam_id: %d,\n" \
                                " insitution: %s,\n" \
                                " pipeline: %s,\n" \
                                " version_number: %s,\n" \
                                " diagnostic_type_name: %s,\n" \
                                " diagnostic_type_description: %s,\n" \
                                " obsType: %s) " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            config.basic.pipeline, self.version_number, \
                            self.name, self.description, self.obstype.lower()))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            errormsgs = []
            for var, fmt in self.to_cmp.iteritems():
                local = (fmt % getattr(self, var)).lower()
                fromdb = (fmt % r[var]).lower()
                if local != fromdb:
                    errormsgs.append("Values for '%s' don't match (local: %s, DB: %s)" % \
                                        (var, local, fromdb))
            if errormsgs:
                errormsg = "Float diagnostic doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise DiagnosticError(errormsg)


class PlotDiagnostic(Diagnostic):
    """An abstract class to represent binary PALFA diagnostics.
    """
    # A dictionary which contains variables to compare (as keys) and
    # how to compare them (as values)
    to_cmp = {'obs_name': '%s', \
              'beam_id': '%d', \
              'institution': '%s', \
              'pipeline': '%s', \
              'version_number': '%s', \
              'name': '%s', \
              'description': '%s', \
              'value': '%s', # The binary file's name \
              'datalen': '%d', \
              'obstype': '%s'}
    
    def __init__(self, *args, **kwargs):
        super(PlotDiagnostic, self).__init__(*args, **kwargs)
        self.value = None # The binary file's name
        self.filedata = None # The binary file's data
        self.datalen = None # The number of bytes in the binary file
        self.get_diagnostic()

    def get_upload_sproc_call(self):
        sprocstr = "EXEC spDiagnosticPlotAdder " \
            "@obs_name='%s', " % self.obs_name + \
            "@beam_id=%d, " % self.beam_id + \
            "@instit='%s', " % config.basic.institution + \
            "@pipeline='%s', " % config.basic.pipeline + \
            "@version_number='%s', " % self.version_number + \
            "@diagnostic_plot_type_name='%s', " % self.name + \
            "@diagnostic_plot_type_description='%s', " % self.description + \
            "@filename='%s', " % os.path.split(self.value)[-1] + \
            "@diagnostic_plot=0x%s, " % self.filedata.encode('hex') + \
            "@obsType='%s'" % self.obstype
        return sprocstr

    def compare_with_db(self, dbname='default'):
        """Grab corresponding diagnostic plot from DB and compare values.
            Raise a DiagnosticError if any mismatch is found.
            
            Input:
                dbname: Name of database to connect to, or a database
                        connection to use (Defaut: 'default').
            Output:
                None
        """
        if isinstance(dbname, database.Database):
            db = dbname
        else:
            db = database.Database(dbname)
        db.execute("SELECT obs.obs_name, " \
                        "h.beam_id, " \
                        "v.institution, " \
                        "v.pipeline, " \
                        "v.version_number, " \
                        "dtype.diagnostic_plot_type_name AS name, " \
                        "dtype.diagnostic_plot_type_description AS description, " \
                        "d.filename AS value, " \
                        "DATALENGTH(d.diagnostic_plot) AS datalen, " \
                        "h.obsType AS obstype " \
                   "FROM diagnostic_plots AS d " \
                   "LEFT JOIN diagnostic_plot_types AS dtype " \
                        "ON dtype.diagnostic_plot_type_id=d.diagnostic_plot_type_id " \
                   "LEFT JOIN headers AS h ON h.header_id=d.header_id " \
                   "LEFT JOIN observations AS obs ON obs.obs_id=h.obs_id " \
                   "LEFT JOIN versions AS v ON v.version_id=d.version_id " \
                   "WHERE obs.obs_name='%s' AND h.beam_id=%d " \
                        "AND v.institution='%s' AND v.version_number='%s' " \
                        "AND v.pipeline='%s' AND h.obsType='%s' " \
                        "AND dtype.diagnostic_plot_type_name='%s' " \
                        "AND dtype.diagnostic_plot_type_description='%s' " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            self.version_number, config.basic.pipeline, \
                            self.obstype.lower(), self.name, self.description))
        rows = db.cursor.fetchall()
        if type(dbname) == types.StringType:
            db.close()
        if not rows:
            # No matching entry in common DB
            raise ValueError("No matching entry in common DB!\n" \
                                "(obs_name: %s,\n" \
                                " beam_id: %d,\n" \
                                " insitution: %s,\n" \
                                " pipeline: %s,\n" \
                                " version_number: %s,\n" \
                                " diagnostic_plot_type_name: %s,\n" \
                                " diagnostic_plot_type_description: %s,\n" \
                                " obsType: %s) " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            config.basic.pipeline, self.version_number, \
                            self.name, self.description, self.obstype.lower()))
        elif len(rows) > 1:
            # Too many matching entries!
            raise ValueError("Too many matching entries in common DB!\n" \
                                "(obs_name: %s,\n" \
                                " beam_id: %d,\n" \
                                " insitution: %s,\n" \
                                " pipeline: %s,\n" \
                                " version_number: %s,\n" \
                                " diagnostic_plot_type_name: %s,\n" \
                                " diagnostic_plot_type_description: %s,\n" \
                                " obsType: %s) " % \
                        (self.obs_name, self.beam_id, config.basic.institution, \
                            config.basic.pipeline, self.version_number, \
                            self.name, self.description, self.obstype.lower()))
        else:
            desc = [d[0] for d in db.cursor.description]
            r = dict(zip(desc, rows[0]))
            errormsgs = []
            for var, fmt in self.to_cmp.iteritems():
                local = (fmt % getattr(self, var)).lower()
                fromdb = (fmt % r[var]).lower()
                if local != fromdb:
                    errormsgs.append("Values for '%s' don't match (local: %s, DB: %s)" % \
                                        (var, local, fromdb))
            if errormsgs:
                errormsg = "Float diagnostic doesn't match what was uploaded to the DB:"
                for msg in errormsgs:
                    errormsg += '\n    %s' % msg
                raise DiagnosticError(errormsg)


class RFIPercentageDiagnostic(FloatDiagnostic):
    name = "RFI mask percentage"
    description = "Percentage of data masked due to RFI."
    
    maskpcnt_re = re.compile(r"Number of  bad   intervals:.*\((?P<masked>.*)%\)")

    def get_diagnostic(self):
        # find *rfifind.out file
        rfiouts = glob.glob(os.path.join(self.directory, "*rfifind.out"))

        if len(rfiouts) != 1:
            raise DiagnosticError("Wrong number of rfifind output files found (%d)!" % \
                                    len(rfiouts))
        rfifile = open(rfiouts[0], 'r')
        for line in rfifile:
            m = self.maskpcnt_re.search(line)
            if m:
                self.value = float(m.groupdict()['masked'])
                break
        rfifile.close()


class RFIPlotDiagnostic(PlotDiagnostic):
    name = "RFIfind png"
    description = "Output image produced by rfifind in png format."

    def get_diagnostic(self):
        # find *rfifind.png file
        rfipngs = glob.glob(os.path.join(self.directory, '*rfifind.png'))

        if len(rfipngs) != 1:
            raise DiagnosticError("Wrong number of rfifind pngs found (%d)!" % \
                                len(rfipngs))
        else:
            self.value = os.path.split(rfipngs[0])[-1]
            self.datalen = os.path.getsize(rfipngs[0])
            rfipng_file = open(rfipngs[0], 'rb')
            self.filedata = rfipng_file.read()
            rfipng_file.close()


class PeriodicitySummaryPlotDiagnostic(PlotDiagnostic):
    name = "Periodicty summary plot"
    description = "A plot summarizing all periodicity candidates " \
                  "generated by accelsearch in png format."

    def get_diagnostic(self):
        # find *.accelcands.summary.png file
        summarypngs = glob.glob(os.path.join(self.directory, \
                            '*.accelcands.summary.png'))

        if len(summarypngs) != 1:
            raise DiagnosticError("Wrong number of accesearch summary " \
                                    "pngs found (%d)!" % len(summarypngs))
        else:
            self.value = os.path.split(summarypngs[0])[-1]
            self.datalen = os.path.getsize(summarypngs[0])
            summarypng_file = open(summarypngs[0], 'rb')
            self.filedata = summarypng_file.read()
            summarypng_file.close()

class FFASummaryPlotDiagnostic(PlotDiagnostic):
    name = "FFA summary plot"
    description = "A plot summarizing all FFA candidates " \
                  "generated by ffa.py in png format."

    def get_diagnostic(self):
        # find *.accelcands.summary.png file
        summarypngs = glob.glob(os.path.join(self.directory, \
                            '*.ffacands.summary.png'))

        if len(summarypngs) != 1:
            raise DiagnosticError("Wrong number of FFA summary " \
                                    "pngs found (%d)!" % len(summarypngs))
        else:
            self.value = os.path.split(summarypngs[0])[-1]
            self.datalen = os.path.getsize(summarypngs[0])
            summarypng_file = open(summarypngs[0], 'rb')
            self.filedata = summarypng_file.read()
            summarypng_file.close()


class PeriodicityRejectsPlotDiagnostic(PlotDiagnostic):
    name = "Rejected cands plot"
    description = "A plot highlighting why periodicity candidates " \
                  "generated by accelsearch were rejected during " \
                  "sifting. (File is in png format)."

    def get_diagnostic(self):
        # find *.accelsearch.rejects.png file
        rejectspngs = glob.glob(os.path.join(self.directory, \
                            '*.accelcands.rejects.png'))

        if len(rejectspngs) != 1:
            raise DiagnosticError("Wrong number of accesearch rejects " \
                                    "pngs found (%d)!" % len(rejectspngs))
        else:
            self.value = os.path.split(rejectspngs[0])[-1]
            self.datalen = os.path.getsize(rejectspngs[0])
            rejectspng_file = open(rejectspngs[0], 'rb')
            self.filedata = rejectspng_file.read()
            rejectspng_file.close()

class FFARejectsPlotDiagnostic(PlotDiagnostic):
    name = "Rejected FFA cands plot"
    description = "A plot highlighting why FFA candidates " \
                  "generated by ffa.py were rejected during " \
                  "sifting. (File is in png format)."

    def get_diagnostic(self):
        # find *.accelsearch.rejects.png file
        rejectspngs = glob.glob(os.path.join(self.directory, \
                            '*.ffacands.rejects.png'))

        if len(rejectspngs) != 1:
            raise DiagnosticError("Wrong number of FFA rejects " \
                                    "pngs found (%d)!" % len(rejectspngs))
        else:
            self.value = os.path.split(rejectspngs[0])[-1]
            self.datalen = os.path.getsize(rejectspngs[0])
            rejectspng_file = open(rejectspngs[0], 'rb')
            self.filedata = rejectspng_file.read()
            rejectspng_file.close()

class SiftingSummaryDiagnostic(PlotDiagnostic):
    name = "Sifting summary"
    description = "A summary of the number of candidates found " \
                  "by accelsearch and how many candidates were " \
                  "rejected for various reasons. (A text file)."
    
    def get_diagnostic(self):
        # find *.accelcands.summary file
        summaries = glob.glob(os.path.join(self.directory, \
                        "*.accelcands.summary"))

        if len(summaries) != 1:
            raise DiagnosticError("Wrong number of sifting summaries found (%d)!" % \
                                    len(summaries))
        summaryfile = open(summaries[0], 'r')
        self.value = os.path.split(summaryfile.name)[-1]
        self.filedata = summaryfile.read()
        self.datalen = os.path.getsize(summaryfile.name)
        summaryfile.close()

class FFASiftingSummaryDiagnostic(PlotDiagnostic):
    name = "FFA Sifting summary"
    description = "A summary of the number of candidates found " \
                  "by ffa and how many candidates were " \
                  "rejected for various reasons. (A text file)."
    
    def get_diagnostic(self):
        # find *.accelcands.summary file
        summaries = glob.glob(os.path.join(self.directory, \
                        "*.ffacands.summary"))

        if len(summaries) != 1:
            raise DiagnosticError("Wrong number of FFA sifting summaries found (%d)!" % \
                                    len(summaries))
        summaryfile = open(summaries[0], 'r')
        self.value = os.path.split(summaryfile.name)[-1]
        self.filedata = summaryfile.read()
        self.datalen = os.path.getsize(summaryfile.name)
        summaryfile.close()

class SiftingReportDiagnostic(PlotDiagnostic):
    name = "Sifting report"
    description = "A comprehensive report of why candidates were " \
                  "rejected during sifting. (A gzipped text file)."
    
    def get_diagnostic(self):
        # find *.accelcands.report.gz file
        reports = glob.glob(os.path.join(self.directory, \
                        "*.accelcands.report.gz"))

        if len(reports) != 1:
            raise DiagnosticError("Wrong number of sifting reports found (%d)!" % \
                                    len(reports))
        reportfile = open(reports[0], 'r')
        self.value = os.path.split(reportfile.name)[-1]
        self.filedata = reportfile.read()
        self.datalen = os.path.getsize(reportfile.name)
        reportfile.close()

class FFASiftingReportDiagnostic(PlotDiagnostic):
    name = "FFA Sifting report"
    description = "A comprehensive report of why FFA candidates were " \
                  "rejected during sifting. (A gzipped text file)."
    
    def get_diagnostic(self):
        # find *.accelcands.report.gz file
        reports = glob.glob(os.path.join(self.directory, \
                        "*.ffacands.report*"))

        if len(reports) != 1:
            raise DiagnosticError("Wrong number of FFA sifting reports found (%d)!" % \
                                    len(reports))
        reportfile = open(reports[0], 'r')
        self.value = os.path.split(reportfile.name)[-1]
        self.filedata = reportfile.read()
        self.datalen = os.path.getsize(reportfile.name)
        reportfile.close()

class AccelCandsDiagnostic(PlotDiagnostic):
    name = "Accelcands list"
    description = "The combined and sifted list of candidates " + \
                  "produced by accelsearch. (A text file)."

    def get_diagnostic(self):
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.accelcands"))

        if len(candlists) != 1:
            raise DiagnosticError("Wrong number of candidate lists found (%d)!" % \
                                    len(candlists))
        accelcandsfile = open(candlists[0], 'r')
        self.value = os.path.split(accelcandsfile.name)[-1]
        self.filedata = accelcandsfile.read()
        self.datalen = os.path.getsize(accelcandsfile.name)
        accelcandsfile.close()

class FFACandsDiagnostic(PlotDiagnostic):
    name = "FFAcands list"
    description = "The combined and sifted list of candidates " + \
                  "produced by ffa. (A text file)."

    def get_diagnostic(self):
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.ffacands"))

        if len(candlists) != 1:
            raise DiagnosticError("Wrong number of FFA candidate lists found (%d)!" % \
                                    len(candlists))
        ffacandsfile = open(candlists[0], 'r')
        self.value = os.path.split(ffacandsfile.name)[-1]
        self.filedata = ffacandsfile.read()
        self.datalen = os.path.getsize(ffacandsfile.name)
        ffacandsfile.close()

class NumFoldedDiagnostic(FloatDiagnostic):
    name = "Num cands folded"
    description = "The number of candidates folded."

    def get_diagnostic(self):
        pfdpngs = glob.glob(os.path.join(self.directory, "*ACCEL*.pfd.png"))
        self.value = len(pfdpngs)

class NumFFAFoldedDiagnostic(FloatDiagnostic):
    name = "Num FFA cands folded"
    description = "The number of FFA candidates folded."

    def get_diagnostic(self):
        pfdpngs = glob.glob(os.path.join(self.directory, "*ffa*.pfd.png"))
        self.value = len(pfdpngs)

class NumCandsDiagnostic(FloatDiagnostic):
    name = "Num cands produced"
    description = "The total number of candidates produced, including " \
                    "those with sigma lower than the folding threshold."

    def get_diagnostic(self):
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.accelcands"))

        if len(candlists) != 1:
            raise DiagnosticError("Wrong number of candidate lists found (%d)!" % \
                                    len(candlists))
        candlist = accelcands.parse_candlist(candlists[0])
        self.value = len(candlist)

class NumFFACandsDiagnostic(FloatDiagnostic):
    name = "Num FFA cands produced"
    description = "The total number of FFA candidates produced, including " \
                    "those with sigma lower than the folding threshold."

    def get_diagnostic(self):
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.ffacands"))

        if len(candlists) != 1:
            raise DiagnosticError("Wrong number of FFA candidate lists found (%d)!" % \
                                    len(candlists))
        candlist = ffacands.parse_candlist(candlists[0])
        self.value = len(candlist)


class MinSigmaFoldedDiagnostic(FloatDiagnostic):
    name = "Min sigma folded"
    description = "The smallest sigma value of all folded candidates "\
                    "from this beam."

    def get_diagnostic(self):
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.accelcands"))
        pfdpngs = [os.path.split(fn)[-1] for fn in \
                    glob.glob(os.path.join(self.directory, "*_ACCEL_*.pfd.png"))]

        if len(candlists) != 1:
            raise DiagnosticError("Wrong number of candidate lists found (%d)!" % \
                                    len(candlists))
        candlist = accelcands.parse_candlist(candlists[0])
        sigmas = []
        for c in candlist:
            base, accel = c.accelfile.split("_ACCEL_")
            pngfn = "%s_Z%s_ACCEL_Cand_%d.pfd.png" % (base, accel, c.candnum)
            if pngfn in pfdpngs:
                sigmas.append(c.sigma)
        
        ffa_candlists = glob.glob(os.path.join(self.directory, "*.ffacands"))
        ffa_pfdpngs = [os.path.split(fn)[-1] for fn in \
                    glob.glob(os.path.join(self.directory, "*_ffa_*.pfd.png"))]
        if len(ffa_candlists) != 1:
            raise DiagnosticError("Wrong number of ffa candidate lists found (%d)!" % \
                                    len(ffa_candlists))
        ffa_candlist = ffacands.parse_candlist(ffa_candlists[0])
        
        #candlist.extend(ffa_candlist)
        pfdpngs.extend(ffa_pfdpngs)
        #sigmas = []
        for c in ffa_candlist:
            #base, accel = c.accelfile.split("_ACCEL_")
            #pngfn = "%s_Z%s_ACCEL_Cand_%d.pfd.png" % (base, accel, c.candnum)
            pngfn = "%s%.2fms_Cand.pfd.png"%(c.ffafile.replace("_cands.ffa","_ffa_"),c.period*1000)
            if pngfn in ffa_pfdpngs:
                sigmas.append(c.sigma)
        if len(pfdpngs) > len(sigmas):
            raise DiagnosticError("Not all *.pfd.png images were found " \
                                    "in candlist! (%d > %d)" % \
                                    (len(pfdpngs), len(sigmas)))
        elif len(pfdpngs) < len(sigmas):
            raise DiagnosticError("Some *.pfd.png image match multiple " \
                                    "entries in candlist! (%d < %d)" % \
                                    (len(pfdpngs), len(sigmas)))

        if not sigmas:
            errormsg = 'No candidates folded.'
            raise DiagnosticNonFatalError(errormsg)

        self.value = min(sigmas)


class NumAboveThreshDiagnostic(FloatDiagnostic):
    name = "Num cands above threshold"
    description = "The number of candidates produced (but not necessarily folded) " \
                    "that are above the desired sigma threshold."

    def get_diagnostic(self):
        # find *.accelcands file
        candlists = glob.glob(os.path.join(self.directory, "*.accelcands"))

        if len(candlists) != 1:
            raise DiagnosticError("Wrong number of candidate lists found (%d)!" % \
                                    len(candlists))
        candlist = accelcands.parse_candlist(candlists[0])
        
        params = get_search_params(self.directory)
        self.value = len([c for c in candlist \
                            if c.sigma >= params['to_prepfold_sigma']])

class NumSPWaterfalledDiagnostic(FloatDiagnostic):
    name = "Num SP cands waterfalled"
    description = "The number of single-pulse candidates waterfalled."

    def get_diagnostic(self):
        spdpngs = glob.glob(os.path.join(self.sp_directory, "*.spd.png"))
        self.value = len(spdpngs)

class MinSigmaWaterfalledDiagnostic(FloatDiagnostic):
    name = "Min sigma waterfalled"
    description = "The smallest sigma value of all waterfalled SP candidates "\
                    "from this beam."

    def get_diagnostic(self):
        max_waterfalled = 100
        groupsfn = os.path.join(self.sp_directory, "groups.txt.gz")

        if not os.path.exists(groupsfn):
            errormsg = 'SP groups file does not exist.'
            raise DiagnosticNonFatalError(errormsg)

        sigmas = []
        for rank in [7, 6, 5, 4, 3]:
            groupsf = gzip.open(groupsfn,"r")
            values = split_parameters(rank, groupsf)
            sigmas += [ val[1] for val in values]
            groupsf.close()

        if not sigmas:
            errormsg = 'No candidates waterfalled.'
            raise DiagnosticNonFatalError(errormsg)

        if len(sigmas) < max_waterfalled:
            self.value = np.min(sigmas)
        else:
            sigmas.sort(reverse=True)
            self.value = sigmas[max_waterfalled-1]

class NumRank0SPGroups(FloatDiagnostic):
    name = "Num rank 0 SP groups"
    description = "The number of rank 0 groups found by SP grouping."

    numrank_re = re.compile(r"Number of rank 0 groups: (?P<numrank>.*)\n")

    def get_diagnostic(self):
        sp_summary_fn = os.path.join(self.sp_directory,"spsummary.txt")

        if not os.path.exists(sp_summary_fn):
            errormsg = 'SP summary file does not exist.'
            raise DiagnosticNonFatalError(errormsg)

        sp_summaryf = open(sp_summary_fn)
        for line in sp_summaryf:
            m = self.numrank_re.search(line)
            if m:
                self.value = float(m.groupdict()['numrank'])
                break
        sp_summaryf.close()

class NumRank2SPGroups(FloatDiagnostic):
    name = "Num rank 2 SP groups"
    description = "The number of rank 2 groups found by SP grouping."

    numrank_re = re.compile(r"Number of rank 2 groups: (?P<numrank>.*)\n")

    def get_diagnostic(self):
        sp_summary_fn = os.path.join(self.sp_directory,"spsummary.txt")

        if not os.path.exists(sp_summary_fn):
            errormsg = 'SP summary file does not exist.'
            raise DiagnosticNonFatalError(errormsg)

        sp_summaryf = open(sp_summary_fn)
        for line in sp_summaryf:
            m = self.numrank_re.search(line)
            if m:
                self.value = float(m.groupdict()['numrank'])
                break
        sp_summaryf.close()

class NumRank3SPGroups(FloatDiagnostic):
    name = "Num rank 3 SP groups"
    description = "The number of rank 3 groups found by SP grouping."

    numrank_re = re.compile(r"Number of rank 3 groups: (?P<numrank>.*)\n")

    def get_diagnostic(self):
        sp_summary_fn = os.path.join(self.sp_directory,"spsummary.txt")

        if not os.path.exists(sp_summary_fn):
            errormsg = 'SP summary file does not exist.'
            raise DiagnosticNonFatalError(errormsg)

        sp_summaryf = open(sp_summary_fn)
        for line in sp_summaryf:
            m = self.numrank_re.search(line)
            if m:
                self.value = float(m.groupdict()['numrank'])
                break
        sp_summaryf.close()

class NumRank4SPGroups(FloatDiagnostic):
    name = "Num rank 4 SP groups"
    description = "The number of rank 4 groups found by SP grouping."

    numrank_re = re.compile(r"Number of rank 4 groups: (?P<numrank>.*)\n")

    def get_diagnostic(self):
        sp_summary_fn = os.path.join(self.sp_directory,"spsummary.txt")

        if not os.path.exists(sp_summary_fn):
            errormsg = 'SP summary file does not exist.'
            raise DiagnosticNonFatalError(errormsg)

        sp_summaryf = open(sp_summary_fn)
        for line in sp_summaryf:
            m = self.numrank_re.search(line)
            if m:
                self.value = float(m.groupdict()['numrank'])
                break
        sp_summaryf.close()

class NumRank5SPGroups(FloatDiagnostic):
    name = "Num rank 5 SP groups"
    description = "The number of rank 5 groups found by SP grouping."

    numrank_re = re.compile(r"Number of rank 5 groups: (?P<numrank>.*)\n")

    def get_diagnostic(self):
        sp_summary_fn = os.path.join(self.sp_directory,"spsummary.txt")

        if not os.path.exists(sp_summary_fn):
            errormsg = 'SP summary file does not exist.'
            raise DiagnosticNonFatalError(errormsg)

        sp_summaryf = open(sp_summary_fn)
        for line in sp_summaryf:
            m = self.numrank_re.search(line)
            if m:
                self.value = float(m.groupdict()['numrank'])
                break
        sp_summaryf.close()

class NumRank6SPGroups(FloatDiagnostic):
    name = "Num rank 6 SP groups"
    description = "The number of rank 6 groups found by SP grouping."

    numrank_re = re.compile(r"Number of rank 6 groups: (?P<numrank>.*)\n")

    def get_diagnostic(self):
        sp_summary_fn = os.path.join(self.sp_directory,"spsummary.txt")

        if not os.path.exists(sp_summary_fn):
            errormsg = 'SP summary file does not exist.'
            raise DiagnosticNonFatalError(errormsg)

        sp_summaryf = open(sp_summary_fn)
        for line in sp_summaryf:
            m = self.numrank_re.search(line)
            if m:
                self.value = float(m.groupdict()['numrank'])
                break
        sp_summaryf.close()

class NumRank7SPGroups(FloatDiagnostic):
    name = "Num rank 7 SP groups"
    description = "The number of rank 7 groups found by SP grouping."

    numrank_re = re.compile(r"Number of rank 7 groups: (?P<numrank>.*)\n")

    def get_diagnostic(self):
        sp_summary_fn = os.path.join(self.sp_directory,"spsummary.txt")

        if not os.path.exists(sp_summary_fn):
            errormsg = 'SP summary file does not exist.'
            raise DiagnosticNonFatalError(errormsg)

        sp_summaryf = open(sp_summary_fn)
        for line in sp_summaryf:
            m = self.numrank_re.search(line)
            if m:
                self.value = float(m.groupdict()['numrank'])
                break
        sp_summaryf.close()

class ZaplistUsed(PlotDiagnostic):
    name = "Zaplist used"
    description = "The list of frequencies and ranges zapped from the " \
                    "power spectrum before searching this beam. (A text file)."

    def get_diagnostic(self):
        # find the *.zaplist file
        zaps = glob.glob(os.path.join(self.directory, '*.zaplist'))

        if len(zaps) != 1:
            raise DiagnosticError("Wrong number of zaplists found (%d)!" % \
                                len(zaps))
        else:
            self.value = os.path.split(zaps[0])[-1]
            self.datalen = os.path.getsize(zaps[0])
            zap_file = open(zaps[0], 'rb')
            self.filedata = zap_file.read()
            zap_file.close()


class PercentZappedBelow1Hz(FloatDiagnostic):
    name = "Percent zapped below 1 Hz"
    description = "The percentage of the power spectrum slower than 1 Hz " \
                    "that was zapped."

    def get_diagnostic(self):
        fctr, width = get_zaplist(self.directory)
        params = get_search_params(self.directory)
        lofreqs = np.clip((fctr - 0.5*width), \
                        1.0/params['sifting_long_period'], 1.0)
        hifreqs = np.clip((fctr + 0.5*width), \
                        1.0/params['sifting_long_period'], 1.0)
        self.value = np.sum(hifreqs-lofreqs) / \
                        (1.0 - 1.0/params['sifting_long_period'])*100


class PercentZappedBelow10Hz(FloatDiagnostic):
    name = "Percent zapped below 10 Hz"
    description = "The percentage of the power spectrum slower than 10 Hz " \
                    "that was zapped."

    def get_diagnostic(self):
        fctr, width = get_zaplist(self.directory)
        params = get_search_params(self.directory)
        lofreqs = np.clip((fctr - 0.5*width), \
                        1.0/params['sifting_long_period'], 10.0)
        hifreqs = np.clip((fctr + 0.5*width), \
                        1.0/params['sifting_long_period'], 10.0)
        self.value = np.sum(hifreqs-lofreqs) / \
                        (10.0 - 1.0/params['sifting_long_period'])*100


class PercentZappedTotal(FloatDiagnostic):
    name = "Percent zapped total"
    description = "The percentage of the power spectrum that was zapped."

    def get_diagnostic(self):
        fctr, width = get_zaplist(self.directory)
        params = get_search_params(self.directory)
        lofreqs = np.clip((fctr - 0.5*width), \
                        1.0/params['sifting_long_period'], \
                        1.0/params['sifting_short_period'])
        hifreqs = np.clip((fctr + 0.5*width), \
                        1.0/params['sifting_long_period'], \
                        1.0/params['sifting_short_period'])
        self.value = np.sum(hifreqs-lofreqs) / \
                        (1.0/params['sifting_short_period'] - \
                        1.0/params['sifting_long_period'])*100

class RadarSamplesUsed(PlotDiagnostic):
    name = "Radar samples used"
    description = "The list of radar time samples clipped from the " \
                    "time series before searching this beam. (A text file)."

    def get_diagnostic(self):
        # find the *_radar_samples.txt file
        samp_files = glob.glob(os.path.join(self.directory, '*_radar_samples.txt'))

        if len(samp_files) != 1:
            raise DiagnosticError("Wrong number of radar sample files found (%d)!" % \
                                len(samp_files))
        else:
            self.value = os.path.split(samp_files[0])[-1]
            self.datalen = os.path.getsize(samp_files[0])
            samp_file = open(samp_files[0], 'rb')
            self.filedata = samp_file.read()
            samp_file.close()

class PercentRadarClipped(FloatDiagnostic):
    name = "Percent radar samples removed"
    description = "The percentage of the time series that is clipped by the radar removal."

    clippcnt_re = re.compile(r"Number of data samples to remove:.*\((?P<masked>.*) %\)")

    def get_diagnostic(self):
        radar_fns = glob.glob(os.path.join(self.directory, '*_radar_samples.txt'))
        radarfile = open(radar_fns[0])

        for line in radarfile:
            m = self.clippcnt_re.search(line)
            if m:
                self.value = float(m.groupdict()['masked'])
                break
        radarfile.close()

class SearchParameters(PlotDiagnostic):
    name = "Search parameters"
    description = "The search parameters used when searching data " \
                    "with the PRESTO pipeline. (A text file)."

    def get_diagnostic(self):
        # find the search_params.txt file
        paramfn = get_search_paramfn(self.directory)
        self.value = os.path.split(paramfn)[-1]
        self.datalen = os.path.getsize(paramfn)
        param_file = open(paramfn, 'rb')
        self.filedata = param_file.read()
        param_file.close()


class CalRemovalSummary(PlotDiagnostic):
    name = "Cal removal summary"
    description = "The command used and other information for " \
                    "the removal of the cal signal. (A text file)."

    def get_diagnostic(self):
        # find the *_calrows.txt file
        calrowsfn = get_calrowsfn(self.directory)
        self.value = os.path.split(calrowsfn)[-1]
        self.datalen = os.path.getsize(calrowsfn)
        calrows_file = open(calrowsfn, 'rb')
        self.filedata = calrows_file.read()
        calrows_file.close()

class NumCalRowsRemoved(FloatDiagnostic):
    name = "Num cal rows removed"
    description = "The number of rows removed from the raw data " \
                    "during removal of the cal signal."

    def get_diagnostic(self):
        # find the cal rows file
        calrowsfn = get_calrowsfn(self.directory)
        calrows = open(calrowsfn,'r')
        num_rows_str = calrows.readlines()[-1]
        calrows.close()
        if num_rows_str.startswith('Total number of rows removed:'):
            self.value = int(num_rows_str.lstrip('Total number of rows removed:'))
        else:
            raise DiagnosticError("Last row of Cal rows file (%s) is not " \
                                   "the number of rows removed!" % calrowsfn)

class SigmaThreshold(FloatDiagnostic):
    name = "Sigma threshold"
    description = "The sigma threshold used for determining which " \
                    "candidates potentially get folded."

    def get_diagnostic(self):
        # find the search parameters
        params = get_search_params(self.directory)
        self.value = params['to_prepfold_sigma']


class MaxCandsToFold(FloatDiagnostic):
    name = "Max cands allowed to fold"
    description = "The maximum number of candidates that are " \
                    "allowed to be folded."
    
    def get_diagnostic(self):
        # find the search parameters
        params = get_search_params(self.directory)
        self.value = params['max_accel_cands_to_fold']+params['max_ffa_cands_to_fold']


def get_search_paramfn(dir):
    # find the search_params.txt file
    paramfn = os.path.join(dir, 'search_params.txt')
    if not os.path.exists(paramfn):
        raise DiagnosticError("Search parameter file doesn't exist!")
    return paramfn


def get_search_params(dir):
    paramfn = get_search_paramfn(dir)
    tmp, params = {}, {}
    execfile(paramfn, tmp, params)
    return params


def get_zaplistfn(dir):
    # find the *.zaplist file
    zaps = glob.glob(os.path.join(dir, '*.zaplist'))

    if len(zaps) != 1:
        raise DiagnosticError("Wrong number of zaplists found (%d)!" % \
                            len(zaps))
    return zaps[0]


def get_zaplist(dir):
    zapfn = get_zaplistfn(dir)
    fctr, width = np.loadtxt(zapfn, usecols=(-2,-1), unpack=True)
    return fctr, width


def get_calrowsfn(dir):
    calrows_fns = glob.glob(os.path.join(dir, '*_calrows.txt'))
    if len(calrows_fns) != 1:
        raise DiagnosticError("Wrong number of cal rows files found (%d)!" % \
                            len(calrows_fns))
    return calrows_fns[0]


def find_in_tarballs(dir, matchfunc):
    """Find all tarballs in the given directory and search
        for a filename (inside the tarballs) for which
        matchfunc(FILENAME) returns True.

        Inputs:
            dir: directory in which to look for tarballs.
                    (i.e. *.tar, *.tgz, *.tar.gz)
            matchfunc: function that returns True when called with 
                    the desired filename as an argument.

        Output:
            t: the tarfile object containing the matching file (already opened)
            f: matching file-like object (already opened)

        NOTE: 't' and 'f' output by this function should be closed manually.
    """
    tar_suffixes = ['*.tar.gz', '*.tgz', '*.tar']

    # Find the tarball containing a matching file
    for suffix in tar_suffixes:
        tars = glob.glob(os.path.join(dir, suffix))
        for tar in tars:
            t = tarfile.open(tar, mode='r')
            for fn in t.getnames():
                if matchfunc(fn):
                    f = t.extractfile(fn)
                    return t, f
            t.close()
    raise DiagnosticError("Could not find matching file!")


class DiagnosticError(upload.UploadNonFatalError):
    """Error to throw when a diagnostic-specific problem 
        is encountered.
    """
    pass

class DiagnosticNonFatalError(pipeline_utils.PipelineError):
    pass


def get_diagnostics(obsname, beamnum, obstype, versionnum, pdm_dir, sp_dir):
    """Get diagnostic to common DB.
        
        Inputs:
            obsname: Observation name in the format:
                        {Project ID}.{Source name}.{MJD}.{Sequence number}
            beamnum: ALFA beam number (an integer between 0 and 7).
            obstype: Type of data (either 'wapp' or 'mock').
            versionnum: A combination of the githash values from 
                        PRESTO and from the pipeline. 
            pdm_dir: The directory containing the periodicity results from the pipeline.
            sp_dir: The directory containing the single-pulse results from the pipeline.

        Outputs:
            diagnostics: List of diagnostic objects.
    """
    if not 0 <= beamnum <= 7:
        raise DiagnosticError("Beam number must be between 0 and 7, inclusive!")
    
    diags = []
    # Loop over diagnostics, adding missing values to the DB
    for diagnostic_type in DIAGNOSTIC_TYPES:
        try:
            d = diagnostic_type(obsname, beamnum, obstype, \
                            versionnum, pdm_dir, sp_dir)
        except DiagnosticNonFatalError:
            continue

        except Exception:
            raise DiagnosticError("Could not create %s object for " \
                                    "observation: %s (beam: %d)" % \
                                    (diagnostic_type.__name__, obsname, beamnum))
        diags.append(d)
    return diags

# Define a list of diagnostics to apply
DIAGNOSTIC_TYPES = [RFIPercentageDiagnostic,
                    RFIPlotDiagnostic,
                    PeriodicitySummaryPlotDiagnostic,
                    FFASummaryPlotDiagnostic,
                    PeriodicityRejectsPlotDiagnostic,
                    FFARejectsPlotDiagnostic,
                    SiftingSummaryDiagnostic,
                    FFASiftingSummaryDiagnostic,
                    SiftingReportDiagnostic,
                    FFASiftingReportDiagnostic,
                    AccelCandsDiagnostic,
                    FFACandsDiagnostic,
                    NumFoldedDiagnostic,
                    NumFFAFoldedDiagnostic,
                    NumCandsDiagnostic,
                    NumFFACandsDiagnostic,
                    MinSigmaFoldedDiagnostic,
                    NumAboveThreshDiagnostic,
                    ZaplistUsed,
                    SearchParameters,
                    SigmaThreshold,
                    MaxCandsToFold,
                    PercentZappedTotal,
                    PercentZappedBelow10Hz,
                    PercentZappedBelow1Hz,
                    CalRemovalSummary,
                    NumCalRowsRemoved,
                    RadarSamplesUsed,
                    PercentRadarClipped,
                    NumSPWaterfalledDiagnostic,
                    MinSigmaWaterfalledDiagnostic,
                    NumRank0SPGroups,
                    NumRank2SPGroups,
                    NumRank3SPGroups,
                    NumRank4SPGroups,
                    NumRank5SPGroups,
                    NumRank6SPGroups,
                    #NumRank7SPGroups
                   ]


def main():
    db = database.Database('default', autocommit=False)
    try:
        diags = get_diagnostics(options.obsname, options.beamnum, \
                                options.versionnum, options.directory)
        for d in diags:
            d.upload(db)
    except:
        print "Rolling back..."
        db.rollback()
        raise
    else:
        db.commit()
    finally:
        db.close()


if __name__ == '__main__':
    parser = optparse.OptionParser(prog="diagnostics.py", \
                version="v0.8 (by Patrick Lazarus, Dec. 20, 2010)", \
                description="Upload diagnostics from a beam of PALFA " \
                            "data analysed using the pipeline2.0.")
    parser.add_option('--obsname', dest='obsname', \
                        help="The observation name is a combination of " \
                             "Project ID, Source name, MJD, and sequence number " \
                             "in the following format 'projid.srcname.mjd.seqnum'.")
    parser.add_option('--beamnum', dest='beamnum', type='int', \
                        help="Beam number (0-7).")
    parser.add_option('--versionnum', dest='versionnum', \
                        help="Version number is a combination of the PRESTO " \
                             "repository's git hash and the Pipeline2.0 " \
                             "repository's git has in the following format " \
                             "PRESTO:prestohash;pipeline:prestohash")
    parser.add_option('-d', '--directory', dest='directory',
                        help="Directory containing results from processing. " \
                             "Diagnostic information will be derived from the " \
                             "contents of this directory.")
    options, args = parser.parse_args()
    main()
