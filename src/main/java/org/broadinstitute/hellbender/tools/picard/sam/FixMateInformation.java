package org.broadinstitute.hellbender.tools.picard.sam;

import htsjdk.samtools.*;
import htsjdk.samtools.util.*;
import org.broadinstitute.hellbender.cmdline.*;
import org.broadinstitute.hellbender.cmdline.programgroups.ReadProgramGroup;
import org.broadinstitute.hellbender.exceptions.UserException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Class to fix mate pair information for all reads in a SAM file.  Will run in fairly limited
 * memory unless there are lots of mate pairs that are far apart from each other in the file.
 *
 * @author Tim Fennell
 */
@CommandLineProgramProperties(
        usage = "Ensure that all mate-pair information is in sync between each read " +
                "and its mate pair.  If no OUTPUT file is supplied then the output is written to a temporary file " +
                "and then copied over the INPUT file.  Reads marked with the secondary alignment flag are written " +
                "to the output file unchanged.",
        usageShort = "Ensure that all mate-pair information is in sync between each read and its mate pair",
        programGroup = ReadProgramGroup.class
)
public final class FixMateInformation extends PicardCommandLineProgram {

    @Argument(shortName = StandardArgumentDefinitions.INPUT_SHORT_NAME, doc = "The input file to fix.")
    public List<File> INPUT;

    @Argument(shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, optional = true,
            doc = "The output file to write to. If no output file is supplied, the input file is overwritten.")
    public File OUTPUT;

    @Argument(shortName = StandardArgumentDefinitions.SORT_ORDER_SHORT_NAME, optional = true,
            doc = "Optional sort order if the OUTPUT file should be sorted differently than the INPUT file.")
    public SAMFileHeader.SortOrder SORT_ORDER;

    @Argument(doc = "If true, assume that the input file is queryname sorted, even if the header says otherwise.",
            shortName = StandardArgumentDefinitions.ASSUME_SORTED_SHORT_NAME)
    public boolean ASSUME_SORTED = false;

    @Argument(shortName = "MC", optional = true, doc = "Adds the mate CIGAR tag (MC) if true, does not if false.")
    public Boolean ADD_MATE_CIGAR = true;

    private static final Log log = Log.getInstance(FixMateInformation.class);

    protected SAMFileWriter out;

    protected Object doWork() {
        // Open up the input
        boolean allQueryNameSorted = true;
        final List<SamReader> readers = new ArrayList<>();
        for (final File f : INPUT) {
            IOUtil.assertFileIsReadable(f);
            final SamReader reader = SamReaderFactory.makeDefault().referenceSequence(REFERENCE_SEQUENCE).open(f);
            readers.add(reader);
            if (reader.getFileHeader().getSortOrder() != SAMFileHeader.SortOrder.queryname) allQueryNameSorted = false;
        }

        // Decide where to write the fixed file - into the specified output file
        // or into a temporary file that will overwrite the INPUT file eventually
        if (OUTPUT != null) OUTPUT = OUTPUT.getAbsoluteFile();
        final boolean differentOutputSpecified = OUTPUT != null;

        if (differentOutputSpecified) {
            IOUtil.assertFileIsWritable(OUTPUT);
        } else if (INPUT.size() != 1) {
            throw new UserException("Must specify either an explicit OUTPUT file or a single INPUT file to be overridden.");
        } else {
            final File soleInput = INPUT.get(0).getAbsoluteFile();
            final File dir = soleInput.getParentFile().getAbsoluteFile();
            try {
                IOUtil.assertFileIsWritable(soleInput);
                IOUtil.assertDirectoryIsWritable(dir);
                OUTPUT = File.createTempFile(soleInput.getName() + ".being_fixed.", BamFileIoUtils.BAM_FILE_EXTENSION, dir);
            } catch (final IOException ioe) {
                throw new RuntimeIOException("Could not create tmp file in " + dir.getAbsolutePath());
            }
        }

        // Get the input records merged and sorted by query name as needed
        final PeekableIterator<SAMRecord> iterator;
        final SAMFileHeader header;

        {
            // Deal with merging if necessary
            final Iterator<SAMRecord> tmp;
            if (INPUT.size() > 1) {
                final List<SAMFileHeader> headers = new ArrayList<>(readers.size());
                for (final SamReader reader : readers) {
                    headers.add(reader.getFileHeader());
                }
                final SAMFileHeader.SortOrder sortOrder = (allQueryNameSorted ? SAMFileHeader.SortOrder.queryname : SAMFileHeader.SortOrder.unsorted);
                final SamFileHeaderMerger merger = new SamFileHeaderMerger(sortOrder, headers, false);
                tmp = new MergingSamRecordIterator(merger, readers, false);
                header = merger.getMergedHeader();
            } else {
                tmp = readers.get(0).iterator();
                header = readers.get(0).getFileHeader();
            }

            // And now deal with re-sorting if necessary
            if (ASSUME_SORTED || allQueryNameSorted) {
                iterator = new SamPairUtil.SetMateInfoIterator(new PeekableIterator<>(tmp), ADD_MATE_CIGAR);
            } else {
                log.info("Sorting input into queryname order.");
                final SortingCollection<SAMRecord> sorter = SortingCollection.newInstance(SAMRecord.class,
                        new BAMRecordCodec(header),
                        new SAMRecordQueryNameComparator(),
                        MAX_RECORDS_IN_RAM,
                        TMP_DIR);
                while (tmp.hasNext()) {
                    sorter.add(tmp.next());

                }

                iterator = new SamPairUtil.SetMateInfoIterator(new PeekableIterator<SAMRecord>(sorter.iterator()) {
                    @Override
                    public void close() {
                        super.close();
                        sorter.cleanup();
                    }
                }, ADD_MATE_CIGAR);
                log.info("Sorting by queryname complete.");
            }

            // Deal with the various sorting complications
            final SAMFileHeader.SortOrder outputSortOrder = SORT_ORDER == null ? readers.get(0).getFileHeader().getSortOrder() : SORT_ORDER;
            log.info("Output will be sorted by " + outputSortOrder);
            header.setSortOrder(outputSortOrder);
        }

        if (CREATE_INDEX && header.getSortOrder() != SAMFileHeader.SortOrder.coordinate) {
            throw new UserException("Can't CREATE_INDEX unless sort order is coordinate");
        }

        createSamFileWriter(header);

        log.info("Traversing query name sorted records and fixing up mate pair information.");
        final ProgressLogger progress = new ProgressLogger(log);
        while (iterator.hasNext()) {
            final SAMRecord record = iterator.next();
            out.addAlignment(record);
            progress.record(record);
        }
        iterator.close();

        if (header.getSortOrder() == SAMFileHeader.SortOrder.queryname) {
            log.info("Closing output file.");
        } else {
            log.info("Finished processing reads; re-sorting output file.");
        }
        closeWriter();

        // Lastly if we're fixing in place, swap the files
        // TODO throw appropriate exceptions instead of writing to log.error and returning
        if (!differentOutputSpecified) {
            log.info("Replacing input file with fixed file.");

            final File soleInput = INPUT.get(0).getAbsoluteFile();
            final File old = new File(soleInput.getParentFile(), soleInput.getName() + ".old");
            if (!old.exists() && soleInput.renameTo(old)) {
                if (OUTPUT.renameTo(soleInput)) {

                    if (!old.delete()) {
                        log.warn("Could not delete old file: " + old.getAbsolutePath());
                        return null;
                    }

                    if (CREATE_INDEX) {
                        final File newIndex = new File(OUTPUT.getParent(),
                                OUTPUT.getName().substring(0, OUTPUT.getName().length() - 4) + ".bai");
                        final File oldIndex = new File(soleInput.getParent(),
                                soleInput.getName().substring(0, soleInput.getName().length() - 4) + ".bai");

                        if (!newIndex.renameTo(oldIndex)) {
                            log.warn("Could not overwrite index file: " + oldIndex.getAbsolutePath());
                        }
                    }

                } else {
                    log.error("Could not move new file to " + soleInput.getAbsolutePath());
                    log.error("Input file preserved as: " + old.getAbsolutePath());
                    log.error("New file preserved as: " + OUTPUT.getAbsolutePath());
                    return null;
                }
            } else {
                log.error("Could not move input file out of the way: " + soleInput.getAbsolutePath());

                if (!OUTPUT.delete()) {
                    log.error("Could not delete temporary file: " + OUTPUT.getAbsolutePath());
                }

                return null;
            }
        }

        CloserUtil.close(readers);
        return null;
    }

    protected void createSamFileWriter(final SAMFileHeader header) {
        out = new SAMFileWriterFactory().makeSAMOrBAMWriter(header,
                header.getSortOrder() == SAMFileHeader.SortOrder.queryname, OUTPUT);

    }

    protected void writeAlignment(final SAMRecord sam) {
        out.addAlignment(sam);
    }

    protected void closeWriter() {
        out.close();
    }

}
