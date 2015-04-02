package org.broadinstitute.hellbender.tools.recalibration;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import org.broadinstitute.hellbender.tools.recalibration.covariates.*;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.MutableRead;
import org.broadinstitute.hellbender.utils.read.ReadUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Random;

public class ReadCovariatesUnitTest {

    @BeforeMethod
    public void init() {
        ReadCovariates.clearKeysCache();
    }

    @Test(enabled = false)
    public void testCovariateGeneration() {
        final RecalibrationArgumentCollection RAC = new RecalibrationArgumentCollection();
        final String RGID = "id";

        ReadGroupCovariate rgCov = new ReadGroupCovariate();
        QualityScoreCovariate qsCov = new QualityScoreCovariate();
        ContextCovariate coCov = new ContextCovariate();
        CycleCovariate cyCov = new CycleCovariate();

        rgCov.initialize(RAC);
        qsCov.initialize(RAC);
        coCov.initialize(RAC);
        cyCov.initialize(RAC);

        Covariate[] requestedCovariates = new Covariate[4];
        requestedCovariates[0] = rgCov;
        requestedCovariates[1] = qsCov;
        requestedCovariates[2] = coCov;
        requestedCovariates[3] = cyCov;

        final int NUM_READS = 100;
        final Random rnd = Utils.getRandomGenerator();

        final String[] readGroups = {"RG1", "RG2", "RGbla"};
        for (int idx = 0; idx < NUM_READS; idx++) {
            for (final String readGroupID : readGroups) {
                final SAMReadGroupRecord readGroupRecord = new SAMReadGroupRecord(readGroupID);
                readGroupRecord.setPlatform("illumina");
                final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeaderWithReadGroup(readGroupRecord);

                final int length = 10 + rnd.nextInt(100); // random read length, at least 10 bp long
                final MutableRead read = ArtificialReadUtils.createRandomRead(header, length, false);
                read.setIsNegativeStrand(rnd.nextBoolean());
                read.setReadGroup(readGroupID);

                final byte[] mQuals = read.getBaseQualities();
                final byte[] iQuals = ReadUtils.getBaseInsertionQualities(read);
                final byte[] dQuals = ReadUtils.getBaseDeletionQualities(read);
                ReadCovariates rc = RecalUtils.computeCovariates(read, header, requestedCovariates);

                // check that the length is correct
                Assert.assertEquals(rc.getMismatchesKeySet().length, length);
                Assert.assertEquals(rc.getInsertionsKeySet().length, length);
                Assert.assertEquals(rc.getDeletionsKeySet().length, length);

                for (int i = 0; i < length; i++) {
                    // check that read group is always the same
                    Assert.assertEquals(rgCov.formatKey(rc.getMismatchesKeySet(i)[0]), readGroupID);
                    Assert.assertEquals(rgCov.formatKey(rc.getInsertionsKeySet(i)[0]), readGroupID);
                    Assert.assertEquals(rgCov.formatKey(rc.getDeletionsKeySet(i)[0]), readGroupID);

                    // check quality score
                    Assert.assertEquals(qsCov.formatKey(rc.getMismatchesKeySet(i)[1]), "" + mQuals[i]);
                    Assert.assertEquals(qsCov.formatKey(rc.getInsertionsKeySet(i)[1]), "" + iQuals[i]);
                    Assert.assertEquals(qsCov.formatKey(rc.getDeletionsKeySet(i)[1]), "" + dQuals[i]);

                    // check context
                    Assert.assertEquals(coCov.formatKey(rc.getMismatchesKeySet(i)[2]), ContextCovariateUnitTest.expectedContext(read, i, RAC.MISMATCHES_CONTEXT_SIZE));
                    Assert.assertEquals(coCov.formatKey(rc.getInsertionsKeySet(i)[2]), ContextCovariateUnitTest.expectedContext(read, i, RAC.INDELS_CONTEXT_SIZE));
                    Assert.assertEquals(coCov.formatKey(rc.getDeletionsKeySet(i)[2]), ContextCovariateUnitTest.expectedContext(read, i, RAC.INDELS_CONTEXT_SIZE));

                    // check cycle
                    Assert.assertEquals(cyCov.formatKey(rc.getMismatchesKeySet(i)[3]), "" + (i + 1));
                    Assert.assertEquals(cyCov.formatKey(rc.getInsertionsKeySet(i)[3]), "" + (i + 1));
                    Assert.assertEquals(cyCov.formatKey(rc.getDeletionsKeySet(i)[3]), "" + (i + 1));
                }

            }

        }

    }

}
