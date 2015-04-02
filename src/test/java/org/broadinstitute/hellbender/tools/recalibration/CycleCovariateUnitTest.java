package org.broadinstitute.hellbender.tools.recalibration;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMReadGroupRecord;
import org.broadinstitute.hellbender.tools.recalibration.covariates.CycleCovariate;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.MutableRead;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CycleCovariateUnitTest {
    CycleCovariate covariate;
    RecalibrationArgumentCollection RAC;
    SAMReadGroupRecord illuminaReadGroup;

    @BeforeClass
    public void init() {
        RAC = new RecalibrationArgumentCollection();
        covariate = new CycleCovariate();
        covariate.initialize(RAC);
        illuminaReadGroup = new SAMReadGroupRecord("MY.ID");
        illuminaReadGroup.setPlatform("illumina");
    }

    @BeforeMethod
    public void initCache() {
        ReadCovariates.clearKeysCache();
    }

    @Test
    public void testSimpleCycles() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeaderWithReadGroup(illuminaReadGroup);

        short readLength = 10;
        MutableRead read = ArtificialReadUtils.createRandomRead(header, readLength);
        read.setIsPaired(true);
        read.setReadGroup(illuminaReadGroup.getReadGroupId());

        ReadCovariates readCovariates = new ReadCovariates(read.getLength(), 1);
        covariate.recordValues(read, header, readCovariates);
        verifyCovariateArray(readCovariates.getMismatchesKeySet(), 1, (short) 1);

        read.setIsNegativeStrand(true);
        covariate.recordValues(read, header, readCovariates);
        verifyCovariateArray(readCovariates.getMismatchesKeySet(), readLength, -1);

        read.setIsSecondOfPair(true);
        covariate.recordValues(read, header, readCovariates);
        verifyCovariateArray(readCovariates.getMismatchesKeySet(), -readLength, 1);

        read.setIsNegativeStrand(false);
        covariate.recordValues(read, header, readCovariates);
        verifyCovariateArray(readCovariates.getMismatchesKeySet(), -1, -1);
    }

    private void verifyCovariateArray(int[][] values, int init, int increment) {
        for (short i = 0; i < values.length; i++) {
            short actual = Short.decode(covariate.formatKey(values[i][0]));
            int expected = init + (increment * i);
            Assert.assertEquals(actual, expected);
        }
    }

    @Test(expectedExceptions={UserException.class})
    public void testMoreThanMaxCycleFails() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeaderWithReadGroup(illuminaReadGroup);

        int readLength = RAC.MAXIMUM_CYCLE_VALUE + 1;
        MutableRead read = ArtificialReadUtils.createRandomRead(readLength);
        read.setIsPaired(true);
        read.setReadGroup(illuminaReadGroup.getReadGroupId());

        ReadCovariates readCovariates = new ReadCovariates(read.getLength(), 1);
        covariate.recordValues(read, header, readCovariates);
    }

    @Test
    public void testMaxCyclePasses() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeaderWithReadGroup(illuminaReadGroup);

        int readLength = RAC.MAXIMUM_CYCLE_VALUE;
        MutableRead read = ArtificialReadUtils.createRandomRead(readLength);
        read.setIsPaired(true);
        read.setReadGroup(illuminaReadGroup.getReadGroupId());

        ReadCovariates readCovariates = new ReadCovariates(read.getLength(), 1);
        covariate.recordValues(read, header, readCovariates);
    }
}
