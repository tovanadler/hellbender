package org.broadinstitute.hellbender.tools.recalibration;

import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.tools.recalibration.covariates.ContextCovariate;
import org.broadinstitute.hellbender.tools.recalibration.covariates.Covariate;
import org.broadinstitute.hellbender.utils.clipping.ClippingRepresentation;
import org.broadinstitute.hellbender.utils.clipping.ReadClipper;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.MutableRead;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ContextCovariateUnitTest {
    ContextCovariate covariate;
    RecalibrationArgumentCollection RAC;

    @BeforeClass
    public void init() {
        RAC = new RecalibrationArgumentCollection();
        covariate = new ContextCovariate();
        covariate.initialize(RAC);
    }

    @BeforeMethod
    public void initCache() {
        ReadCovariates.clearKeysCache();
    }

    @Test
    public void testSimpleContexts() {
        final SAMFileHeader header = ArtificialReadUtils.createArtificialSamHeader();
        MutableRead read = ArtificialReadUtils.createRandomRead(header, 1000);
        MutableRead clippedRead = ReadClipper.clipLowQualEnds(read, RAC.LOW_QUAL_TAIL, ClippingRepresentation.WRITE_NS);
        ReadCovariates readCovariates = new ReadCovariates(read.getLength(), 1);
        covariate.recordValues(read, header, readCovariates);

        verifyCovariateArray(readCovariates.getMismatchesKeySet(), RAC.MISMATCHES_CONTEXT_SIZE, clippedRead, covariate);
        verifyCovariateArray(readCovariates.getInsertionsKeySet(), RAC.INDELS_CONTEXT_SIZE, clippedRead, covariate);
        verifyCovariateArray(readCovariates.getDeletionsKeySet(),  RAC.INDELS_CONTEXT_SIZE,  clippedRead, covariate);
    }

    public static void verifyCovariateArray(int[][] values, int contextSize, MutableRead read, Covariate contextCovariate) {
        for (int i = 0; i < values.length; i++)
            Assert.assertEquals(contextCovariate.formatKey(values[i][0]), expectedContext(read, i, contextSize));

    }

    public static String expectedContext( MutableRead read, int offset, int contextSize ) {
        final String bases = stringFrom(read.getBases());
        String expectedContext = null;
        if (offset - contextSize + 1 >= 0) {
            String context = bases.substring(offset - contextSize + 1, offset + 1);
            if (!context.contains("N"))
                expectedContext = context;
        }
        return expectedContext;
    }

    private static String stringFrom(byte[] array) {
        String s = "";
        for (byte value : array)
            s += (char) value;
        return s;
    }

}
