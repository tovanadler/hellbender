package org.broadinstitute.hellbender.utils.read;


import com.google.api.services.genomics.model.LinearAlignment;
import com.google.api.services.genomics.model.Position;
import htsjdk.samtools.Cigar;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SAMUtils;
import htsjdk.samtools.TextCigarCodec;
import htsjdk.samtools.util.Locatable;
import htsjdk.samtools.util.StringUtil;
import org.broadinstitute.hellbender.exceptions.GATKException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public final class GoogleGenomicsReadToReadAdapter implements MutableRead {

    private final com.google.api.services.genomics.model.Read genomicsRead;

    public GoogleGenomicsReadToReadAdapter( final com.google.api.services.genomics.model.Read genomicsRead ) {
        this.genomicsRead = genomicsRead;
    }

    private static <T> T assertFieldValueNotNull( final T fieldValue, final String fieldName ) {
        if ( fieldValue == null ) {
            throw new GATKException.MissingReadField(fieldName);
        }
        return fieldValue;
    }

    private void assertHasAlignment() {
        assertFieldValueNotNull(genomicsRead.getAlignment(), "alignment");
    }

    private void assertHasPosition() {
        assertHasAlignment();
        assertFieldValueNotNull(genomicsRead.getAlignment().getPosition(), "position");
    }

    @Override
    public String getContig() {
        if ( isUnmapped() ) {
            throw new GATKException.MissingReadField("contig", "Cannot get the contig of an unmapped read");
        }

        return genomicsRead.getAlignment().getPosition().getReferenceName();
    }

    @Override
    public int getStart() {
        if ( isUnmapped() ) {
            throw new GATKException.MissingReadField("start", "Cannot get the start position of an unmapped read");
        }

        // Convert from 0-based to 1-based start position
        return genomicsRead.getAlignment().getPosition().getPosition().intValue() + 1;
    }

    @Override
    public int getEnd() {
        if ( isUnmapped() ) {
            throw new GATKException.MissingReadField("end", "Cannot get the end position of an unmapped read");
        }

        // Position in genomicsRead is 0-based, so add getCigar().getReferenceLength() to it,
        // not getCigar().getReferenceLength() - 1, in order to convert to a 1-based end position.
        return genomicsRead.getAlignment().getPosition().getPosition().intValue() + getCigar().getReferenceLength();
    }

    @Override
    public String getName() {
        return assertFieldValueNotNull(genomicsRead.getFragmentName(), "name");
    }

    @Override
    public int getLength() {
        return getBases().length;
    }

    @Override
    public byte[] getBases() {
        final String basesString = assertFieldValueNotNull(genomicsRead.getAlignedSequence(), "bases");

        if ( basesString.equals(SAMRecord.NULL_SEQUENCE_STRING) ) {
            return new byte[0];
        }

        final byte[] bases = StringUtil.stringToBytes(basesString);
        ReadUtils.normalizeBases(bases);

        return bases;
    }

    @Override
    public byte[] getBaseQualities() {
        final List<Integer> baseQualities = assertFieldValueNotNull(genomicsRead.getAlignedQuality(), "base qualities");

        byte[] convertedBaseQualities = new byte[baseQualities.size()];
        for ( int i = 0; i < baseQualities.size(); ++i ) {
            if ( baseQualities.get(i) < Byte.MIN_VALUE || baseQualities.get(i) > Byte.MAX_VALUE ) {
                throw new GATKException("Quality score value " + baseQualities.get(i) + " not convertible to byte");
            }
            convertedBaseQualities[i] = baseQualities.get(i).byteValue();
        }

        return convertedBaseQualities;
    }

    @Override
    public int getUnclippedStart() {
        return SAMUtils.getUnclippedStart(getStart(), getCigar());
    }

    @Override
    public int getUnclippedEnd() {
        return SAMUtils.getUnclippedEnd(getEnd(), getCigar());
    }

    @Override
    public String getMateContig() {
        if ( mateIsUnmapped() ) {
            throw new GATKException.MissingReadField("mate contig", "Cannot get the contig of an unmapped mate");
        }

        return genomicsRead.getNextMatePosition().getReferenceName();
    }

    @Override
    public int getMateStart() {
        if ( mateIsUnmapped() ) {
            throw new GATKException.MissingReadField("mate start", "Cannot get the start position of an unmapped mate");
        }

        return genomicsRead.getNextMatePosition().getPosition().intValue() + 1;
    }

    @Override
    public int getFragmentLength() {
        return genomicsRead.getFragmentLength() != null ? genomicsRead.getFragmentLength() : 0;
    }

    @Override
    public int getMappingQuality() {
        assertHasAlignment();
        return assertFieldValueNotNull(genomicsRead.getAlignment().getMappingQuality(), "mapping quality");
    }

    @Override
    public Cigar getCigar() {
        assertHasAlignment();
        assertFieldValueNotNull(genomicsRead.getAlignment().getCigar(), "cigar");

        return CigarConverter.convertCigarUnitListToSAMCigar(genomicsRead.getAlignment().getCigar());
    }

    @Override
    public String getReadGroup() {
        return genomicsRead.getReadGroupId() != null ? genomicsRead.getReadGroupId() : null;
    }

    @Override
    public int getNumberOfReadsInFragment() {
        return assertFieldValueNotNull(genomicsRead.getNumberReads(), "number of reads in fragment");
    }

    @Override
    public int getReadNumber() {
        // Convert from 0-based to 1-based numbering
        return assertFieldValueNotNull(genomicsRead.getReadNumber(), "read number") + 1;
    }

    @Override
    public boolean isPaired() {
        assertFieldValueNotNull(genomicsRead.getNumberReads(), "number of reads");
        return genomicsRead.getNumberReads() == 2;
    }

    @Override
    public boolean isProperlyPaired() {
        assertFieldValueNotNull(genomicsRead.getProperPlacement(), "proper placement");
        return isPaired() && genomicsRead.getProperPlacement();
    }

    @Override
    public boolean isUnmapped() {
        return genomicsRead.getAlignment() == null ||
               positionIsUnmapped(genomicsRead.getAlignment().getPosition());
    }

    @Override
    public boolean mateIsUnmapped() {
        // TODO: require isPaired() here?
        return positionIsUnmapped(genomicsRead.getNextMatePosition());
    }

    private boolean positionIsUnmapped( final Position position ) {
        return position == null ||
               position.getReferenceName() == null || position.getReferenceName().equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) ||
               position.getPosition() == null || position.getPosition() == SAMRecord.NO_ALIGNMENT_START;
    }

    @Override
    public boolean isReverseStrand() {
        assertHasPosition();
        return assertFieldValueNotNull(genomicsRead.getAlignment().getPosition().getReverseStrand(), "strand");
    }

    @Override
    public boolean mateIsReverseStrand() {
        // TODO: require isPaired() here?
        final Position matePosition = assertFieldValueNotNull(genomicsRead.getNextMatePosition(), "mate position");
        return assertFieldValueNotNull(matePosition.getReverseStrand(), "mate strand");
    }

    @Override
    public boolean isFirstOfPair() {
        final int readNumber = assertFieldValueNotNull(genomicsRead.getReadNumber(), "read number");
        return isPaired() && readNumber == 0;
    }

    @Override
    public boolean isSecondOfPair() {
        final int readNumber = assertFieldValueNotNull(genomicsRead.getReadNumber(), "read number");
        return isPaired() && readNumber == 1;
    }

    @Override
    public boolean isNonPrimaryAlignment() {
        return assertFieldValueNotNull(genomicsRead.getSecondaryAlignment(), "secondary alignment");
    }

    @Override
    public boolean isSupplementaryAlignment() {
        return assertFieldValueNotNull(genomicsRead.getSupplementaryAlignment(), "supplementary alignment");
    }

    @Override
    public boolean failsVendorQualityCheck() {
        return assertFieldValueNotNull(genomicsRead.getFailedVendorQualityChecks(), "failed vendor quality checks");
    }

    @Override
    public boolean isDuplicate() {
        return assertFieldValueNotNull(genomicsRead.getDuplicateFragment(), "duplicate fragment");
    }

    @Override
    public boolean hasAttribute( String attributeName ) {
        return genomicsRead.getInfo() != null && genomicsRead.getInfo().containsKey(attributeName);
    }

    @Override
    public Integer getAttributeAsInteger( String attributeName ) {
        if ( genomicsRead.getInfo() == null ) {
            return null;
        }

        final List<String> rawValue = genomicsRead.getInfo().get(attributeName);
        if ( rawValue == null || rawValue.isEmpty() || rawValue.get(0) == null ) {
            return null;
        }

        try {
            // Assume (for now) that integer attributes are encoded as a single String in the first position of the List
            return Integer.parseInt(rawValue.get(0));
        }
        catch ( NumberFormatException e ) {
            throw new GATKException("Attribute " + attributeName + " not of integer type", e);
        }
    }

    @Override
    public String getAttributeAsString( String attributeName ) {
        if ( genomicsRead.getInfo() == null ) {
            return null;
        }

        final List<String> rawValue = genomicsRead.getInfo().get(attributeName);
        if ( rawValue == null || rawValue.isEmpty() ) {
            return null;
        }

        // Assume (for now) that String attributes are encoded as a single String in the first position of the List
        return rawValue.get(0);
    }

    @Override
    public byte[] getAttributeAsByteArray( String attributeName ) {
        if ( genomicsRead.getInfo() == null ) {
            return null;
        }

        final List<String> rawValue = genomicsRead.getInfo().get(attributeName);
        if ( rawValue == null || rawValue.isEmpty() || rawValue.get(0) == null ) {
            return null;
        }

        // Assume (for now) that byte array attributes are encoded as a single String in the first position of the List
        return rawValue.get(0).getBytes();
    }

    @Override
    public Read copy() {
        // clone() actually makes a deep copy of all fields here (via GenericData.clone())
        return new GoogleGenomicsReadToReadAdapter(genomicsRead.clone());
    }

    @Override
    public void setName( String name ) {
        genomicsRead.setFragmentName(name);
    }

    @Override
    public void setBases( byte[] bases ) {
        genomicsRead.setAlignedSequence(StringUtil.bytesToString(bases));
    }

    @Override
    public void setBaseQualities( byte[] baseQualities ) {
        final List<Integer> convertedBaseQualities = new ArrayList<Integer>(baseQualities.length);
        for ( byte b : baseQualities ) {
            convertedBaseQualities.add((int)b);
        }

        genomicsRead.setAlignedQuality(convertedBaseQualities.isEmpty() ? null : convertedBaseQualities);
    }

    private void makeAlignmentIfNecessary() {
        if ( genomicsRead.getAlignment() == null ) {
            genomicsRead.setAlignment(new LinearAlignment());
        }
    }

    private void makePositionIfNecessary() {
        makeAlignmentIfNecessary();

        if ( genomicsRead.getAlignment().getPosition() == null ) {
            genomicsRead.getAlignment().setPosition(new Position());
        }
    }

    @Override
    public void setPosition( String contig, int start ) {
        if ( contig == null || contig.equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) || start < 1 ) {
            throw new IllegalArgumentException("contig must be non-null and not equal to " + SAMRecord.NO_ALIGNMENT_REFERENCE_NAME + ", and start must be >= 1");
        }

        makePositionIfNecessary();

        genomicsRead.getAlignment().getPosition().setReferenceName(contig);
        // Convert from a 1-based to a 0-based position
        genomicsRead.getAlignment().getPosition().setPosition((long)start - 1);
    }

    @Override
    public void setPosition( Locatable locatable ) {
        setPosition(locatable.getContig(), locatable.getStart());
    }

    private void makeMatePositionIfNecessary() {
        if ( genomicsRead.getNextMatePosition() == null ) {
            genomicsRead.setNextMatePosition(new Position());
        }

        // TODO: is this wise?
        if ( genomicsRead.getNumberReads() == null || genomicsRead.getNumberReads() < 2 )  {
            genomicsRead.setNumberReads(2);
        }
    }

    @Override
    public void setMatePosition( String contig, int start ) {
        if ( contig == null || contig.equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) || start < 1 ) {
            throw new IllegalArgumentException("contig must be non-null and not equal to " + SAMRecord.NO_ALIGNMENT_REFERENCE_NAME + ", and start must be >= 1");
        }

        makeMatePositionIfNecessary();

        genomicsRead.getNextMatePosition().setReferenceName(contig);
        // Convert from a 1-based to a 0-based position
        genomicsRead.getNextMatePosition().setPosition((long)start - 1);
    }

    @Override
    public void setMatePosition( Locatable locatable ) {
        setMatePosition(locatable.getContig(), locatable.getStart());
    }

    @Override
    public void setFragmentLength( int fragmentLength ) {
        if ( fragmentLength < 0 ) {
            throw new IllegalArgumentException("fragment length must be >= 0");
        }

        genomicsRead.setFragmentLength(fragmentLength);
    }

    @Override
    public void setMappingQuality( int mappingQuality ) {
        if ( mappingQuality < 0 || mappingQuality > 255 ) {
            throw new IllegalArgumentException("mapping quality must be >= 0 and <= 255");
        }

        makeAlignmentIfNecessary();
        genomicsRead.getAlignment().setMappingQuality(mappingQuality);
    }

    @Override
    public void setCigar( Cigar cigar ) {
        makeAlignmentIfNecessary();

        genomicsRead.getAlignment().setCigar(CigarConverter.convertSAMCigarToCigarUnitList(cigar));
    }

    @Override
    public void setCigar( String cigarString ) {
        makeAlignmentIfNecessary();

        genomicsRead.getAlignment().setCigar(CigarConverter.convertSAMCigarToCigarUnitList(TextCigarCodec.decode(cigarString)));
    }

    @Override
    public void setReadGroup( String readGroupID ) {
        genomicsRead.setReadGroupId(readGroupID);
    }

    @Override
    public void setNumberOfReadsInFragment( int numberOfReads ) {
        if ( numberOfReads < 1 ) {
            throw new IllegalArgumentException("number of reads in fragment must be >= 1");
        }

        genomicsRead.setNumberReads(numberOfReads);
    }

    @Override
    public void setReadNumber( int readNumber) {
        if ( readNumber < 1 ) {
            throw new IllegalArgumentException("read number must be >= 1");
        }

        // Convert from 1-based to 0-based numbering
        genomicsRead.setReadNumber(readNumber - 1);
    }

    @Override
    public void setIsPaired( boolean isPaired ) {
        if ( isPaired ) {
            genomicsRead.setNumberReads(2);
        }
        else {
            genomicsRead.setNumberReads(1);
        }
    }

    @Override
    public void setIsProperPaired( boolean isProperPaired ) {
        if ( isProperPaired ) {
            setIsPaired(true);
        }

        genomicsRead.setProperPlacement(isProperPaired);
    }

    @Override
    public void setIsUnmapped() {
        genomicsRead.setAlignment(null);
    }

    @Override
    public void setMateIsUnmapped() {
        genomicsRead.setNextMatePosition(null);
    }

    @Override
    public void setIsNegativeStrand( boolean isNegativeStrand ) {
        makePositionIfNecessary();
        genomicsRead.getAlignment().getPosition().setReverseStrand(isNegativeStrand);
    }

    @Override
    public void setMateIsNegativeStrand( boolean mateIsNegativeStrand ) {
        makeMatePositionIfNecessary();
        genomicsRead.getNextMatePosition().setReverseStrand(mateIsNegativeStrand);
    }

    @Override
    public void setIsFirstOfPair( boolean isFirstOfPair ) {
        // TODO: set to second of pair if first of pair is false?
        genomicsRead.setReadNumber(isFirstOfPair ? 0 : 1);
    }

    @Override
    public void setIsSecondOfPair( boolean isSecondOfPair ) {
        // TODO: set to first of pair if second of pair is false?
        genomicsRead.setReadNumber(isSecondOfPair ? 1 : 0);
    }

    @Override
    public void setIsNonPrimaryAlignment( boolean isNonPrimaryAlignment ) {
        genomicsRead.setSecondaryAlignment(isNonPrimaryAlignment);
    }

    @Override
    public void setIsSupplementaryAlignment( boolean isSupplementaryAlignment ) {
        genomicsRead.setSupplementaryAlignment(isSupplementaryAlignment);
    }

    @Override
    public void setFailsVendorQualityCheck( boolean failsVendorQualityCheck ) {
        genomicsRead.setFailedVendorQualityChecks(failsVendorQualityCheck);
    }

    @Override
    public void setIsDuplicate( boolean isDuplicate ) {
        genomicsRead.setDuplicateFragment(isDuplicate);
    }

    @Override
    public void setAttribute( String attributeName, Integer attributeValue ) {
        makeInfoMapIfNecessary();
        if ( attributeValue == null ) {
            clearAttribute(attributeName);
            return;
        }

        final List<String> encodedValue = Arrays.asList(attributeValue.toString());
        genomicsRead.getInfo().put(attributeName, encodedValue);
    }

    @Override
    public void setAttribute( String attributeName, String attributeValue ) {
        makeInfoMapIfNecessary();
        if ( attributeValue == null ) {
            clearAttribute(attributeName);
            return;
        }

        final List<String> encodedValue = Arrays.asList(attributeValue);
        genomicsRead.getInfo().put(attributeName, encodedValue);
    }

    @Override
    public void setAttribute( String attributeName, byte[] attributeValue ) {
        makeInfoMapIfNecessary();
        if ( attributeValue == null ) {
            clearAttribute(attributeName);
            return;
        }

        final List<String> encodedValue = Arrays.asList(new String(attributeValue));
        genomicsRead.getInfo().put(attributeName, encodedValue);
    }

    private void makeInfoMapIfNecessary() {
        if ( genomicsRead.getInfo() == null ) {
            genomicsRead.setInfo(new HashMap<>());
        }
    }

    @Override
    public void clearAttribute( String attributeName ) {
        if ( genomicsRead.getInfo() != null ) {
            genomicsRead.getInfo().remove(attributeName);
        }
    }

    @Override
    public void clearAttributes() {
        genomicsRead.setInfo(null);
    }

    public com.google.api.services.genomics.model.Read unpackRead() {
        return genomicsRead;
    }

}
