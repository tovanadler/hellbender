package org.broadinstitute.hellbender.utils.read;


import htsjdk.samtools.*;
import htsjdk.samtools.util.Locatable;
import org.broadinstitute.hellbender.exceptions.GATKException;

public final class SAMRecordToReadAdapter implements MutableRead {

    private final SAMRecord samRecord;

    public SAMRecordToReadAdapter( final SAMRecord samRecord ) {
        this.samRecord = samRecord;
    }

    private static <T> T assertFieldValueNotNull( final T fieldValue, final String fieldName ) {
        if ( fieldValue == null ) {
            throw new GATKException.MissingReadField(fieldName);
        }
        return fieldValue;
    }

    @Override
    public String getContig() {
        if ( isUnmapped() ) {
            throw new GATKException.MissingReadField("contig", "Cannot get the contig of an unmapped read");
        }

        // Guaranteed not to be null or SAMRecord.NO_ALIGNMENT_REFERENCE_NAME due to the isUnmapped() check above
        return samRecord.getReferenceName();
    }

    @Override
    public int getStart() {
        if ( isUnmapped() ) {
            throw new GATKException.MissingReadField("start", "Cannot get the start position of an unmapped read");
        }

        // Guaranteed not to be SAMRecord.NO_ALIGNMENT_START due to the isUnmapped() check above
        return samRecord.getAlignmentStart();
    }

    @Override
    public int getEnd() {
        if ( isUnmapped() ) {
            throw new GATKException.MissingReadField("end", "Cannot get the end position of an unmapped read");
        }

        // Guaranteed not to be SAMRecord.NO_ALIGNMENT_START due to the isUnmapped() check above
        return samRecord.getAlignmentEnd();
    }

    @Override
    public String getName() {
        return assertFieldValueNotNull(samRecord.getReadName(), "name");
    }

    @Override
    public int getLength() {
        return samRecord.getReadLength();
    }

    @Override
    public byte[] getBases() {
        return assertFieldValueNotNull(samRecord.getReadBases(), "bases");
    }

    @Override
    public byte[] getBaseQualities() {
        return assertFieldValueNotNull(samRecord.getBaseQualities(), "base qualities");
    }

    @Override
    public int getUnclippedStart() {
        if ( isUnmapped() ) {
            throw new GATKException.MissingReadField("unclipped start", "Cannot get the unclipped start of an unmapped read");
        }

        return samRecord.getUnclippedStart();
    }

    @Override
    public int getUnclippedEnd() {
        if ( isUnmapped() ) {
            throw new GATKException.MissingReadField("unclipped end", "Cannot get the unclipped end of an unmapped read");
        }

        return samRecord.getUnclippedEnd();
    }

    @Override
    public String getMateContig() {
        if ( mateIsUnmapped() ) {
            throw new GATKException.MissingReadField("mate contig", "Cannot get the contig of an unmapped mate");
        }

        return samRecord.getMateReferenceName();
    }

    @Override
    public int getMateStart() {
        if ( mateIsUnmapped() ) {
            throw new GATKException.MissingReadField("mate start", "Cannot get the start position of an unmapped mate");
        }

        return samRecord.getMateAlignmentStart();
    }

    @Override
    public int getFragmentLength() {
        // Will be 0 if unknown
        return samRecord.getInferredInsertSize();
    }

    @Override
    public int getMappingQuality() {
        return samRecord.getMappingQuality();
    }

    @Override
    public Cigar getCigar() {
        return assertFieldValueNotNull(samRecord.getCigar(), "cigar");
    }

    @Override
    public String getReadGroup() {
        // May return null
        return (String)samRecord.getAttribute(SAMTagUtil.getSingleton().RG);
    }

    @Override
    public int getNumberOfReadsInFragment() {
        return isPaired() ? 2 : 1;
    }

    @Override
    public int getReadNumber() {
        return (isPaired() && isFirstOfPair()) ? 1 : ((isPaired() && isSecondOfPair()) ? 2 : 0);
    }

    @Override
    public boolean isPaired() {
        return samRecord.getReadPairedFlag();
    }

    @Override
    public boolean isProperlyPaired() {
        return isPaired() && samRecord.getProperPairFlag();
    }

    @Override
    public boolean isUnmapped() {
        return samRecord.getReadUnmappedFlag() ||
               samRecord.getReferenceIndex() == null || samRecord.getReferenceIndex() == SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX ||
               samRecord.getReferenceName() == null || samRecord.getReferenceName().equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) ||
               samRecord.getAlignmentStart() == SAMRecord.NO_ALIGNMENT_START;
    }

    @Override
    public boolean mateIsUnmapped() {
        // TODO: require isPaired() here?
        return samRecord.getMateUnmappedFlag() ||
               samRecord.getMateReferenceName() == null || samRecord.getMateReferenceName().equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) ||
               samRecord.getMateAlignmentStart() == SAMRecord.NO_ALIGNMENT_START;
    }

    @Override
    public boolean isReverseStrand() {
        return samRecord.getReadNegativeStrandFlag();
    }

    @Override
    public boolean mateIsReverseStrand() {
        // TODO: require isPaired() here?
        return samRecord.getMateNegativeStrandFlag();
    }

    @Override
    public boolean isFirstOfPair() {
        return samRecord.getFirstOfPairFlag();
    }

    @Override
    public boolean isSecondOfPair() {
        return samRecord.getSecondOfPairFlag();
    }

    @Override
    public boolean isNonPrimaryAlignment() {
        return samRecord.getNotPrimaryAlignmentFlag();
    }

    @Override
    public boolean isSupplementaryAlignment() {
        return samRecord.getSupplementaryAlignmentFlag();
    }

    @Override
    public boolean failsVendorQualityCheck() {
        return samRecord.getReadFailsVendorQualityCheckFlag();
    }

    @Override
    public boolean isDuplicate() {
        return samRecord.getDuplicateReadFlag();
    }

    @Override
    public boolean hasAttribute( String attributeName ) {
        return samRecord.getAttribute(attributeName) != null;
    }

    @Override
    public Integer getAttributeAsInteger( String attributeName ) {
        try {
            return samRecord.getIntegerAttribute(attributeName);
        }
        catch ( RuntimeException e ) {
            throw new GATKException("Attribute " + attributeName + " not of integer type", e);
        }
    }

    @Override
    public String getAttributeAsString( String attributeName ) {
        try {
            return samRecord.getStringAttribute(attributeName);
        }
        catch ( SAMException e ) {
            throw new GATKException("Attribute " + attributeName + " not of String type", e);
        }
    }

    @Override
    public byte[] getAttributeAsByteArray( String attributeName ) {
        try {
            return samRecord.getByteArrayAttribute(attributeName);
        }
        catch ( SAMException e ) {
            throw new GATKException("Attribute " + attributeName + " not of byte array type", e);
        }
    }

    @Override
    public Read copy() {
        // Produces a shallow but "safe to use" copy. TODO: perform a deep copy here
        return new SAMRecordToReadAdapter(ReadUtils.cloneSAMRecord(samRecord));
    }

    @Override
    public void setName( String name ) {
        samRecord.setReadName(name);
    }

    @Override
    public void setBases( byte[] bases ) {
        samRecord.setReadBases(bases);
    }

    @Override
    public void setBaseQualities( byte[] baseQualities ) {
        samRecord.setBaseQualities(baseQualities);
    }

    @Override
    public void setPosition( String contig, int start ) {
        if ( contig == null || contig.equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) || start < 1 ) {
            throw new IllegalArgumentException("contig must be non-null and not equal to " + SAMRecord.NO_ALIGNMENT_REFERENCE_NAME + ", and start must be >= 1");
        }

        samRecord.setReferenceName(contig);
        samRecord.setAlignmentStart(start);
        samRecord.setReadUnmappedFlag(false);
    }

    @Override
    public void setPosition( Locatable locatable ) {
        setPosition(locatable.getContig(), locatable.getStart());
    }

    @Override
    public void setMatePosition( String contig, int start ) {
        if ( contig == null || contig.equals(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME) || start < 1 ) {
            throw new IllegalArgumentException("contig must be non-null and not equal to " + SAMRecord.NO_ALIGNMENT_REFERENCE_NAME + ", and start must be >= 1");
        }

        samRecord.setMateReferenceName(contig);
        samRecord.setMateAlignmentStart(start);
        samRecord.setMateUnmappedFlag(false);
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

        samRecord.setInferredInsertSize(fragmentLength);
    }

    @Override
    public void setMappingQuality( int mappingQuality ) {
        if ( mappingQuality < 0 || mappingQuality > 255 ) {
            throw new IllegalArgumentException("mapping quality must be >= 0 and <= 255");
        }

        samRecord.setMappingQuality(mappingQuality);
    }

    @Override
    public void setCigar( Cigar cigar ) {
        samRecord.setCigar(cigar);
    }

    @Override
    public void setCigar( String cigarString ) {
        samRecord.setCigarString(cigarString);
    }

    @Override
    public void setReadGroup( String readGroupID ) {
        samRecord.setAttribute(SAMTag.RG.name(), readGroupID);
    }

    @Override
    public void setNumberOfReadsInFragment( int numberOfReads ) {
        if ( numberOfReads < 1 ) {
            throw new IllegalArgumentException("number of reads in fragment must be >= 1");
        }
        else if ( numberOfReads == 1 ) {
            setIsPaired(false);
        }
        else if ( numberOfReads == 2 ) {
            setIsPaired(true);
        }
        else {
            throw new IllegalArgumentException("The underlying Read type (SAMRecord) does not support fragments with > 2 reads");
        }
    }

    @Override
    public void setReadNumber( int readNumber) {
        if ( readNumber < 1 ) {
            throw new IllegalArgumentException("read number must be >= 1");
        }
        else if ( readNumber == 1 ) {
            setIsFirstOfPair(true);
            setIsSecondOfPair(false);
        }
        else if ( readNumber == 2 ) {
            setIsFirstOfPair(false);
            setIsSecondOfPair(true);
        }
        else {
            throw new IllegalArgumentException("The underlying Read type (SAMRecord) does not support fragments with > 2 reads");
        }
    }

    @Override
    public void setIsPaired( boolean isPaired ) {
        samRecord.setReadPairedFlag(isPaired);
    }

    @Override
    public void setIsProperPaired( boolean isProperPaired ) {
        samRecord.setProperPairFlag(isProperPaired);
    }

    @Override
    public void setIsUnmapped() {
        samRecord.setReadUnmappedFlag(true);
    }

    @Override
    public void setMateIsUnmapped() {
        samRecord.setMateUnmappedFlag(true);
    }

    @Override
    public void setIsNegativeStrand( boolean isNegativeStrand ) {
        samRecord.setReadNegativeStrandFlag(isNegativeStrand);
    }

    @Override
    public void setMateIsNegativeStrand( boolean mateIsNegativeStrand ) {
        samRecord.setMateNegativeStrandFlag(mateIsNegativeStrand);
    }

    @Override
    public void setIsFirstOfPair( boolean isFirstOfPair ) {
        samRecord.setFirstOfPairFlag(isFirstOfPair);
    }

    @Override
    public void setIsSecondOfPair( boolean isSecondOfPair ) {
        samRecord.setSecondOfPairFlag(isSecondOfPair);
    }

    @Override
    public void setIsNonPrimaryAlignment( boolean isNonPrimaryAlignment ) {
        samRecord.setNotPrimaryAlignmentFlag(isNonPrimaryAlignment);
    }

    @Override
    public void setIsSupplementaryAlignment( boolean isSupplementaryAlignment ) {
        samRecord.setSupplementaryAlignmentFlag(isSupplementaryAlignment);
    }

    @Override
    public void setFailsVendorQualityCheck( boolean failsVendorQualityCheck ) {
        samRecord.setReadFailsVendorQualityCheckFlag(failsVendorQualityCheck);
    }

    @Override
    public void setIsDuplicate( boolean isDuplicate ) {
        samRecord.setDuplicateReadFlag(isDuplicate);
    }

    @Override
    public void setAttribute( String attributeName, Integer attributeValue ) {
        samRecord.setAttribute(attributeName, attributeValue);
    }

    @Override
    public void setAttribute( String attributeName, String attributeValue ) {
        samRecord.setAttribute(attributeName, attributeValue);
    }

    @Override
    public void setAttribute( String attributeName, byte[] attributeValue ) {
        samRecord.setAttribute(attributeName, attributeValue);
    }

    @Override
    public void clearAttribute( String attributeName ) {
        samRecord.setAttribute(attributeName, null);
    }

    @Override
    public void clearAttributes() {
        samRecord.clearAttributes();
    }

    public SAMRecord unpackSAMRecord() {
        return samRecord;
    }
}
