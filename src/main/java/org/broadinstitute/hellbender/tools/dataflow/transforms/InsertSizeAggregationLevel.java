package org.broadinstitute.hellbender.tools.dataflow.transforms;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.genomics.dataflow.coders.GenericJsonCoder;
import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamPairUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by louisb on 6/9/15.
 */
@DefaultCoder(GenericJsonCoder.class)
public final class InsertSizeAggregationLevel extends GenericJson implements Serializable{
    @Key
    private String orientation;
    @Key
    private String readGroup;
    @Key
    private String library;
    @Key
    private String sample;

    public String getReadGroup() {
        return readGroup;
    }

    public String getLibrary() {
        return library;
    }

    public String getSample() {
        return sample;
    }


    public InsertSizeAggregationLevel(final SAMRecord read, final boolean includeLibrary, final boolean includeReadGroup, final boolean includeSample) {
        this.orientation = SamPairUtil.getPairOrientation(read).toString();
        this.library = includeLibrary ? read.getReadGroup().getLibrary() : null;
        this.readGroup = includeReadGroup ? read.getReadGroup().getId() : null;
        this.sample = includeSample ? read.getReadGroup().getSample() : null;
    }

    public InsertSizeAggregationLevel() {
    }

    ;

    public InsertSizeAggregationLevel(final String orientation, final String library, final String readgroup, final String sample) {
        this.orientation = orientation;
        this.library = library;
        this.readGroup = readgroup;
        this.sample = sample;
    }

    public static List<InsertSizeAggregationLevel> getKeysForAllAggregationLevels(final SAMRecord read, final boolean includeAll, final boolean includeLibrary, final boolean includeReadGroup, final boolean includeSample) {
        final List<InsertSizeAggregationLevel> aggregationLevels = new ArrayList<>();
        if (includeAll) {
            aggregationLevels.add(new InsertSizeAggregationLevel(read, false, false, false));
        }
        if (includeLibrary) {
            aggregationLevels.add(new InsertSizeAggregationLevel(read, true, false, true));
        }
        if (includeReadGroup) {
            aggregationLevels.add(new InsertSizeAggregationLevel(read, true, true, true));
        }
        if (includeSample) {
            aggregationLevels.add(new InsertSizeAggregationLevel(read, false, false, true));
        }
        return aggregationLevels;

    }

    public SamPairUtil.PairOrientation getOrientation() {
        return SamPairUtil.PairOrientation.valueOf(orientation);
    }

    @Override
    public String toString() {
        return "AggregationLevel{" +
                "orientation=" + orientation +
                ", readGroup='" + readGroup + '\'' +
                ", library='" + library + '\'' +
                ", sample='" + sample + '\'' +
                '}';
    }

    public String getValueLabel(){
        String prefix;
        if (readGroup != null) {
            prefix = readGroup;
        }
        else if (this.library != null) {
            prefix = library;
        }
        else if (this.sample != null) {
            prefix = sample;
        }
        else {
            prefix = "All_Reads";
        }

        return prefix + "." + orientation.toLowerCase() + "_count";
    }


}
