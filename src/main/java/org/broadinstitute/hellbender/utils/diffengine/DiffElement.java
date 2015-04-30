package org.broadinstitute.hellbender.utils.diffengine;

import com.google.common.base.Strings;
import org.broadinstitute.hellbender.exceptions.GATKException;

/**
 * An interface that must be implemented to allow us to calculate differences
 * between structured objects
 */
public final class DiffElement {
    public final static DiffElement ROOT = new DiffElement();

    final private String name;
    final private DiffElement parent;
    final private DiffValue value;

    /**
     * For ROOT only
     */
    private DiffElement() {
        this.name = "ROOT";
        this.parent = null;
        this.value = new DiffValue(this, "ROOT");
    }

    public DiffElement(String name, DiffElement parent, DiffValue value) {
        if ( name.equals("ROOT") ) throw new IllegalArgumentException("Cannot use reserved name ROOT");
        this.name = name;
        this.parent = parent;
        this.value = value;
        this.value.setBinding(this);
    }

    public String getName() {
        return name;
    }

    public DiffElement getParent() {
        return parent;
    }

    public DiffValue getValue() {
        return value;
    }

    public boolean isRoot() { return this == ROOT; }

    @Override
    public String toString() {
        return getName() + "=" + getValue().toString();
    }

    public String toString(int offset) {
        return (offset > 0 ? Strings.repeat(" ", offset) : 0) + getName() + "=" + getValue().toString(offset);
    }

    public final String fullyQualifiedName() {
        if ( isRoot() )
            return "";
        else if ( parent.isRoot() )
            return name;
        else
            return parent.fullyQualifiedName() + "." + name;
    }

    public String toOneLineString() {
        return getName() + "=" + getValue().toOneLineString();
    }

    public DiffNode getValueAsNode() {
        if ( getValue().isCompound() )
            return (DiffNode)getValue();
        else
            throw new GATKException("Illegal request conversion of a DiffValue into a DiffNode: " + this);
    }

    public int size() {
        return 1 + getValue().size();
    }
}
