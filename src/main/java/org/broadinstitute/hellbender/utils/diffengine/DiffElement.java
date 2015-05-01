package org.broadinstitute.hellbender.utils.diffengine;

import com.google.common.base.Strings;
import org.broadinstitute.hellbender.exceptions.GATKException;

/**
 * An interface that must be implemented to allow us to calculate differences
 * between structured objects
 */
public final class DiffElement {

    /**
     * Special element that is on top of the hierarchy and has no parent.
     */
    public final static DiffElement ROOT = new DiffElement();
    public static final String ROOT_NAME= "ROOT";

    private final String name;
    private final DiffElement parent;
    private final DiffValue value;

    /**
     * For ROOT only
     */
    private DiffElement() {
        this.name = ROOT_NAME;
        this.parent = null;
        this.value = new DiffValue(this, ROOT_NAME);
    }

    public DiffElement(String name, DiffElement parent, DiffValue value) {
        if ( name.equals(ROOT_NAME) ) {
            throw new IllegalArgumentException("Cannot use reserved name ROOT");
        }
        this.name = name;
        this.parent = parent;
        this.value = value;
        this.value.setBinding(this);
    }

    /**
     * Returns the name of this element.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the parent element of this element.
     */
    public DiffElement getParent() {
        return parent;
    }

    /**
     * Returns the value of this element.
     */
    public DiffValue getValue() {
        return value;
    }

    /**
     * Returns whether this element is the ROOT.
     */
    public boolean isRoot() { return this == ROOT; }

    @Override
    public String toString() {
        return getName() + "=" + getValue().toString();
    }

    public String toString(int offset) {
        return (offset > 0 ? Strings.repeat(" ", offset) : 0) + getName() + "=" + getValue().toString(offset);
    }

    /**
     * Returns the name of this element prefixed by the name of all of its parents.
     */
    public String fullyQualifiedName() {
        if ( isRoot() )
            return "";
        else if ( parent.isRoot() )
            return name;
        else
            return parent.fullyQualifiedName() + "." + name;
    }

    /**
     * Returns a one-line representation of this element.
     */
    public String toOneLineString() {
        return getName() + "=" + getValue().toOneLineString();
    }

    /**
     * If this element's value is a DiffNode, this method returns that DiffNode.
     * Otherwise, it throws a GATKException.
     */
    public DiffNode getValueAsNode() {
        if ( getValue().isCompound() )
            return (DiffNode)getValue();
        else
            throw new GATKException("Illegal request conversion of a DiffValue into a DiffNode: " + this);
    }

    /**
     * Returns the size of this element, which is 1 + the size of the element's value.
     */
    public int size() {
        return 1 + getValue().size();
    }
}
