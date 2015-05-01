package org.broadinstitute.hellbender.utils.diffengine;

import com.google.common.base.Strings;
import org.broadinstitute.hellbender.exceptions.GATKException;

import java.util.Arrays;
import java.util.List;

/**
 * Represents an element in the tree of differences.
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
     * Makes a new element from the given parent and unparsed tree.
     */
    private static DiffElement fromString(String tree, DiffElement parent) {
        // X=(A=A B=B C=(D=D))
        final String[] parts = tree.split("=", 2);
        if ( parts.length != 2 ) {
            throw new GATKException("Unexpected tree structure: " + tree);
        }
        final String name = parts[0];
        final String value = parts[1];

        if ( value.length() == 0 ) {
            throw new GATKException("Illegal tree structure: " + value + " at " + tree);
        }

        if ( value.charAt(0) == '(' ) {
            if ( ! value.endsWith(")") ) {
                throw new GATKException("Illegal tree structure.  Missing ): " + value + " at " + tree);
            }
            final String subtree = value.substring(1, value.length()-1);
            final DiffNode rec = DiffNode.empty(name, parent);
            String[] subParts = subtree.split(" ");
            for ( String subPart : subParts ) {
                rec.add(fromString(subPart, rec.getBinding()));
            }
            return rec.getBinding();
        } else {
            return new DiffValue(name, parent, value).getBinding();
        }
    }

    /**
     * Makes a new element from parsing the tree. Puts the new element right under the ROOT.
     */
    public static DiffElement fromString(String tree) {
        return fromString(tree, DiffElement.ROOT);
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

    public List<Difference> diff(final DiffElement test) {
        final DiffValue masterValue = this.getValue();
        final DiffValue testValue = test.getValue();

        if ( masterValue.isCompound() && masterValue.isCompound() ) {
            return this.getValueAsNode().diff(test.getValueAsNode());
        } else if ( masterValue.isAtomic() && testValue.isAtomic() ) {
            return masterValue.diff(testValue);
        } else {
            // structural difference in types.  one is node, other is leaf
            return Arrays.asList(new Difference(this, test));
        }
    }


}
